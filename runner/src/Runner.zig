const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");

const Setting = @import("./settings/Setting.zig");
const StageCount = @import("./Config.zig").StageCount;
const app_context = @import("build_options").app_context;

const Symbol = core.Symbol;
const systemLog = core.Logger.SystemDirect(app_context);
const traceLog = core.Logger.TraceDirect(app_context);
const log = core.Logger.Stage.log;

const Self = @This();

allocator: std.mem.Allocator,
context: zmq.ZContext,
connection: *core.sockets.Connection.Server(app_context),

pub fn init(allocator: std.mem.Allocator, setting: Setting) !Self {
    var ctx = try zmq.ZContext.init(allocator);

    var connection = try core.sockets.Connection.Server(app_context).init(allocator, &ctx);
    try connection.bind(setting.general.runner_endpoints);

    return .{
        .allocator = allocator,
        .context = ctx,
        .connection = connection,
    };
}

pub fn deinit(self: *Self) void {
    self.connection.deinit();
    self.context.deinit();
}

pub fn run(self: *Self, stage_count: StageCount, setting: Setting) !void {
    systemLog.debug("Launched", .{});

    dump_setting: {
        systemLog.debug("CLI: Req/Rep Channel = {s}", .{setting.general.runner_endpoints.req_rep});
        systemLog.debug("CLI: Pub/Sub Channel = {s}", .{setting.general.runner_endpoints.pub_sub});
        systemLog.debug("CLI: Watch mode = {}", .{setting.command.watchModeEnabled()});
        break :dump_setting;
    }

    // const oneshot = (!setting.watch);
    const oneshot = true;
    var left_launching = stage_count.stage_watch + stage_count.stage_extract + stage_count.stage_generate;
    var left_topic_stage = stage_count.stage_extract;
    var left_launched = stage_count.stage_watch + stage_count.stage_extract + stage_count.stage_generate;

    var source_cache = try PayloadCacheManager.init(self.allocator);
    defer source_cache.deinit();

    try self.connection.dispatcher.state.ready();
    
    while (self.connection.dispatcher.isReady()) {
        const _item = try self.connection.dispatcher.dispatch();

        if (_item) |*item| {
            defer item.deinit();

            switch (item.event) {
                .launched => {
                    try self.connection.dispatcher.reply(item.socket, .ack);
                    
                    if (left_launching > 0) {
                        left_launching -= 1;
                        systemLog.debug("Received launched: '{s}' (left: {})", .{item.from, left_launching});
                    }
                    else {
                        systemLog.debug("Received rebooted: '{s}' (left: {})", .{item.from, left_launching});
                    }

                    if (left_launching <= 0) {
                        try self.onAfterLaunch();
                    }
                },
                .failed_launching => {
                    try self.connection.dispatcher.reply(item.socket, .ack);
                    try self.connection.dispatcher.state.receiveTerminate();

                    if (left_launching > 0) {
                        left_launching -= 1;
                        systemLog.debug("Received to failed launching: '{s}' (left: {})", .{item.from, left_launching});
                    }
                    if (left_launching <= 0) {
                        try self.onAfterLaunch();
                    }
                },
                .topic => |payload| {
                    try self.connection.dispatcher.reply(item.socket, .ack);

                    for (payload.names) |name| {
                        try source_cache.topics.insert(name);
                    }

                    left_topic_stage -= 1;
                    systemLog.debug("Receive 'topic' ({})", .{left_topic_stage});
                    try dumpTopics(self.allocator, source_cache.topics);

                    if (left_topic_stage <= 0) {
                        try self.connection.dispatcher.post(.ready_watch_path);
                    }
                },
                .source_path => |path| {
                    try self.connection.dispatcher.reply(item.socket, .ack);

                    if (try source_cache.addNewEntry(path)) {
                        systemLog.debug("Received source name: {s}, path: {s}, hash: {s}", .{path.name, path.path, path.hash});
                        try self.connection.dispatcher.post(.{.source_path = try path.clone(self.allocator)});
                    }
                },
                .finish_watch_path => {
                    traceLog.debug("Watching stage finished", .{});
                    if (oneshot) {
                        // request quit for Watch stage
                        traceLog.debug("Request quit for Watching stage", .{});
                        try self.connection.dispatcher.reply(item.socket, .quit);
                        try self.connection.dispatcher.state.receiveTerminate();
                    }
                    else {
                        try self.connection.dispatcher.reply(item.socket, .ack);
                    }
                },
                .topic_body => |payload| {
                    // delay 1 cycle
                    try self.connection.dispatcher.delay(item.socket, item.from, .pending_finish_source_path);

                    switch (try source_cache.update(payload)) {
                        .expired => {
                            systemLog.debug("Content expired: {s}", .{payload.header.path});
                        },
                        .missing => {
                            systemLog.debug("Waiting left content: {s}", .{payload.header.path});
                        },
                        .fulfil => {
                            systemLog.debug("Source is ready: {s}", .{payload.header.name});
                            if (try source_cache.ready(payload.header)) {
                                try self.connection.dispatcher.post(.ready_topic_body);
                            }
                        },
                    }
                },
                .invalid_topic_body => |payload| {
                    log(payload.log.level, item.from, payload.log.content);

                    try source_cache.dismiss(payload.header);
                    // delay 1 cycle
                    try self.connection.dispatcher.delay(item.socket, item.from, .pending_finish_source_path);
                },
                .pending_finish_source_path => {
                    if ((self.connection.dispatcher.state.level.terminating) and (source_cache.cache.count() == 0)) {
                        systemLog.debug("No more source path", .{});
                        try self.connection.dispatcher.reply(item.socket, .ack);
                        try self.connection.dispatcher.post(.finish_source_path);
                    }
                    else {
                        systemLog.debug("Wait receive next source path", .{});
                        try self.connection.dispatcher.reply(item.socket, .ack);
                    }
                },
                .finish_topic_body => {
                    // request quit for Extract stage
                    try self.connection.dispatcher.reply(item.socket, .quit);
                },
                .ready_generate => {
                    if (source_cache.ready_queue.dequeue()) |source| {
                        systemLog.debug("Send source: {s}", .{source.header.name});
                        try self.connection.dispatcher.reply(item.socket, .{.topic_body = source});
                    }
                    else {
                        // delay 1 cycle
                        try self.connection.dispatcher.delay(item.socket, item.from, .pending_finish_topic_body);
                    }
                },
                .pending_finish_topic_body => {
                    if ((self.connection.dispatcher.state.level.terminating) and (source_cache.isEmpty())) {
                        systemLog.debug("No more sources", .{});
                        try self.connection.dispatcher.reply(item.socket, .finish_topic_body);
                    }
                    else {
                        systemLog.debug("Wait receive next source", .{});
                        try self.connection.dispatcher.reply(item.socket, .ack);
                    }
                },
                .finish_generate => {
                    // request quit for Generate stage
                    try self.connection.dispatcher.reply(item.socket, .quit);
                },
                .quit_accept => {
                    try self.connection.dispatcher.reply(item.socket, .ack);

                    left_launched -= 1;
                    systemLog.debug("Quit acceptrd: {s} (left: {})", .{item.from, left_launched});

                    if (left_launched <= 0) {
                        systemLog.debug("All Quit acceptrd", .{});
                        try self.connection.dispatcher.state.done();
                    }
                },
                .log => |payload| {
                    try self.connection.dispatcher.reply(item.socket, .ack);
                    log(payload.level, item.from, payload.content);
                },
                else => {
                    try self.connection.dispatcher.reply(item.socket, .ack);
                    systemLog.debug("Discard command: {}", .{std.meta.activeTag(item.event)});
                },
            }
        }
    }

    systemLog.debug("terminated", .{});
}

fn onAfterLaunch(self: Self) !void {
    if (self.connection.dispatcher.state.level.terminating) {
        traceLog.debug("Stopping launch process", .{});
        try self.connection.dispatcher.post(.quit_all);
    }
    else {
        traceLog.debug("Received launched all", .{});
        // collect topics
        try self.connection.dispatcher.post(.request_topic);
    }
}

fn dumpTopics(allocator: std.mem.Allocator, topics: std.BufSet) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const managed_allocator = arena.allocator();

    var buf = std.ArrayList(u8).init(managed_allocator);
    var writer = buf.writer();

    try writer.writeAll(try std.fmt.allocPrint(managed_allocator, "[{s}] Received topics ({}): ", .{app_context, topics.count()}));

    var it = topics.iterator();

    while (it.next()) |topic| {
        try writer.writeAll(topic.*);
        try writer.writeAll(", ");
    }

    traceLog.debug("{s}", .{buf.items});
}

const PayloadCacheManager = struct {
    arena: *std.heap.ArenaAllocator,
    cache: std.StringHashMap(*Entry),
    topics: std.BufSet,
    ready_queue: core.Queue(core.Event.Payload.TopicBody),

    pub const CacheStatus = enum {
        expired, missing, fulfil
    };

    pub fn init(allocator: std.mem.Allocator) !PayloadCacheManager {
        const arena = try allocator.create(std.heap.ArenaAllocator);
        arena.* = std.heap.ArenaAllocator.init(allocator);

        const managed_allocator = arena.allocator();
        return .{
            .arena = arena,
            .topics = std.BufSet.init(managed_allocator),
            .cache = std.StringHashMap(*Entry).init(managed_allocator),
            .ready_queue = core.Queue(core.Event.Payload.TopicBody).init(managed_allocator),
        };
    }

    pub fn deinit(self: *PayloadCacheManager) void {
        const child = self.arena.child_allocator;

        self.ready_queue.deinit();
        self.topics.deinit();
        self.cache.deinit();
        self.arena.deinit();
        
        child.destroy(self.arena);
    }

    fn addNewEntry(self: *PayloadCacheManager, source: core.Event.Payload.SourcePath) !bool {
        const allocator = self.arena.allocator();

        const path = try allocator.dupe(u8, source.path);
        defer allocator.free(path);
        const entry = try self.cache.getOrPut(path);

        if (entry.found_existing) {
            if (! entry.value_ptr.*.isExpired(source.hash)) return false;
            entry.value_ptr.*.deinit();
        }
        
        entry.value_ptr.* = try Entry.init(allocator, source, self.topics);
        entry.key_ptr.* = entry.value_ptr.*.source.path;

        return true;
    }

    pub fn update(self: *PayloadCacheManager, topic_body: core.Event.Payload.TopicBody) !CacheStatus {
        if (self.cache.get(topic_body.header.path)) |entry| {
            if (entry.isExpired(topic_body.header.hash)) return .expired;

            return entry.update(topic_body.bodies);
        }

        return .expired;
    }

    pub fn ready(self: *PayloadCacheManager, path: core.Event.Payload.SourcePath) !bool {
        if (self.cache.fetchRemove(path.path)) |kv| {
            var entry: *Entry = kv.value;
            defer entry.deinit();

            const a = self.arena.allocator();

            const bodies = try a.alloc(core.StructView(core.Event.Payload.TopicBody.Item), entry.contents.count());
            defer a.free(bodies);
            
            var it = entry.contents.iterator();
            var i: usize = 0;

            while (it.next()) |content| {
                bodies[i] = .{content.key_ptr.*, content.value_ptr.*};
                i += 1;
            }

            try self.ready_queue.enqueue(
                try core.Event.Payload.TopicBody.init(a, entry.source.values(), bodies)
            );

            return true;
        }

        return false;
    }

    pub fn dismiss(self: *PayloadCacheManager, path: core.Event.Payload.SourcePath) !void {
        if (self.cache.fetchRemove(path.path)) |kv| {
            var entry = kv.value;
            defer entry.deinit();
        }
    }

    pub fn isEmpty(self: PayloadCacheManager) bool {
        return (self.cache.count() == 0) and (self.ready_queue.count() == 0);
    }

    const Entry = struct {
        allocator: std.mem.Allocator, 
        source: core.Event.Payload.SourcePath,
        left_topics: std.BufSet,
        contents: std.BufMap,

        pub fn init(allocator: std.mem.Allocator, source: core.Event.Payload.SourcePath, topics: std.BufSet) !*Entry {
            const self = try allocator.create(Entry);
            self.* =  .{
                .allocator = allocator,
                .source = try source.clone(allocator),
                .left_topics = try topics.cloneWithAllocator(allocator),
                .contents = std.BufMap.init(allocator),
            };

            return self;
        }

        pub fn deinit(self: *Entry) void {
            self.contents.deinit();
            self.left_topics.deinit();
            self.source.deinit();
            self.allocator.destroy(self);
            self.* = undefined;
        }

        pub fn isExpired(self: *Entry, hash: Symbol) bool {
            return ! std.mem.eql(u8, self.source.hash, hash);
        }

        pub fn update(self: *Entry, bodies: []const core.Event.Payload.TopicBody.Item) !CacheStatus {
            for (bodies) |body| {
                try self.contents.put(body.topic, body.content);
                self.left_topics.remove(body.topic);
            }

            return if (self.left_topics.count() > 0) .missing else .fulfil;
        }
    };
};

test "Runner#1" {
    // const test_allocator = std.testing.allocator;
    // var arena = std.heap.ArenaAllocator.init(test_allocator);
    // defer arena.deinit();

    // const allocator = arena.allocator();

    // var t = try std.Thread.spawn(.{}, mockRunner, .{allocator});

    // var ctx = try zmq.ZContext.init(allocator);
    // defer ctx.deinit();
    // var stage = try core.sockets.Connection.Client.init(allocator, &ctx);
    // defer stage.deinit();
    // try stage.subscribe_socket.socket.setSocketOption(.{.Subscribe = ""});
    // try stage.connect();

    // const send_socket = stage.request_socket;
    
    // var event: core.Event = undefined;

    // // launched
    // try core.sendEvent(
    //     allocator, send_socket,
    //     .{.launched = try core.Event.Payload.Stage.init(allocator, "TEST")}
    // );
    // event = try core.receiveEventWithPayload(allocator, send_socket);
    // std.debug.print("[C:Rec] {}\n", .{event});

    // // quit
    // try core.sendEvent(
    //     allocator, send_socket,
    //     .{.quit_accept = try core.Event.Payload.Stage.init(allocator, "TEST")}
    // );

    // t.join();
    // try std.testing.expect(true);
}

fn mockRunner(allocator: std.mem.Allocator) !void {
    std.debug.print("runner invoked\n", .{});

    var runner = try Self.init(allocator);
    defer runner.deinit();
    
    // const send_socket = runner.connection.send_socket;
    const receive_socket = runner.connection.reply_socket;
    
    var event: core.Event = undefined;

    // launched
    event = try core.receiveEventWithPayload(allocator, receive_socket);
    std.debug.print("[R:Rec] {}\n", .{event});
    try core.sendEvent(allocator, receive_socket, .ack);

    // quit_accept
    event = try core.receiveEventWithPayload(allocator, receive_socket);
    std.debug.print("[R:Rec] {}\n", .{event});
    try core.sendEvent(allocator, receive_socket, .ack);
}