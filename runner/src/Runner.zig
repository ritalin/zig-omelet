const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");

const Setting = @import("./settings/Setting.zig");
const StageCount = @import("./Config.zig").StageCount;

const Symbol = core.Symbol;
const systemLog = core.Logger.SystemDirect(APP_CONTEXT);
const traceLog = core.Logger.TraceDirect(APP_CONTEXT);
const log = core.Logger.Stage.log;

pub const APP_CONTEXT = "runner";
const Self = @This();

allocator: std.mem.Allocator,
context: zmq.ZContext,
connection: *core.sockets.Connection.Server,

pub fn init(allocator: std.mem.Allocator, setting: Setting) !Self {
    var ctx = try zmq.ZContext.init(allocator);

    var connection = try core.sockets.Connection.Server.init(allocator, &ctx);
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

            traceLog.debug("Received command: {}", .{std.meta.activeTag(item.event)});

            switch (item.event) {
                .launched => |payload| {
                    try self.connection.dispatcher.reply(item.socket, .ack);
                    
                    if (left_launching > 0) {
                        left_launching -= 1;
                        traceLog.debug("Received launched: '{s}' (left: {})", .{payload.stage_name, left_launching});
                    }
                    else {
                        traceLog.debug("Received rebooted: '{s}' (left: {})", .{payload.stage_name, left_launching});
                    }

                    if (left_launching <= 0) {
                        traceLog.debug("Received launched all", .{});
                        // collect topics
                        try self.connection.dispatcher.post(.request_topic);
                    }
                },
                .topic => |payload| {
                    try self.connection.dispatcher.reply(item.socket, .ack);

                    for (payload.names) |name| {
                        try source_cache.topics.insert(name);
                    }

                    left_topic_stage -= 1;
                    traceLog.debug("Receive 'topic' ({})", .{left_topic_stage});

                    if (left_topic_stage <= 0) {
                        try dumpTopics(self.allocator, source_cache.topics);
                        try self.connection.dispatcher.post(.begin_watch_path);
                    }
                },
                .source_path => |path| {
                    try self.connection.dispatcher.reply(item.socket, .ack);

                    if (try source_cache.addNewEntry(path)) {
                        traceLog.debug("Received source name: {s}, path: {s}, hash: {s}", .{path.name, path.path, path.hash});
                        try self.connection.dispatcher.post(.{.source_path = try path.clone(self.allocator)});
                    }
                },
                .topic_body => |payload| {
                    try self.connection.dispatcher.reply(item.socket, .ack);

                    switch (try source_cache.update(payload)) {
                        .expired => {
                            traceLog.debug("Content expired: {s}", .{payload.header.path});
                        },
                        .missing => {
                            traceLog.debug("Waiting left content: {s}", .{payload.header.path});
                        },
                        .fulfil => {
                            traceLog.debug("Source is ready: {s}", .{payload.header.name});
                            if (try source_cache.ready(payload.header)) {
                                try self.connection.dispatcher.post(.ready_topic_body);
                            }
                        },
                    }
                },
                .invalid_topic_body => |payload| {
                    log(payload.log_level, payload.log_from, payload.log_content);

                    try source_cache.dismiss(payload.header);
                    try self.connection.dispatcher.delay(item.socket, .finish_topic_body);
                },
                .end_watch_path => {
                    traceLog.debug("Received finished somewhere", .{});
                    if (oneshot) {
                        try self.connection.dispatcher.state.receiveTerminate();
                        try self.connection.dispatcher.reply(item.socket, .quit);
                        try self.connection.dispatcher.post(.end_watch_path);
                    }
                    else {
                        try self.connection.dispatcher.reply(item.socket, .ack);
                    }
                },
                .ready_generate => {
                    if (source_cache.ready_queue.dequeue()) |source| {
                        defer source.deinit();
                        traceLog.debug("Send source: {s}", .{source.header.name});
                        try self.connection.dispatcher.reply(item.socket, .{.topic_body = try source.clone(self.allocator)});
                    }
                    else {
                        try self.connection.dispatcher.delay(item.socket, .finish_topic_body);
                    }
                },
                .finish_topic_body => {
                    if ((self.connection.dispatcher.state.level.terminating) and (source_cache.cache.count() == 0)) {
                        traceLog.debug("No more sources", .{});
                        try self.connection.dispatcher.reply(item.socket, .quit);
    
                        try self.connection.dispatcher.post(.finish_topic_body);
                    }
                    else {
                        traceLog.debug("Wait receive next source", .{});
                        try self.connection.dispatcher.reply(item.socket, .ack);
                    }
                },
                .quit_accept => |payload| {
                    try self.connection.dispatcher.reply(item.socket, .ack);

                    left_launched -= 1;
                    traceLog.debug("Quit acceptrd: {s} (left: {})", .{payload.stage_name, left_launched});

                    if (left_launched <= 0) {
                        traceLog.debug("All Quit acceptrd", .{});
                        try self.connection.dispatcher.state.done();
                    }
                },
                .log => |payload| {
                    try self.connection.dispatcher.reply(item.socket, .ack);
                    log(payload.level, payload.from, payload.content);
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

fn dumpTopics(allocator: std.mem.Allocator, topics: std.BufSet) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const managed_allocator = arena.allocator();

    var buf = std.ArrayList(u8).init(managed_allocator);
    var writer = buf.writer();

    try writer.writeAll(try std.fmt.allocPrint(managed_allocator, "[{s}] Received topics ({}): ", .{APP_CONTEXT, topics.count()}));

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
    ready_queue: core.Queue(core.EventPayload.TopicBody),

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
            .ready_queue = core.Queue(core.EventPayload.TopicBody).init(managed_allocator),
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

    fn addNewEntry(self: *PayloadCacheManager, path: core.EventPayload.SourcePath) !bool {
        const entry = try self.cache.getOrPut(path.path);

        if (entry.found_existing) {

            if (entry.value_ptr.*.isExpired(path.hash)) return false;

            entry.value_ptr.*.deinit();
        }
        
        entry.value_ptr.* = try Entry.init(self.arena.allocator(), path, self.topics);
        entry.key_ptr.* = entry.value_ptr.*.path.path;

        return true;
    }

    pub fn update(self: *PayloadCacheManager, topic_body: core.EventPayload.TopicBody) !CacheStatus {
        const entry = try self.cache.getOrPut(topic_body.header.path);

        if (entry.found_existing) {
            if (entry.value_ptr.*.isExpired(topic_body.header.hash)) return .expired;
        }
        else {
            entry.value_ptr.* = try Entry.init(self.arena.allocator(), topic_body.header, self.topics);
            entry.key_ptr.* = entry.value_ptr.*.path.path;
        }

        return entry.value_ptr.*.update(topic_body.bodies);
    }

    pub fn ready(self: *PayloadCacheManager, path: core.EventPayload.SourcePath) !bool {
        if (self.cache.fetchRemove(path.path)) |kv| {
            var entry: *Entry = kv.value;
            defer entry.deinit();

            const a = self.arena.allocator();

            const bodies = try a.alloc(core.EventPayload.TopicBody.Item.Values, entry.contents.count());
            defer a.free(bodies);
            
            var it = entry.contents.iterator();
            var i: usize = 0;

            while (it.next()) |content| {
                bodies[i] = .{content.key_ptr.*, content.value_ptr.*};
                i += 1;
            }

            try self.ready_queue.enqueue(
                try core.EventPayload.TopicBody.init(a, entry.path.name, entry.path.path, entry.path.hash, bodies)
            );

            // return self.cache.fetchRemove(path.path) != null;
        }

        return false;
    }

    pub fn dismiss(self: *PayloadCacheManager, path: core.EventPayload.SourcePath) !void {
        if (self.cache.fetchRemove(path.path)) |kv| {
            var entry = kv.value;
            defer entry.deinit();
            // _ = self.cache.fetchRemove(path.path);
        }
    }

    const Entry = struct {
        allocator: std.mem.Allocator, 
        path: core.EventPayload.SourcePath,
        left_topics: std.BufSet,
        contents: std.BufMap,

        pub fn init(allocator: std.mem.Allocator, path: core.EventPayload.SourcePath, topics: std.BufSet) !*Entry {
            const self = try allocator.create(Entry);
            self.* =  .{
                .allocator = allocator,
                .path = try path.clone(allocator),
                .left_topics = try topics.cloneWithAllocator(allocator),
                .contents = std.BufMap.init(allocator),
            };

            return self;
        }

        pub fn deinit(self: *Entry) void {
            self.contents.deinit();
            self.left_topics.deinit();
            self.path.deinit();
            self.allocator.destroy(self);
            self.* = undefined;
        }

        pub fn isExpired(self: *Entry, hash: Symbol) bool {
            return ! std.mem.eql(u8, self.path.hash, hash);
        }

        pub fn update(self: *Entry, bodies: []const core.EventPayload.TopicBody.Item) !CacheStatus {
            for (bodies) |body| {
                try self.contents.put(body.topic, body.content);
                self.left_topics.remove(body.topic);
            }

            return if (self.left_topics.count() > 0) .missing else .fulfil;
        }
    };
};

test "Runner#1" {
    const test_allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(test_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    var t = try std.Thread.spawn(.{}, mockRunner, .{allocator});

    var ctx = try zmq.ZContext.init(allocator);
    defer ctx.deinit();
    var stage = try core.sockets.Connection.Client.init(allocator, &ctx);
    defer stage.deinit();
    try stage.subscribe_socket.socket.setSocketOption(.{.Subscribe = ""});
    try stage.connect();

    const send_socket = stage.request_socket;
    
    var event: core.Event = undefined;

    // launched
    try core.sendEvent(
        allocator, send_socket,
        .{.launched = try core.EventPayload.Stage.init(allocator, "TEST")}
    );
    event = try core.receiveEventWithPayload(allocator, send_socket);
    std.debug.print("[C:Rec] {}\n", .{event});

    // quit
    try core.sendEvent(
        allocator, send_socket,
        .{.quit_accept = try core.EventPayload.Stage.init(allocator, "TEST")}
    );

    t.join();
    try std.testing.expect(true);
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