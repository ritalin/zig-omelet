const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");

const Setting = @import("./Setting.zig");
const StageCount = @import("./Config.zig").StageCount;

const Symbol = core.Symbol;
const systemLog = core.Logger.Server.systemLog;
const traceLog = core.Logger.Server.traceLog;
const log = core.Logger.Server.log;

const APP_CONTEXT = "runner";
const Self = @This();

allocator: std.mem.Allocator,
context: zmq.ZContext,
connection: *core.sockets.Connection.Server,

pub fn init(allocator: std.mem.Allocator, setting: Setting) !Self {
    var ctx = try zmq.ZContext.init(allocator);

    var connection = try core.sockets.Connection.Server.init(allocator, &ctx);
    try connection.bind(setting.runner_endpoints);

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
    systemLog.debug("[{s}] Launched", .{APP_CONTEXT});

    dump_setting: {
        systemLog.debug("CLI: Req/Rep Channel = {s}", .{setting.runner_endpoints.req_rep});
        systemLog.debug("CLI: Pub/Sub Channel = {s}", .{setting.runner_endpoints.pub_sub});
        systemLog.debug("CLI: Watch mode = {}", .{setting.watch});
        break :dump_setting;
    }

    // const oneshot = (!setting.watch);
    const oneshot = true;
    var left_launching = stage_count.stage_watch + stage_count.stage_extract + stage_count.stage_generate;
    var left_topic_stage = stage_count.stage_extract;
    var left_launched = stage_count.stage_watch + stage_count.stage_extract + stage_count.stage_generate;

    var source_cache = try PayloadCacheManager.init(self.allocator);
    defer source_cache.deinit();

    var state: core.StageState = .booting;
    
    while (self.connection.dispatcher.isReady()) {
        const _item = try self.connection.dispatcher.dispatch();

        if (_item) |*item| {
            defer item.deinit();

            traceLog.debug("[{s}] Received command: {}", .{APP_CONTEXT, std.meta.activeTag(item.event)});

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
                    traceLog.debug("[{s}] Receive 'topic' ({})", .{APP_CONTEXT, left_topic_stage});

                    if (left_topic_stage <= 0) {
                        try dumpTopics(self.allocator, source_cache.topics);
                        try self.connection.dispatcher.post(.begin_watch_path);

                        state = .ready;
                    }
                },
                .source_path => |path| {
                    try self.connection.dispatcher.reply(item.socket, .ack);

                    if (try source_cache.addNewEntry(path)) {
                        traceLog.debug("[{s}] Received source name: {s}, path: {s}, hash: {s}", .{APP_CONTEXT, path.name, path.path, path.hash});
                        try self.connection.dispatcher.post(.{.source_path = try path.clone(self.allocator)});
                    }
                },
                .topic_body => |payload| {
                    try self.connection.dispatcher.reply(item.socket, .ack);

                    switch (try source_cache.update(payload)) {
                        .expired => {
                            traceLog.debug("[{s}] Content expired: {s}", .{APP_CONTEXT, payload.header.path});
                        },
                        .missing => {
                            traceLog.debug("[{s}] Waiting left content: {s}", .{APP_CONTEXT, payload.header.path});
                        },
                        .fulfil => {
                            traceLog.debug("[{s}] Source is ready: {s}", .{APP_CONTEXT, payload.header.name});
                            if (try source_cache.ready(payload.header)) {
                                try self.connection.dispatcher.post(.ready_topic_body);
                            }
                        },
                    }
                },
                .invalid_topic_body => |payload| {
                    log(payload.log_level, payload.log_content);

                    try source_cache.dismiss(payload.header);

                    if ((state == .terminating) and (source_cache.cache.count() == 0)) {
                        traceLog.debug("[{s}] No more sources", .{APP_CONTEXT});
                        try self.connection.dispatcher.reply(item.socket, .quit);
    
                        try self.connection.dispatcher.post(.finish_topic_body);
                    }
                    else {
                        traceLog.debug("[{s}] Wait receive next source", .{APP_CONTEXT});
                        try self.connection.dispatcher.reply(item.socket, .ack);
                    }
                },
                .ready_generate => {
                    if (source_cache.ready_queue.dequeue()) |source| {
                        defer source.deinit();
                        traceLog.debug("[{s}] Send source: {s}", .{APP_CONTEXT, source.header.name});
                        try self.connection.dispatcher.reply(item.socket, .{.topic_body = try source.clone(self.allocator)});
                    }
                    else if ((state == .terminating) and (source_cache.cache.count() == 0)) {
                        traceLog.debug("[{s}] No more sources", .{APP_CONTEXT});
                        try self.connection.dispatcher.reply(item.socket, .quit);
                    }
                    else {
                        traceLog.debug("[{s}] Wait receive next source", .{APP_CONTEXT});
                        try self.connection.dispatcher.reply(item.socket, .ack);
                    }
                },
                .end_watch_path => {
                    traceLog.debug("[{s}] Received finished somewhere", .{APP_CONTEXT});
                    if (oneshot) {
                        try self.connection.dispatcher.reply(item.socket, .quit);
                        state = .terminating;

                        try self.connection.dispatcher.post(.end_watch_path);
                    }
                    else {
                        try self.connection.dispatcher.reply(item.socket, .ack);
                    }
                },
                .finish_topic_body => {
                    if (state == .terminating) {
                        try self.connection.dispatcher.reply(item.socket, .quit);
    
                        try self.connection.dispatcher.post(.finish_topic_body);
                    }
                    else {
                        try self.connection.dispatcher.reply(item.socket, .ack);
                    }
                },
                .quit_accept => |payload| {
                    try self.connection.dispatcher.reply(item.socket, .ack);

                    left_launched -= 1;
                    traceLog.debug("[{s}] Quit acceptrd: {s} (left: {})", .{APP_CONTEXT, payload.stage_name, left_launched});

                    if (left_launched <= 0) {
                        traceLog.debug("[{s}] All Quit acceptrd", .{APP_CONTEXT});
                        try self.connection.dispatcher.done();
                    }
                },
                .log => |payload| {
                    try self.connection.dispatcher.reply(item.socket, .ack);
                    log(payload.level, payload.content);
                },
                else => {
                    try self.connection.dispatcher.reply(item.socket, .ack);
                    systemLog.debug("[{s}] Discard command: {}", .{APP_CONTEXT, std.meta.activeTag(item.event)});
                },
            }
        }
    }

    systemLog.debug("[{s}] terminated", .{APP_CONTEXT});
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