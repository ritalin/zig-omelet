const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");

const Symbol = core.Symbol;
const systemLog = core.Logger.Server.systemLog;
const traceLog = core.Logger.Server.traceLog;
const log = core.Logger.Server.log;

const APP_CONTEXT = "runner";
const Self = @This();

allocator: std.mem.Allocator,
context: zmq.ZContext,
connection: *core.sockets.Connection.Server,

pub fn init(allocator: std.mem.Allocator) !Self {
    var ctx = try zmq.ZContext.init(allocator);

    var connection = try core.sockets.Connection.Server.init(allocator, &ctx);
    try connection.bind();

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

pub fn run(self: *Self, stage_count: struct { watch: usize, extract: usize, generate: usize }) !void {
    systemLog.debug("[{s}] Launched", .{APP_CONTEXT});

    const oneshot = true;
    var left_launching = stage_count.watch + stage_count.extract + stage_count.generate;
    var left_topic_stage = stage_count.extract;
    var left_launched = stage_count.watch + stage_count.extract + stage_count.generate;
    
    var topics = std.BufSet.init(self.allocator);
    defer topics.deinit();

    var source_payloads = try PayloadCacheManager.init(self.allocator);
    defer source_payloads.deinit();

    while (self.connection.dispatcher.isReady()) {
        const _item = try self.connection.dispatcher.dispatch();

        if (_item) |*item| {
            defer item.deinit();

            traceLog.debug("[{s}] Received command: {}", .{APP_CONTEXT, std.meta.activeTag(item.event)});

            switch (item.event) {
                .launched => |payload| {
                    try self.connection.dispatcher.reply(item.socket, .ack);

                    left_launching -= 1;
                    traceLog.debug("Received launched: '{s}' (left: {})", .{payload.stage_name, left_launching});
                    if (left_launching <= 0) {
                        // collect topics
                        try self.connection.dispatcher.post(.begin_topic);
                    }
                },
                .topic => |payload| {
                    try self.connection.dispatcher.reply(item.socket, .ack);

                    traceLog.debug("[{s}] Receive 'topic': {s}", .{APP_CONTEXT, payload.name});
                    try topics.insert(payload.name);
                },
                .end_topic => {
                    try self.connection.dispatcher.reply(item.socket, .ack);

                    left_topic_stage -= 1;
                    traceLog.debug("[{s}] Receive 'end_topic' ({})", .{APP_CONTEXT, left_topic_stage});

                    if (left_topic_stage <= 0) {
                        try dumpTopics(self.allocator, topics);
                        try self.connection.dispatcher.post(.begin_session);
                    }
                },
                .source => |payload| {
                    try self.connection.dispatcher.reply(item.socket, .ack);

                    try source_payloads.resetExpired(payload.hash, payload.path);
                    traceLog.debug("[{s}] Received source name: {s}, path: {s}, hash: {s}", .{APP_CONTEXT, payload.name, payload.path, payload.hash});

                    try self.connection.dispatcher.post(.{.source = payload});
                },
                .topic_payload => |source| {
                    try self.connection.dispatcher.reply(item.socket, .ack);

                    // TODO
                    traceLog.err("[{s}] TODO Payload cache system not implemented...", .{APP_CONTEXT});

                    topics.remove(source.topic);

                    if (topics.count() > 0) {
                        traceLog.debug("[{s}] topics left ({})", .{APP_CONTEXT, topics.count()});
                        try self.connection.dispatcher.post(.next_generate);
                    }
                    else {
                        try self.connection.dispatcher.post(.end_generate);
                    }
                },
                .next_generate => {
                    try self.connection.dispatcher.reply(item.socket, .ack);
                    traceLog.err("[{s}] TODO Next cache sending is not implemented...", .{APP_CONTEXT});
                },
                .finished => {
                    traceLog.debug("[{s}] Received finished somewhere", .{APP_CONTEXT});
                    if (oneshot) {
                        try self.connection.dispatcher.reply(item.socket, .quit);
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
    cache: std.StringHashMap(Entry),

    pub fn init(allocator: std.mem.Allocator) !PayloadCacheManager {
        const arena = try allocator.create(std.heap.ArenaAllocator);
        arena.* = std.heap.ArenaAllocator.init(allocator);

        return .{
            .arena = arena,
            .cache = std.StringHashMap(Entry).init(allocator),
        };
    }

    pub fn deinit(self: *PayloadCacheManager) void {
        const child = self.arena.child_allocator;

        self.cache.deinit();
        self.arena.deinit();
        
        child.destroy(self.arena);
    }

    pub fn resetExpired(self: *PayloadCacheManager, hash: Symbol, path: Symbol) !void {
        var entry = try self.cache.getOrPut(path);

        if (entry.found_existing) {
            if (std.mem.eql(u8, entry.value_ptr.hash, hash)) return;

            entry.value_ptr.deinit();
        }

        entry.value_ptr.* = try Entry.init(self.arena.allocator(), hash);
    }

    pub const Entry = struct {
        allocator: std.mem.Allocator,
        hash: Symbol,
        contents: std.BufMap,

        pub fn init(allocator: std.mem.Allocator, hash: Symbol) !Entry {
            return .{
                .allocator = allocator,
                .hash = try allocator.dupe(u8, hash),
                .contents = std.BufMap.init(allocator),
            };
        }

        pub fn deinit(self: *Entry) void {
            self.contents.deinit();
            self.allocator.free(self.hash);
        }
    };
};
