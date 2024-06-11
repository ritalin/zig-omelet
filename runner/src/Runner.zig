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
sender_socket: *zmq.ZSocket,
rep_socket: *zmq.ZSocket,
// receiver_socket: *zmq.ZSocket,

pub fn init(allocator: std.mem.Allocator) !Self {
    var ctx = try zmq.ZContext.init(allocator);

    // const receiver_socket = try zmq.ZSocket.init(zmq.ZSocketType.Pull, &ctx);
    // try receiver_socket.bind(core.CMD_C2S_END_POINT);

    const sender_socket = try zmq.ZSocket.init(zmq.ZSocketType.Pub, &ctx);
    try sender_socket.bind(core.CMD_S2C_END_POINT);

    const rep_socket = try zmq.ZSocket.init(zmq.ZSocketType.Rep, &ctx);
    try rep_socket.bind(core.REQ_C2S_END_POINT);

    return .{
        .allocator = allocator,
        .context = ctx,
        .sender_socket = sender_socket,
        .rep_socket = rep_socket,
        // .receiver_socket = receiver_socket,
    };
}

pub fn deinit(self: *Self) void {
    self.rep_socket.deinit();
    self.sender_socket.deinit();
    // self.receiver_socket.deinit();
    self.context.deinit();
}

pub fn run(self: *Self, stage_count: struct { watch: usize, extract: usize, generate: usize }) !void {
    systemLog.debug("[{s}] Beginning", .{APP_CONTEXT});

    const oneshot = true;

    ack_launch: {
        var left_count = stage_count.watch + stage_count.extract + stage_count.generate;

        while (left_count > 0) {
            systemLog.debug("[{s}] Wait launching ({})", .{ APP_CONTEXT, left_count });
            
            const ev = core.receiveEventWithPayload(self.allocator, self.rep_socket) catch |err| switch (err) {
                error.InvalidResponse => {
                    try core.sendEvent(self.allocator, self.rep_socket, .nack);
                    continue;
                },
                else => return err,
            };
            defer ev.deinit(self.allocator);

            try core.sendEvent(self.allocator, self.rep_socket, .ack);

            switch (ev) {
                .launched => |payload| {
                    traceLog.debug("Received launched ('{s}')", .{payload.stage_name});
                    left_count -= 1;
                },
                .log => |payload| {
                    log(payload.level, payload.content);
                },
                else => {
                    systemLog.debug("[{s}] Discard command: {} (wait launch)", .{APP_CONTEXT, std.meta.activeTag(ev)});
                },
            }
        }

        traceLog.debug("[{s}] End sync launch", .{APP_CONTEXT});
        break :ack_launch;
    }

    sync_topic: {
        try core.sendEvent(self.allocator, self.sender_socket, .begin_topic);
        break :sync_topic;
    }

    var topics = std.BufSet.init(self.allocator);
    defer topics.deinit();

    ack_topic: {
        var topic_polling = try zmq.ZPolling.init(self.allocator, &.{
            zmq.ZPolling.Item.fromSocket(self.rep_socket, .{ .PollIn = true }),
        });
        defer topic_polling.deinit();

        var left_count = stage_count.extract;

        loop: while (true) {
            traceLog.debug("[{s}] Wait sync topic ({})", .{APP_CONTEXT, left_count});

            var it = try topic_polling.poll();
            defer it.deinit();

            while (it.next()) |item| {
                const ev = core.receiveEventWithPayload(self.allocator, item.socket) catch |err| switch (err) {
                    error.InvalidResponse => {
                        try core.sendEvent(self.allocator, item.socket, .nack);
                        continue;
                    },
                    else => return err,
                };
                defer ev.deinit(self.allocator);

                try core.sendEvent(self.allocator, item.socket, .ack);

                switch (ev) {
                    .topic => |payload| {
                        traceLog.debug("[{s}] Receive topic: {s}", .{APP_CONTEXT, payload.name});
                        try topics.insert(payload.name);
                    },
                    .end_topic => {
 
                        left_count -= 1;
                        if (left_count <= 0) {
                            break :loop;
                        }
                    },
                    .log => |payload| {
                        log(payload.level, payload.content);
                    },
                    else => {
                        systemLog.debug("[{s}] Discard command: {} (wait topic)", .{APP_CONTEXT, std.meta.activeTag(ev)});
                    },
                }
            }
        }
        traceLog.debug("[{s}] End sync topic", .{APP_CONTEXT});
        break :ack_topic;
    }

    try dumpTopics(self.allocator, topics);

    try core.sendEvent(self.allocator, self.sender_socket, .begin_session);

    main_loop: {
        const main_polling = try zmq.ZPolling.init(self.allocator, &[_]zmq.ZPolling.Item{
            // zmq.ZPolling.Item.fromSocket(self.receiver_socket, .{ .PollIn = true }),
            zmq.ZPolling.Item.fromSocket(self.rep_socket, .{ .PollIn = true }),
        });

        var left_launched = stage_count.watch + stage_count.extract + stage_count.generate;
        
        var source_payloads = try PayloadCacheManager.init(self.allocator);
        defer source_payloads.deinit();

        while (true) {
            systemLog.debug("[{s}] Waiting...", .{APP_CONTEXT});

            var it = try main_polling.poll();
            defer it.deinit();

            while (it.next()) |item| {
                const ev = core.receiveEventWithPayload(self.allocator, item.socket) catch |err| switch (err) {
                    error.InvalidResponse => {
                        try core.sendEvent(self.allocator, item.socket, .nack);
                        continue;
                    },
                    else => return err,
                };
                defer ev.deinit(self.allocator);
                traceLog.debug("[{s}] Received command: {}", .{APP_CONTEXT, std.meta.activeTag(ev)});
            
                switch (ev) {
                    .source => |payload| {
                        try core.sendEvent(self.allocator, item.socket, .ack);

                        try source_payloads.resetExpired(payload.hash, payload.path);
                        traceLog.debug("[{s}] Received source name: {s}, path: {s}, hash: {s}", .{APP_CONTEXT, payload.name, payload.path, payload.hash});

                        try core.sendEvent(self.allocator, self.sender_socket, .{.source = payload});
                    },
                    .topic_payload => |source| {
                        try core.sendEvent(self.allocator, item.socket, .ack);

                        // TODO
                        traceLog.err("[{s}] TODO Payload cache system not implemented...", .{APP_CONTEXT});

                        topics.remove(source.topic);

                        if (topics.count() > 0) {
                            traceLog.debug("[{s}] topics left ({})", .{APP_CONTEXT, topics.count()});
                            // try core.sendEvent(self.allocator, self.sender_socket, .{.topic_payload = source});
                            try core.sendEvent(self.allocator, self.sender_socket, .next_generate);
                        }
                        else {
                            try core.sendEvent(self.allocator, self.sender_socket, .end_generate);
                        }
                    },
                    .next_generate => {
                        try core.sendEvent(self.allocator, item.socket, .ack);
                        traceLog.err("[{s}] TODO Next cache sending is not implemented...", .{APP_CONTEXT});
                    },
                    .finished => {
                        traceLog.debug("[{s}] Received finished somewhere", .{APP_CONTEXT});
                        if (oneshot) {
                            try core.sendEvent(self.allocator, self.rep_socket, .quit);
                        }
                        else {
                            try core.sendEvent(self.allocator, item.socket, .ack);
                        }
                    },
                    .quit_accept => |payload| {
                        try core.sendEvent(self.allocator, item.socket, .ack);

                        left_launched -= 1;
                        traceLog.debug("[{s}] Quit acceptrd: ({s}) ,Left: {}", .{APP_CONTEXT, payload.stage_name, left_launched});
                        if (left_launched <= 0) {
                            break :main_loop;
                        }
                    },
                    .log => |payload| {
                        try core.sendEvent(self.allocator, item.socket, .ack);
                        log(payload.level, payload.content);
                    },
                    else => {
                        try core.sendEvent(self.allocator, item.socket, .ack);
                        systemLog.debug("[{s}] Discard command: {}", .{APP_CONTEXT, std.meta.activeTag(ev)});
                    },
                }
            }
        }
        break :main_loop;
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
