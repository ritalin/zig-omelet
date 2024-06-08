const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");

const Symbol = core.Symbol;

const APP_CONTEXT = "runner";

pub fn run(allocator: std.mem.Allocator, stage_count: struct { watch: usize, extract: usize, generate: usize }) !void {
    std.debug.print("({s}) Beginning\n", .{APP_CONTEXT});

    const oneshot = true;

    var ctx = try zmq.ZContext.init(allocator);
    defer ctx.deinit();

    const cmd_c2s_socket = try zmq.ZSocket.init(zmq.ZSocketType.Pull, &ctx);
    defer cmd_c2s_socket.deinit();
    try cmd_c2s_socket.bind(core.CMD_C2S_END_POINT);

    const cmd_s2c_socket = try zmq.ZSocket.init(zmq.ZSocketType.Pub, &ctx);
    defer cmd_s2c_socket.deinit();
    try cmd_s2c_socket.bind(core.CMD_S2C_END_POINT);

    var ctx2 = try zmq.ZContext.init(allocator);
    defer ctx2.deinit();

    // const src_c2s_socket = try zmq.ZSocket.init(zmq.ZSocketType.Pull, &ctx2);
    // defer src_c2s_socket.deinit();
    // try src_c2s_socket.bind(core.SRC_C2S_END_POINT);

    // const sub_socket = try zmq.ZSocket.init(zmq.ZSocketType.Pull, &ctx);
    // defer sub_socket.deinit();
    // try sub_socket.bind(IPC_OUT_END_POINT);

    std.time.sleep(1);

    ack_launch: {
        var left_count = stage_count.watch + stage_count.extract + stage_count.generate;

        while (left_count > 0) {
            std.debug.print("({s}) Wait launching ({})\n", .{ APP_CONTEXT, left_count });
            const ev = try core.receiveEventType(cmd_c2s_socket);

            if (ev == .launched) {
                left_count -= 1;
            }
        }

        std.debug.print("({s}) End sync launch\n", .{APP_CONTEXT});
        break :ack_launch;
    }

    sync_topic: {
        try core.sendEvent(allocator, cmd_s2c_socket, .begin_topic);
        break :sync_topic;
    }

    var topics = std.BufSet.init(allocator);
    defer topics.deinit();

    ack_topic: {
        const topic_polling = zmq.ZPolling.init(&[_]zmq.ZPolling.Item{
            zmq.ZPolling.Item.fromSocket(cmd_c2s_socket, .{ .PollIn = true }),
        });

        var left_count = stage_count.extract;

        loop: while (true) {
            var arena = std.heap.ArenaAllocator.init(allocator);
            defer arena.deinit();
            const managed_allocator = arena.allocator();

            std.debug.print("({s}) Wait sync topic ({})\n", .{APP_CONTEXT, left_count});

            var it = try topic_polling.poll(managed_allocator);
            defer it.deinit();

            while (it.next()) |item| {
                const ev = try core.receiveEventWithPayload(managed_allocator, item.socket);

                switch (ev) {
                    .topic => |payload| {
                        std.debug.print("Receive topic: {s}\n", .{payload.name});
                        try topics.insert(payload.name);
                    },
                    .end_topic => {
                        left_count -= 1;
                        if (left_count <= 0) {
                            break :loop;
                        }
                    },
                    else => {},
                }
            }
        }
        std.debug.print("({s}) End sync topic \n", .{APP_CONTEXT});
        break :ack_topic;
    }

    dumpTopics(topics);

    try core.sendEvent(allocator, cmd_s2c_socket, .begin_session);

    main_loop: {
        const main_polling = zmq.ZPolling.init(&[_]zmq.ZPolling.Item{
            zmq.ZPolling.Item.fromSocket(cmd_c2s_socket, .{ .PollIn = true }),
            // TODO response_socket
        });

        var left_launched = stage_count.extract + stage_count.generate;
        
        var source_payloads = try PayloadCacheManager.init(allocator);
        defer source_payloads.deinit();

        while (true) {
            var arena = std.heap.ArenaAllocator.init(allocator);
            defer arena.deinit();
            const managed_allocator = arena.allocator();

            std.debug.print("({s}) Waiting...\n", .{APP_CONTEXT});

            var it = try main_polling.poll(managed_allocator);
            defer it.deinit();

            while (it.next()) |item| {
                const ev = try core.receiveEventWithPayload(managed_allocator, item.socket);
            
                switch (ev) {
                    .source => |payload| {
                        try source_payloads.resetExpired(payload.hash, payload.path);
                        std.debug.print("({s}) Received source path: {s}, hash: {s}\n", .{APP_CONTEXT, payload.path, payload.hash});

                        try core.sendEventWithPayload(allocator, cmd_s2c_socket, .source, &[_]Symbol{payload.path, payload.content, payload.hash});
                    },
                    .finished => {
                        if (oneshot) {
                            // TODO Need to send quit event
                            left_launched -= 1;
                            std.debug.print("({s}) Left connected ({})\n", .{APP_CONTEXT, left_launched});
                            break :main_loop;
                        }
                    },
                    else => {},
                }
            }
        }
        break :main_loop;
    }

    std.debug.print("Runner terminated\n", .{});
}

fn dumpTopics(topics: std.BufSet) void {
    std.debug.print("({s}) Received topics ({}): ", .{APP_CONTEXT, topics.count()});

    var it = topics.iterator();

    while (it.next()) |topic| {
        std.debug.print("{s}, ", .{topic.*});
    }
    std.debug.print("\n", .{});
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