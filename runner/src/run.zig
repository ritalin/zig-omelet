const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");

const APP_CONTEXT = "runner";

pub fn run(allocator: std.mem.Allocator, stage_count: struct { extract: usize, generate: usize }) !void {
    std.debug.print("({s}) Beginning\n", .{APP_CONTEXT});

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
        var left_count = stage_count.extract + stage_count.generate;

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

    const topic_polling = zmq.ZPolling.init(&[_]zmq.ZPolling.Item{
        zmq.ZPolling.Item.fromSocket(cmd_c2s_socket, .{ .PollIn = true }),
        // zmq.ZPolling.Item.fromSocket(src_c2s_socket, .{ .PollIn = true }),
    });

    var topics = std.BufSet.init(allocator);
    defer topics.deinit();

    ack_topic: {
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

    // TODO .start_session

    // loop: while (true) {
    //     topic: {
    //         var frame = try sub_socket.receive(.{});
    //         defer frame.deinit();

    //         std.debug.print("topic: {s}\n", .{try frame.data()});
    //         break :topic;
    //     }

    //     std.debug.print("Begin receive data \n", .{});
    //     data: {
    //         var frame = try sub_socket.receive(.{});
    //         defer frame.deinit();

    //         const msg = try frame.data();
    //         std.debug.print("data: {s}\n", .{msg});

    //         if (std.mem.eql(u8, msg, ".exit")) { break :loop; }

    //         break :data;
    //     }
    //     std.debug.print("End receive\n", .{});
    // }

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