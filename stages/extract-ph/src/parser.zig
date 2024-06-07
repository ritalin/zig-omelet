const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");

const APP_CONTEXT = "exctract-ph";

pub fn run(allocator: std.mem.Allocator) !void {
    std.debug.print("({s}) Beginning\n", .{APP_CONTEXT});

    const oneshot = true;

    var ctx = try zmq.ZContext.init(allocator);
    defer ctx.deinit();

    const cmd_s2c_socket = try zmq.ZSocket.init(zmq.ZSocketType.Sub, &ctx);
    defer cmd_s2c_socket.deinit();
    try cmd_s2c_socket.setSocketOption(.{ .Subscribe = "" });
    try cmd_s2c_socket.connect(core.CMD_S2C_END_POINT);

    const cmd_c2s_socket = try zmq.ZSocket.init(zmq.ZSocketType.Push, &ctx);
    defer cmd_c2s_socket.deinit();
    try cmd_c2s_socket.connect(core.CMD_C2S_END_POINT);

    // const src_c2s_socket = try zmq.ZSocket.init(zmq.ZSocketType.Push, &ctx);
    // defer src_c2s_socket.deinit();
    // try src_c2s_socket.connect(SRC_C2S_END_POINT);

    launch: {
        try core.sendEvent(allocator, cmd_c2s_socket, .launched);
        std.debug.print("({s}) End send .launch\n", .{APP_CONTEXT});
        break :launch;
    }

    const polling = zmq.ZPolling.init(&[_]zmq.ZPolling.Item{
        zmq.ZPolling.Item.fromSocket(cmd_s2c_socket, .{ .PollIn = true }),
    });

    loop: while (true) {
        std.debug.print("({s}) Waiting...\n", .{APP_CONTEXT});

        var it = try polling.poll(allocator);
        defer it.deinit();

        while (it.next()) |item| {
            var frame = try item.socket.receive(.{});
            defer frame.deinit();

            const command = try frame.data();
            std.debug.print("({s}) Command: {s}\n", .{ APP_CONTEXT, command });

            if (std.mem.eql(u8, command, ".begin_topic")) {
                // topics: {
                //     const topics = std.StaticStringMap(void).initComptime(.{
                //        .{ "name"}, .{"query"}, .{"placeholder"}
                //     });

                //     inline for (0..topics.kvs.len) |i| {
                //         topic: {
                //             var msg = try zmq.ZMessage.init(allocator, ".topic");
                //             defer msg.deinit();
                //             try src_c2s_socket.send(&msg, .{.more = true});
                //             break :topic;
                //         }
                //         data: {
                //             var msg = try zmq.ZMessage.init(allocator, topics.kvs.keys[i]);
                //             defer msg.deinit();
                //             try src_c2s_socket.send(&msg, .{});
                //             break :data;
                //         }
                //     }
                //     break :topics;
                // }

                ack_topics: {
                    try core.sendEvent(allocator, cmd_c2s_socket, .end_topic);
                    // var msg = try zmq.ZMessage.init(allocator, ".end_topic");
                    // defer msg.deinit();
                    // try cmd_c2s_socket.send(&msg, .{});
                    break :ack_topics;
                }

                std.debug.print("({s}) End Send topics\n", .{APP_CONTEXT});
            }

            parse: {
                break :parse;
            }

            if (oneshot) {
                end: {
                    var msg = try zmq.ZMessage.init(allocator, ".quit_accept");
                    defer msg.deinit();
                    try cmd_c2s_socket.send(&msg, .{});
                    break :end;
                }
                break :loop;
            } else if (std.mem.eql(u8, command, ".quit")) {
                end: {
                    var msg = try zmq.ZMessage.init(allocator, ".quit_accept");
                    defer msg.deinit();
                    try cmd_c2s_socket.send(&msg, .{});
                    break :end;
                }
                break :loop;
            }
        }
    }
    std.debug.print("({s}) Terminated\n", .{APP_CONTEXT});
}
