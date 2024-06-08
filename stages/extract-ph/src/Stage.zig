const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");

const APP_CONTEXT = "exctract-ph";

pub fn run(allocator: std.mem.Allocator) !void {
    std.debug.print("({s}) Beginning\n", .{APP_CONTEXT});

    var ctx = try zmq.ZContext.init(allocator);
    defer ctx.deinit();

    const cmd_s2c_socket = try zmq.ZSocket.init(zmq.ZSocketType.Sub, &ctx);
    defer cmd_s2c_socket.deinit();
    defer cmd_s2c_socket.deinit();
    try core.addSubscriberFilters(cmd_s2c_socket, .{
        .begin_topic = true,
        .source = true,
        .quit = true,
    });
    try cmd_s2c_socket.connect(core.CMD_S2C_END_POINT);

    const cmd_c2s_socket = try zmq.ZSocket.init(zmq.ZSocketType.Push, &ctx);
    defer cmd_c2s_socket.deinit();
    try cmd_c2s_socket.connect(core.CMD_C2S_END_POINT);

    const req_c2s_socket = try zmq.ZSocket.init(zmq.ZSocketType.Req, &ctx);
    defer req_c2s_socket.deinit();
    try req_c2s_socket.connect(core.REQ_C2S_END_POINT);

    std.time.sleep(100_000);

    launch: {
        try core.sendEvent(allocator, cmd_c2s_socket, .launched);
        std.debug.print("({s}) End send .launch\n", .{APP_CONTEXT});
        break :launch;
    }

    const polling = zmq.ZPolling.init(&[_]zmq.ZPolling.Item{
        zmq.ZPolling.Item.fromSocket(cmd_s2c_socket, .{ .PollIn = true }),
        zmq.ZPolling.Item.fromSocket(req_c2s_socket, .{ .PollIn = true }),
    });

    loop: while (true) {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const managed_allocator = arena.allocator();

        std.debug.print("({s}) Waiting...\n", .{APP_CONTEXT});

        var it = try polling.poll(managed_allocator);
        defer it.deinit();

        while (it.next()) |item| {
            const ev = try core.receiveEventWithPayload(managed_allocator, item.socket);
            std.debug.print("({s}) Received command: {}\n", .{APP_CONTEXT, std.meta.activeTag(ev)});

            switch (ev) {
                .begin_topic => {
                    topics: {
                        try core.sendEventWithPayload(allocator, cmd_c2s_socket, .topic, &[_]core.Symbol{"query"});
                        try core.sendEventWithPayload(allocator, cmd_c2s_socket, .topic, &[_]core.Symbol{"name"});
                        try core.sendEventWithPayload(allocator, cmd_c2s_socket, .topic, &[_]core.Symbol{"placeholder"});
                        break :topics;
                    }
                    ack_topics: {
                        try core.sendEvent(managed_allocator, cmd_c2s_socket, .end_topic);
                        break :ack_topics;
                    }

                    std.debug.print("({s}) End Send topics\n", .{APP_CONTEXT});
                },
                .source => {
                    std.debug.print("({s}) Recdeived source\n", .{APP_CONTEXT});
                },
                .quit_all => {
                    std.debug.print("({s}) Begin Send quit accept (quit_all)\n", .{APP_CONTEXT});
                    try core.sendEvent(managed_allocator, cmd_c2s_socket, .quit_accept);
                    std.debug.print("({s}) End Send quit accept (quit_all)\n", .{APP_CONTEXT});
                    break :loop;
                },
                .quit => {
                    try core.sendEvent(managed_allocator, req_c2s_socket, .quit_accept);
                    std.debug.print("({s}) End Send quit accept (quit)\n", .{APP_CONTEXT});
                    break :loop;
                },
                else => {
                    std.debug.print("({s}) Discard command: {}\n", .{APP_CONTEXT, std.meta.activeTag(ev)});
                },
            }
        }
    }
    std.debug.print("({s}) Terminated\n", .{APP_CONTEXT});
}
