const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");

const APP_CONTEXT = "generate-ts";

pub fn run(allocator: std.mem.Allocator) !void {
    std.debug.print("({s}) Beginning\n", .{APP_CONTEXT});

    var ctx = try zmq.ZContext.init(allocator);
    defer ctx.deinit();

    const cmd_s2c_socket = try zmq.ZSocket.init(zmq.ZSocketType.Sub, &ctx);
    defer cmd_s2c_socket.deinit();
    try core.addSubscriberFilters(cmd_s2c_socket, .{
        .topic_payload = true,
        .next_generate = true,
        .end_generate = true,
        .quit = true,
    });
    try cmd_s2c_socket.connect(core.CMD_S2C_END_POINT);

    const cmd_c2s_socket = try zmq.ZSocket.init(zmq.ZSocketType.Push, &ctx);
    defer cmd_c2s_socket.deinit();
    try cmd_c2s_socket.connect(core.CMD_C2S_END_POINT);

    std.time.sleep(1000);

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

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const managed_allocator = arena.allocator();

        var it = try polling.poll(managed_allocator);
        defer it.deinit();

        while (it.next()) |item| {
            const ev = try core.receiveEventWithPayload(managed_allocator, item.socket);
            std.debug.print("({s}) Command: {}\n", .{ APP_CONTEXT, ev });

            switch (ev) {
                .quit => {
                    try core.sendEvent(managed_allocator, cmd_c2s_socket, .quit_accept);
                    break :loop;
                },
                else => {}
            }
        }
    }
}
