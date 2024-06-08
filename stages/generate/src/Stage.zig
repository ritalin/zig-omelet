const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");

const APP_CONTEXT = "generate-ts";
const Self = @This();

allocator: std.mem.Allocator,
context: zmq.ZContext,
sender_socket: *zmq.ZSocket,
req_socket: *zmq.ZSocket,
receiver_socket: *zmq.ZSocket,

pub fn init(allocator: std.mem.Allocator) !Self {
    var ctx = try zmq.ZContext.init(allocator);

    const receiver_socket = try zmq.ZSocket.init(zmq.ZSocketType.Sub, &ctx);
    try core.addSubscriberFilters(receiver_socket, .{
        .topic_payload = true,
        .next_generate = true,
        .end_generate = true,
        .quit = true,
    });
    try receiver_socket.connect(core.CMD_S2C_END_POINT);

    const sender_socket = try zmq.ZSocket.init(zmq.ZSocketType.Push, &ctx);
    try sender_socket.connect(core.CMD_C2S_END_POINT);

    const req_socket = try zmq.ZSocket.init(zmq.ZSocketType.Req, &ctx);
    try req_socket.connect(core.REQ_C2S_END_POINT);

    return .{
        .allocator = allocator,
        .context = ctx,
        .sender_socket = sender_socket,
        .req_socket = req_socket,
        .receiver_socket = receiver_socket,
    };
}

pub fn deinit(self: *Self) void {
    self.req_socket.deinit();
    self.sender_socket.deinit();
    self.receiver_socket.deinit();
    self.context.deinit();
}

pub fn run(self: *Self) !void {
    std.debug.print("({s}) Beginning\n", .{APP_CONTEXT});
    std.time.sleep(100_000);

    launch: {
        try core.sendEvent(self.allocator, self.sender_socket, .launched);
        std.debug.print("({s}) End send .launch\n", .{APP_CONTEXT});
        break :launch;
    }

    const polling = zmq.ZPolling.init(&[_]zmq.ZPolling.Item{
        zmq.ZPolling.Item.fromSocket(self.receiver_socket, .{ .PollIn = true }),
        zmq.ZPolling.Item.fromSocket(self.req_socket, .{ .PollIn = true }),
    });

    loop: while (true) {
        std.debug.print("({s}) Waiting...\n", .{APP_CONTEXT});

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const managed_allocator = arena.allocator();

        var it = try polling.poll(managed_allocator);
        defer it.deinit();

        while (it.next()) |item| {
            const ev = try core.receiveEventWithPayload(managed_allocator, item.socket);
            std.debug.print("({s}) Received command: {}\n", .{APP_CONTEXT, std.meta.activeTag(ev)});

            switch (ev) {
                .quit_all => {
                    std.debug.print("({s}) Begin Send quit accept (quit_all)\n", .{APP_CONTEXT});
                    try core.sendEvent(managed_allocator, self.sender_socket, .quit_accept);
                    std.debug.print("({s}) End Send quit accept (quit_all)\n", .{APP_CONTEXT});
                    break :loop;
                },
                .quit => {
                    try core.sendEvent(managed_allocator, self.req_socket, .quit_accept);
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
