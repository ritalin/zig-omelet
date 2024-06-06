const std = @import("std");
const zmq = @import("zmq");

// (control) Client -> Server
const CTRL_C2S_END_POINT = "ipc:///tmp/duckdb-extract-placeholder_ctrl_c2s";
// (control) Server -> Client
const CTRL_S2C_END_POINT = "ipc:///tmp/duckdb-extract-placeholder_ctrl_s2c";

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    const ctx = try allocator.create(zmq.ZContext);
    ctx.* = try zmq.ZContext.init(allocator);
    defer ctx.deinit();

    const ctrl2c_socket = try zmq.ZSocket.init(zmq.ZSocketType.Pull, ctx);
    defer ctrl2c_socket.deinit();
    try ctrl2c_socket.bind(CTRL_S2C_END_POINT);

    const ctrl2s_socket = try zmq.ZSocket.init(zmq.ZSocketType.Push, ctx);
    defer ctrl2s_socket.deinit();
    try ctrl2s_socket.connect(CTRL_C2S_END_POINT);
}

test "simple test" {
    var list = std.ArrayList(i32).init(std.testing.allocator);
    defer list.deinit(); // try commenting this out and see if zig detects the memory leak!
    try list.append(42);
    try std.testing.expectEqual(@as(i32, 42), list.pop());
}
