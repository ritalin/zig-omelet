const std = @import("std");
const zzmq = @import("zzmq");

extern fn dmpParseedSQL(query: [*c]const u8, len: usize) callconv(.C) void;
extern fn duckDbParseSQL(query: [*c]const u8, len: usize, socket: *anyopaque) callconv(.C) void;

pub fn parse_sql(allocator: std.mem.Allocator, query: []const u8) !void {
    const ctx = try allocator.create(zzmq.ZContext);
    ctx.* = try zzmq.ZContext.init(allocator);
    defer ctx.deinit();

    const END_POINT: []const u8 = "inproc://#0";

    const sub_socket = try zzmq.ZSocket.init(zzmq.ZSocketType.Sub, ctx);
    defer sub_socket.deinit();
    try sub_socket.setSocketOption(.Subscribe);
    try sub_socket.connect(END_POINT);

    const pub_socket = try zzmq.ZSocket.init(zzmq.ZSocketType.Pub, ctx);
    defer pub_socket.deinit();
    try pub_socket.bind(END_POINT);

    const q = try allocator.dupeZ(u8, query);
    defer allocator.free(q);

    std.debug.print("Begin parse\n", .{});
    duckDbParseSQL(q.ptr, q.len, pub_socket.socket_);

    {
        var frame = try sub_socket.receive(.{});
        defer frame.deinit();

        std.debug.print("[1] Frame#hasmore: {}\n", .{frame.hasMore()});
        std.debug.print("[1] Frame#data: {s}\n", .{try frame.data()});
    }
    {
        var frame = try sub_socket.receive(.{});
        defer frame.deinit();

        std.debug.print("[2] Frame#hasmore: {}\n", .{frame.hasMore()});
        std.debug.print("[2] Frame#data: {s}\n", .{try frame.data()});
    }
    {
        var frame = try sub_socket.receive(.{});
        defer frame.deinit();

        std.debug.print("[3] Frame#hasmore: {}\n", .{frame.hasMore()});
        std.debug.print("[3] Frame#data: {s}\n", .{try frame.data()});
    }
    {
        var frame = try sub_socket.receive(.{});
        defer frame.deinit();

        std.debug.print("[4] Frame#hasmore: {}\n", .{frame.hasMore()});
        std.debug.print("[4] Frame#data: {s}\n", .{try frame.data()});
    }

    // dmpParseedSQL(q.ptr, q.len);
    std.debug.print("End parse\n", .{});
}