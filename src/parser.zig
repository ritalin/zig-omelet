const std = @import("std");
const zzmq = @import("zzmq");

extern fn dmpParseedSQL(query: [*c]const u8, len: usize) callconv(.C) void;
extern fn duckDbParseSQL(query: [*c]const u8, len: usize, socket: *anyopaque) callconv(.C) void;

pub fn parse_sql(allocator: std.mem.Allocator, query: ?[]const u8) !void {
    const ctx = try allocator.create(zzmq.ZContext);
    ctx.* = try zzmq.ZContext.init(allocator);
    defer ctx.deinit();

    const INPROC_END_POINT: []const u8 = "inproc://#0";
    const IPC_IN_END_POINT: []const u8 = "ipc:///tmp/duckdb-extract-placeholder_pull";
    const IPC_OUT_END_POINT: []const u8 = "ipc:///tmp/duckdb-extract-placeholder_pub";
    // const IPC_OUT_END_POINT: []const u8 = "tcp://127.0.0.1:6002";

    // publisher
    const pub_socket = try zzmq.ZSocket.init(zzmq.ZSocketType.Pub, ctx);
    defer pub_socket.deinit();
    try pub_socket.bind(INPROC_END_POINT);

    if (query) |q0| {
        // w/o proxy
        const sub_socket = try zzmq.ZSocket.init(zzmq.ZSocketType.Sub, ctx);
        defer sub_socket.deinit();
        try sub_socket.setSocketOption(.{.Subscribe = ""});
        try sub_socket.connect(INPROC_END_POINT);
    
        const q = try allocator.dupeZ(u8, q0);
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
    else {
        // inbound socket
        const pull_socket = try zzmq.ZSocket.init(zzmq.ZSocketType.Pull, ctx);
        defer pull_socket.deinit();
        try pull_socket.bind(IPC_IN_END_POINT);

        // with proxy
        const sub_proxy_socket = try zzmq.ZSocket.init(zzmq.ZSocketType.Sub, ctx);
        defer sub_proxy_socket.deinit();
        try sub_proxy_socket.setSocketOption(.{.Subscribe = ""});
        try sub_proxy_socket.connect(INPROC_END_POINT);

        const pub_proxy_socket = try zzmq.ZSocket.init(zzmq.ZSocketType.XPub, ctx);
        defer pub_proxy_socket.deinit();
        try pub_proxy_socket.bind(IPC_OUT_END_POINT);

        std.debug.print("Send test\n", .{});
        const test_push_socket = try zzmq.ZSocket.init(zzmq.ZSocketType.Push, ctx);
        defer test_push_socket.deinit();
        try test_push_socket.connect(IPC_IN_END_POINT);

        var push_payload = try zzmq.ZMessage.init(allocator, "select ?::varchar as name from Foo where v = ?::int");
        {
            defer push_payload.deinit();
            try test_push_socket.send(&push_payload, .{});
        }
        std.debug.print("Waiting\n", .{});
        
        const test_sub_socket = try zzmq.ZSocket.init(zzmq.ZSocketType.Sub, ctx);
        defer test_sub_socket.deinit();
        try test_sub_socket.setSocketOption(.{ .Subscribe = ""});
        try test_sub_socket.connect(IPC_OUT_END_POINT);

        const polling = zzmq.ZPolling.init(&[_]zzmq.ZPolling.Item {
            zzmq.ZPolling.Item.fromSocket(pull_socket, .{ .PollIn = true }),
            zzmq.ZPolling.Item.fromSocket(sub_proxy_socket, .{ .PollIn = true }),
            zzmq.ZPolling.Item.fromSocket(test_sub_socket, .{ .PollIn = true }),
        });

        while (true) {
            var it = try polling.poll(allocator);
            defer it.deinit();

            while (it.next()) |item| {
                if (item.socket == pull_socket) {
                    std.debug.print("Begin pull\n", .{});
                    var frame = try item.socket.receive(.{});
                    defer frame.deinit();

                    const q = try allocator.dupeZ(u8, try frame.data());
                    defer allocator.free(q);

                    std.debug.print("Begin parse\n", .{});
                    duckDbParseSQL(q.ptr, q.len, pub_socket.socket_);
                    std.debug.print("End parse\n", .{});
                }
                else if (item.socket == sub_proxy_socket) {
                    std.debug.print("Begin intercept sub\n", .{});
                    for (0..2) |_| {
                        topic: {
                            var frame = try item.socket.receive(.{});
                            try pub_proxy_socket.send(&frame.message_, .{ .more = true });
                            break :topic;
                        }
                        data: {
                            var frame = try item.socket.receive(.{});
                            try pub_proxy_socket.send(&frame.message_, .{});
                            break :data;
                        }
                    }
                    std.debug.print("End intercept sub\n", .{});
                }
                else if (item.socket == test_sub_socket) {
                    for (0..2) |_| {
                        topic: {
                            var frame = try item.socket.receive(.{});
                            defer frame.deinit();
                            std.debug.print("[Topic]: {s}\n", .{try frame.data()});
                            break :topic;
                        }
                        data: {
                            var frame = try item.socket.receive(.{});
                            defer frame.deinit();
                            std.debug.print("[data]: {s}\n", .{try frame.data()});
                            break :data;
                        }
                    }

                    std.debug.print("Message received\n", .{});
                }
            }
        }
    }
}