const std = @import("std");
const zmq = @import("zmq");

extern fn dmpParseedSQL(query: [*c]const u8, len: usize) callconv(.C) void;
extern fn duckDbParseSQL(query: [*c]const u8, len: usize, socket: *anyopaque) callconv(.C) void;

const IPC_SYNC_END_POINT: []const u8 = "ipc:///tmp/duckdb-extract-placeholder_sync";
const IPC_IN_END_POINT: []const u8 = "ipc:///tmp/duckdb-extract-placeholder_pull";
const IPC_OUT_END_POINT: []const u8 = "ipc:///tmp/duckdb-extract-placeholder_pub";
const INPROC_END_POINT: []const u8 = "inproc://#0";

pub fn parse_sql(allocator: std.mem.Allocator, query: ?[]const u8) !void {
    const ctx = try allocator.create(zmq.ZContext);
    ctx.* = try zmq.ZContext.init(allocator);
    defer ctx.deinit();

    // publisher
    const pub_socket = try zmq.ZSocket.init(zmq.ZSocketType.Pub, ctx);
    defer pub_socket.deinit();
    try pub_socket.bind(INPROC_END_POINT);

    if (query) |q0| {
        // w/o proxy
        const sub_socket = try zmq.ZSocket.init(zmq.ZSocketType.Sub, ctx);
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

        std.debug.print("End parse\n", .{});
    }
    else {
        // inbound socket
        const pull_socket = try zmq.ZSocket.init(zmq.ZSocketType.Pull, ctx);
        defer pull_socket.deinit();
        try pull_socket.bind(IPC_IN_END_POINT);

        // with proxy
        const sub_proxy_socket = try zmq.ZSocket.init(zmq.ZSocketType.Sub, ctx);
        defer sub_proxy_socket.deinit();
        try sub_proxy_socket.setSocketOption(.{.Subscribe = ""});
        try sub_proxy_socket.connect(INPROC_END_POINT);

        const pub_proxy_socket = try zmq.ZSocket.init(zmq.ZSocketType.Push, ctx);
        // const pub_proxy_socket = try zmq.ZSocket.init(zmq.ZSocketType.Pub, ctx);
        defer pub_proxy_socket.deinit();
        try pub_proxy_socket.bind(IPC_OUT_END_POINT);

        // sync
        const sync_socket = try zmq.ZSocket.init(zmq.ZSocketType.Rep, ctx);
        defer sync_socket.deinit();
        try sync_socket.bind(IPC_SYNC_END_POINT);

        const test_push_socket = try zmq.ZSocket.init(zmq.ZSocketType.Push, ctx);
        defer test_push_socket.deinit();
        try test_push_socket.connect(IPC_IN_END_POINT);

        const polling = zmq.ZPolling.init(&[_]zmq.ZPolling.Item {
            zmq.ZPolling.Item.fromSocket(pull_socket, .{ .PollIn = true }),
            zmq.ZPolling.Item.fromSocket(sub_proxy_socket, .{ .PollIn = true }),
            zmq.ZPolling.Item.fromSocket(sync_socket, .{ .PollIn = true }),
        });

        std.debug.print("Waiting...\n", .{});

        const dontwait = false;

        while (true) {
            var it = try polling.poll(allocator);
            defer it.deinit();

            // it.dump();

            while (it.next()) |item| {
                if (item.socket == sync_socket) {
                    var frame = try item.socket.receive(.{});
                    defer frame.deinit();
                    std.debug.print("Accept Sync-Req", .{});

                    sync_ack: {
                        var msg = try zmq.ZMessage.init(allocator, "");
                        try item.socket.send(&msg, .{});
                        break :sync_ack;
                    }
                    std.debug.print("End send Sync-Ack", .{});

                    push_sql: {
                        var msg = try zmq.ZMessage.init(allocator, "select ?::varchar as name from Foo where v = ?::int");
                        defer msg.deinit();
                        try test_push_socket.send(&msg, .{});
                        break :push_sql;
                    }
                    std.debug.print("End push SQL\n", .{});
                }
                else if (item.socket == pull_socket) {
                    std.debug.print("Begin pull\n", .{});
                    var frame = try item.socket.receive(.{});
                    defer frame.deinit();

                    const q = try allocator.dupeZ(u8, try frame.data());
                    defer allocator.free(q);

                    std.debug.print("Begin parse\n", .{});
                    duckDbParseSQL(q.ptr, q.len, pub_socket.socket_);
                    std.debug.print("End parse\n", .{});

                    terminate: {
                        var term_envelope = try zmq.ZMessage.init(allocator, "command");
                        defer term_envelope.deinit();
                        try pub_socket.send(&term_envelope, .{.dontwait = dontwait, .more = true});

                        var term_body = try zmq.ZMessage.init(allocator, ".exit");
                        defer term_body.deinit();
                        try pub_socket.send(&term_body, .{.dontwait = dontwait});
                        break :terminate;
                    }
                }
                else if (item.socket == sub_proxy_socket) {
                    std.debug.print("Begin intercept sub\n", .{});
                    {
                        topic: {
                            var frame = try item.socket.receive(.{});
                            std.debug.print("Begin send1 {s}\n", .{try frame.data()});
                            
                            try pub_proxy_socket.send(&frame.message_, .{ .dontwait = dontwait, .more = true });
                            break :topic;
                        }
                        data: {
                            var frame = try item.socket.receive(.{});
                            std.debug.print("Begin send2: {s}\n", .{try frame.data()});

                            try pub_proxy_socket.send(&frame.message_, .{.dontwait = dontwait});
                            std.debug.print("End send2\n", .{});
                            break :data;
                        }
                    }
                    std.debug.print("End intercept sub\n", .{});
                }
            }

            std.debug.print(".next\n", .{});
        }
    }
}