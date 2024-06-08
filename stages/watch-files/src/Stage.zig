const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");

const Symbol = core.Symbol;

const SOURCE_NAME = "/sql/master/Foo.sql";
const SOURCE_PREFIX = "/sql";
const SQL = "select $id::bigint, $name::varchar from foo where kind = $kind::int";

const APP_CONTEXT = "watch-files";

allocator: std.mem.Allocator,
context: zmq.ZContext,
sender_socket: *zmq.ZSocket,
receiver_socket: *zmq.ZSocket,

const Self = @This();

pub fn init(allocator: std.mem.Allocator) !Self {
    var ctx = try zmq.ZContext.init(allocator);

    const receiver_socket = try zmq.ZSocket.init(zmq.ZSocketType.Sub, &ctx);
    try core.addSubscriberFilters(receiver_socket, .{
        .begin_session = true,
        .quit = true,
    });
    
    const sender_socket = try zmq.ZSocket.init(zmq.ZSocketType.Push, &ctx);

    return .{
        .allocator = allocator,
        .context = ctx,
        .sender_socket = sender_socket,
        .receiver_socket = receiver_socket,
    };
}

pub fn deinit(self: *Self) void {
    self.sender_socket.deinit();
    self.receiver_socket.deinit();
    self.context.deinit();
}

pub fn run(self: *Self) !void {
    std.debug.print("({s}) Beginning\n", .{APP_CONTEXT});

    try self.receiver_socket.connect(core.CMD_S2C_END_POINT);
    try self.sender_socket.connect(core.CMD_C2S_END_POINT);

    std.time.sleep(1000);

    launch: {
        try core.sendEvent(self.allocator, self.sender_socket, .launched);
        std.debug.print("({s}) End send .launch\n", .{APP_CONTEXT});
        break :launch;
    }

    const polling = zmq.ZPolling.init(&[_]zmq.ZPolling.Item{
        zmq.ZPolling.Item.fromSocket(self.receiver_socket, .{ .PollIn = true }),
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
            std.debug.print("({s}) Command: {}\n", .{ APP_CONTEXT, ev });

            switch (ev) {
                .begin_session => {
                    try self.sendAllFiles();
                },
                .quit => {
                    try core.sendEvent(managed_allocator, self.sender_socket, .quit_accept);
                    break :loop;
                },
                else => {}
            }
        }
    }
}

fn sendAllFiles(self: *Self) !void {
    const name = try std.fs.path.relative(self.allocator, SOURCE_NAME, SOURCE_PREFIX);
    defer self.allocator.free(name);
    const hash = try makeHash(self.allocator, name, SQL);
    defer self.allocator.free(hash);

    // Send path, content, hash
    try core.sendEventWithPayload(self.allocator, self.sender_socket, .source, &[_]Symbol{name, SQL, hash});
    
    try core.sendEvent(self.allocator, self.sender_socket, .finished);
}

const Hasher = std.crypto.hash.sha2.Sha256;

fn makeHash(allocator: std.mem.Allocator, file_path: []const u8, content: []const u8) !Symbol {
    var hasher = Hasher.init(.{});

    hasher.update(file_path);

    // var buf: [8192]u8 = undefined;

    // while (true) {
        // const read_size = try file.read(&buf);
        // if (read_size == 0) break;

        // hasher.update(buf[0..read_size]);
        hasher.update(content);
    // }

    return bytesToHexAlloc(allocator, &hasher.finalResult());
}

fn bytesToHexAlloc(allocator: std.mem.Allocator, input: []const u8) ![]const u8 {
    var result = try allocator.alloc(u8, input.len * 2);
    if (input.len == 0) return result;

    const charset = "0123456789" ++ "abcdef";

    for (input, 0..) |b, i| {
        result[i * 2 + 0] = charset[b >> 4];
        result[i * 2 + 1] = charset[b & 15];
    }
    return result;
}