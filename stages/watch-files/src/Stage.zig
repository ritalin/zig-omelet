const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");

const Symbol = core.Symbol;

const SOURCE_NAME = "/sql/master/Foo.sql";
const SOURCE_PREFIX = "/sql";
const PATH = "/path/to/sql/master/Foo.sql";
const SQL = "select $id::bigint, $name::varchar from foo where kind = $kind::int";

const APP_CONTEXT = "watch-files";

allocator: std.mem.Allocator,
context: zmq.ZContext,
connection: *core.sockets.Connection.Client,
logger: core.Logger,

const Self = @This();

pub fn init(allocator: std.mem.Allocator, settings: struct { stand_alone: bool }) !Self {
    var ctx = try zmq.ZContext.init(allocator);

    var connection = try core.sockets.Connection.Client.init(allocator, &ctx);
    try connection.subscribe_socket.addFilters(.{
        .begin_session = true,
        .quit = true,
    });
    try connection.connect();

    return .{
        .allocator = allocator,
        .context = ctx,
        .connection = connection,
        .logger = core.Logger.init(allocator, APP_CONTEXT, connection, settings.stand_alone),
    };
}

pub fn deinit(self: *Self) void {
    self.connection.deinit();
    self.context.deinit();
}

pub fn run(self: *Self) !void {
    try self.logger.log(.info, "Beginning...", .{});
    try self.logger.log(.debug, "Subscriber filters: {}", .{self.connection.subscribe_socket.listFilters()});

    launch: {
        try self.connection.dispatcher.post(.{.launched = .{.stage_name = APP_CONTEXT} });
        break :launch;
    }

     while (self.connection.dispatcher.isReady()) {
        const _item = self.connection.dispatcher.dispatch() catch |err| switch (err) {
            error.InvalidResponse => {
                try self.logger.log(.warn, "Unexpected data received", .{});
                continue;
            },
            else => return err,
        };

        if (_item) |item| {
            defer item.deinit();
            
            switch (item.event) {
                .begin_session => {
                    try self.sendAllFiles();
                    try self.connection.dispatcher.post(.finished);
                },
                .quit_all => {
                    try self.connection.dispatcher.post(.{.quit_accept = .{ .stage_name = APP_CONTEXT }});
                    try self.connection.dispatcher.done();
                },
                .quit => {
                    try self.connection.dispatcher.approve();
                    try self.connection.dispatcher.post(.{.quit_accept = .{ .stage_name = APP_CONTEXT }});
                    try self.connection.dispatcher.done();
                },
                else => {
                    try self.logger.log(.warn, "Discard command: {}", .{std.meta.activeTag(item.event)});
                },
            }
        }
    }
}

fn sendAllFiles(self: *Self) !void {
    const name = try std.fs.path.relative(self.allocator, SOURCE_PREFIX, SOURCE_NAME);
    defer self.allocator.free(name);
    const hash = try makeHash(self.allocator, name, SQL);
    defer self.allocator.free(hash);
    // std.debug.print("[DEBUG] Generated hash: {s}\n", .{hash});

    // Send path, content, hash
    try self.connection.dispatcher.post(.{
        .source = .{
            .name = name, .path = PATH, .hash = hash
        }
    });
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