const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");

const Setting = @import("./Setting.zig");

const Symbol = core.Symbol;

const APP_CONTEXT = "watch-files";

allocator: std.mem.Allocator,
context: zmq.ZContext,
connection: *core.sockets.Connection.Client(void),
logger: core.Logger,

const Self = @This();

pub fn init(allocator: std.mem.Allocator, setting: Setting) !Self {
    var ctx = try zmq.ZContext.init(allocator);

    var connection = try core.sockets.Connection.Client(void).init(allocator, &ctx);
    try connection.subscribe_socket.addFilters(.{
        .begin_watch_path = true,
        .quit = true,
    });
    try connection.connect(setting.endpoints);

    return .{
        .allocator = allocator,
        .context = ctx,
        .connection = connection,
        .logger = core.Logger.init(allocator, APP_CONTEXT, connection.dispatcher, false),
    };
}

pub fn deinit(self: *Self) void {
    self.connection.deinit();
    self.context.deinit();
}

pub fn run(self: *Self, setting: Setting) !void {
    try self.logger.log(.info, "Beginning...", .{});
    try self.logger.log(.debug, "Subscriber filters: {}", .{self.connection.subscribe_socket.listFilters()});

    dump_setting: {
        try self.logger.log(.debug, "CLI: Req/Rep Channel = {s}", .{setting.endpoints.req_rep});
        try self.logger.log(.debug, "CLI: Pub/Sub Channel = {s}", .{setting.endpoints.pub_sub});

        for (setting.sources, 1..) |src, i| {
            try self.logger.log(.debug, "CLI: sources[{}] = {s}", .{i, src.dir_path});
        }
        try self.logger.log(.debug, "CLI: Watch mode = {}", .{setting.watch});
        break :dump_setting;
    }

    launch: {
        try self.connection.dispatcher.post(.{
            .launched = try core.EventPayload.Stage.init(self.allocator, APP_CONTEXT),
        });
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
                .begin_watch_path => {
                    try self.sendAllFiles(setting.sources);
                    try self.connection.dispatcher.post(.end_watch_path);
                },
                .quit_all => {
                    try self.connection.dispatcher.post(.{
                        .quit_accept = try core.EventPayload.Stage.init(self.allocator, APP_CONTEXT),
                    });
                    try self.connection.dispatcher.done();
                },
                .quit => {
                    try self.connection.dispatcher.approve();

                    try self.connection.dispatcher.post(.{
                        .quit_accept = try core.EventPayload.Stage.init(self.allocator, APP_CONTEXT),
                    });
                    try self.connection.dispatcher.done();
                },
                else => {
                    try self.logger.log(.warn, "Discard command: {}", .{std.meta.activeTag(item.event)});
                },
            }
        }
    }
}

fn sendAllFiles(self: *Self, sources: []const Setting.SourceDir) !void {
    for (sources) |src| {
        const file_stat = try std.fs.cwd().statFile(src.dir_path);
        if (file_stat.kind == .file) {
            try self.sendFile(src.dir_path, src.prefix);
        }
        else if (file_stat.kind == .directory) {
            try self.sendFiledOfDir(src.dir_path, src.prefix);
        }
    }
}

fn sendFiledOfDir(self: *Self, dir_path: core.FilePath, prefix: core.FilePath) !void {
    var dir = try std.fs.cwd().openDir(dir_path, .{});
    defer dir.close();

    var iter = try dir.walk(self.allocator);

    while (try iter.next()) |entry| {
        if (entry.kind == .file) {
            try self.sendFile(entry.path, prefix);
        }
    }
}

fn sendFile(self: *Self, file_path: core.FilePath, prefix: core.FilePath) !void {
    try self.logger.log(.debug, "Sending source file: `{s}`", .{file_path});

    const name = try std.fs.path.relative(self.allocator, prefix, file_path);
    defer self.allocator.free(name);

    var file = try std.fs.openFileAbsolute(file_path, .{});
    defer file.close();

    const hash = try makeHash(self.allocator, name, file);
    defer self.allocator.free(hash);
    std.debug.print("[DEBUG] name: {s}\n", .{name});

    // Send path, content, hash
    try self.connection.dispatcher.post(.{
        .source_path = try core.EventPayload.SourcePath.init(
            self.allocator, name, file_path, hash, 1
        ),
    });
}

const Hasher = std.crypto.hash.sha2.Sha256;

fn makeHash(allocator: std.mem.Allocator, file_path: []const u8, file: std.fs.File) !Symbol {
    var hasher = Hasher.init(.{});

    hasher.update(file_path);

    var buf: [8192]u8 = undefined;

    while (true) {
        const read_size = try file.read(&buf);
        if (read_size == 0) break;

        hasher.update(buf[0..read_size]);
    }

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