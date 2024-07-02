const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");

const Setting = @import("./Setting.zig");

const Symbol = core.Symbol;
const Connection = core.sockets.Connection.Client(APP_CONTEXT, void);

allocator: std.mem.Allocator,
context: zmq.ZContext,
connection: *Connection,
logger: core.Logger,

const Self = @This();

pub const APP_CONTEXT = @import("build_options").APP_CONTEXT;

pub fn init(allocator: std.mem.Allocator, setting: Setting) !Self {
    var ctx = try zmq.ZContext.init(allocator);

    var connection = try Connection.init(allocator, &ctx);
    try connection.subscribe_socket.addFilters(.{
        .begin_watch_path = true,
        .quit = true,
    });
    try connection.connect(setting.endpoints);

    return .{
        .allocator = allocator,
        .context = ctx,
        .connection = connection,
        .logger = core.Logger.init(allocator, APP_CONTEXT, connection.dispatcher, setting.standalone),
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

    try self.connection.dispatcher.state.ready();

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
                .quit, .quit_all => {
                    try self.connection.dispatcher.post(.{
                        .quit_accept = try core.EventPayload.Stage.init(self.allocator, APP_CONTEXT),
                    });
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
            try self.sendFile(std.fs.cwd(), src.dir_path, src.prefix);
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
    defer iter.deinit();

    while (try iter.next()) |entry| {
        if (entry.kind == .file) {
            try self.sendFile(entry.dir, entry.path, prefix);
        }
    }
}

fn sendFile(self: *Self, base_dir: std.fs.Dir, file_path: core.FilePath, prefix: core.FilePath) !void {
    const file_path_abs = try base_dir.realpathAlloc(self.allocator, file_path);
    defer self.allocator.free(file_path_abs);

    try self.logger.log(.debug, "Sending source file: `{s}`", .{file_path_abs});

    const name = try std.fs.path.relative(self.allocator, prefix, file_path_abs);
    defer self.allocator.free(name);

    var file = try base_dir.openFile(file_path, .{});
    defer file.close();

    const hash = try makeHash(self.allocator, name, file);
    defer self.allocator.free(hash);

    // Send path, content, hash
    try self.connection.dispatcher.post(.{
        .source_path = try core.EventPayload.SourcePath.init(
            self.allocator, name, file_path_abs, hash, 1
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

    return core.bytesToHexAlloc(allocator, &hasher.finalResult());
}
