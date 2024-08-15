const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");

const Setting = @import("./Setting.zig");
const app_context = @import("build_options").app_context;

const Symbol = core.Symbol;
const Connection = core.sockets.Connection.Client(app_context, void);
const Logger = core.Logger.withAppContext(app_context);

allocator: std.mem.Allocator,
context: zmq.ZContext,
connection: *Connection,
logger: Logger,

const Self = @This();

pub fn init(allocator: std.mem.Allocator, setting: Setting) !Self {
    var ctx = try zmq.ZContext.init(allocator);

    var connection = try Connection.init(allocator, &ctx);

    // try connection.subscribe_socket.socket.setSocketOption(.{.Subscribe=""});

    try connection.subscribe_socket.addFilters(.{
        .ready_watch_path = true,
        .quit = true,
        .quit_all = true,
    });
    try connection.connect(setting.endpoints);

    return .{
        .allocator = allocator,
        .context = ctx,
        .connection = connection,
        .logger = Logger.init(allocator, connection.dispatcher, setting.standalone),
    };
}

pub fn deinit(self: *Self) void {
    self.connection.deinit();
    self.context.deinit();
}

pub fn run(self: *Self, setting: Setting) !void {
    try self.logger.log(.debug, "Beginning...", .{});
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
        try self.connection.dispatcher.post(.launched);
        break :launch;
    }

    try self.connection.dispatcher.state.ready();

    while (self.connection.dispatcher.isReady()) {
        self.waitNextDispatch(setting) catch {
            if (true) {
            try self.connection.dispatcher.postFatal(@errorReturnTrace());
            }
        };
    }
}

fn waitNextDispatch(self: *Self, setting: Setting) !void {
    const _item = self.connection.dispatcher.dispatch() catch |err| switch (err) {
        error.InvalidResponse => {
            try self.logger.log(.warn, "Unexpected data received", .{});
            return;
        },
        else => return err,
    };

    if (_item) |item| {
        defer item.deinit();
        
        switch (item.event) {
            .ready_watch_path => {
                try self.sendAllFiles(setting.sources);
                try self.connection.dispatcher.post(.finish_watch_path);
            },
            .quit => {
                try self.connection.dispatcher.quitAccept();
            },
            .quit_all => {
                try self.connection.dispatcher.quitAccept();
                try self.connection.pull_sink_socket.stop();
            },
            else => {
                try self.logger.log(.warn, "Discard command: {}", .{std.meta.activeTag(item.event)});
            },
        }
    }  
}

fn sendAllFiles(self: *Self, sources: []const Setting.SourceDir) !void {
    for (sources) |src| {
        const file_stat = try std.fs.cwd().statFile(src.dir_path);
        if (file_stat.kind == .file) {
            try self.sendFile(src.category, std.fs.cwd(), src.dir_path, src.dir_path);
        }
        else if (file_stat.kind == .directory) {
            try self.sendFiledOfDir(src.category, src.dir_path);
        }
    }
}

fn sendFiledOfDir(self: *Self, category: core.TopicCategory, dir_path: core.FilePath) !void {
    var dir = try std.fs.cwd().openDir(dir_path, .{});
    defer dir.close();

    var iter = try dir.walk(self.allocator);
    defer iter.deinit();

    while (try iter.next()) |entry| {
        if (entry.kind == .file) {
            try self.sendFile(category, entry.dir, entry.basename, entry.path);
        }
    }
}

fn sendFile(self: *Self, category: core.TopicCategory, base_dir: std.fs.Dir, file_path: core.FilePath, name: core.FilePath) !void {
    const base_dir_path = try base_dir.realpathAlloc(self.allocator, ".");
    defer self.allocator.free(base_dir_path);

    const file_path_abs = try base_dir.realpathAlloc(self.allocator, file_path);
    defer self.allocator.free(file_path_abs);

    try self.logger.log(.debug, "Sending source file: `{s}`", .{file_path_abs});

    var file = try base_dir.openFile(file_path, .{});
    defer file.close();

    const hash = try makeHash(self.allocator, name, file);
    defer self.allocator.free(hash);

    // Send path, content, hash
    try self.connection.dispatcher.post(.{
        .source_path = try core.Event.Payload.SourcePath.init(
            self.allocator, .{category, name, file_path_abs, hash, 1}
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
