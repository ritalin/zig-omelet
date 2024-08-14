const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");
const c = @import("worker_runtime");

const Self = @This();

allocator: std.mem.Allocator,
path: [:0]const u8,
database: c.DatabaseRef,
on_handle: *const fn (database: c.DatabaseRef, file_path: core.FilePath, query: core.Symbol, socket: *zmq.ZSocket) void,

pub fn init(allocator: std.mem.Allocator, category: core.TopicCategory, database: c.DatabaseRef, file_path: core.Symbol) !*Self {
    const self = try allocator.create(Self);
    self.* = .{
        .allocator = allocator,
        .path = try allocator.dupeZ(u8, file_path),
        .database = database,
        .on_handle = if (category == .source) SourceHandler.run else SchemaHandler.run,
    };

    return self;
}

pub fn deinit(self: *Self) void {
    self.allocator.free(self.path);
    self.allocator.destroy(self);
}

const ResultSet = struct { core.Symbol, core.Symbol, bool };

pub fn run(self: *Self, socket: *zmq.ZSocket) !void {
    var file = std.fs.cwd().openFile(self.path, .{}) catch |err| {
        const message = switch (err) {
            error.FileNotFound => try std.fmt.allocPrint(self.allocator, "File not found: {s}", .{self.path}),
            else => try std.fmt.allocPrint(self.allocator, "Invalid file: {s}", .{self.path}),
        };
        defer self.allocator.free(message);

        const log: core.Event = .{
            .log = try core.Event.Payload.Log.init(self.allocator, .{.err, message}),
        };
        defer log.deinit();
        try core.sendEvent(self.allocator, socket, "task", log);
        return;
    };
    defer file.close();
    var meta = try file.metadata();

    const query = try file.readToEndAlloc(self.allocator, meta.size());
    defer self.allocator.free(query);

    self.on_handle(self.database, self.path, query, socket);
}

pub const SourceHandler = struct {
    pub fn run(database: c.DatabaseRef, file_path: core.FilePath, query: core.Symbol, socket: *zmq.ZSocket) void {
        var collector: c.CollectorRef = undefined;
        _ = c.initSourceCollector(database, file_path.ptr, file_path.len, socket.socket_, &collector);
        defer c.deinitSourceCollector(collector);

        _ = c.executeDescribe(collector, query.ptr, query.len);
    }
};

pub const SchemaHandler = struct {
    pub fn run(database: c.DatabaseRef, file_path: core.FilePath, query: core.Symbol, socket: *zmq.ZSocket) void {
        var collector: c.CollectorRef = undefined;
        _ = c.initUserTypeCollector(database, file_path.ptr, file_path.len, socket.socket_, &collector);
        defer c.deinitUserTypeCollector(collector);

        _ = c.describeUserType(collector, query.ptr, query.len);
    }
};
