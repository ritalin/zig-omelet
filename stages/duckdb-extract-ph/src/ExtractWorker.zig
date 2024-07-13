const std = @import("std");
const zmq = @import("zmq");

const c = @cImport({
    @cInclude("parse_duckdb.h");
});

pub const Topics = struct {
    pub const Id = c.TOPIC_ID;
    pub const Offset = c.TOPIC_OFFSET;
    pub const Query = c.TOPIC_QUERY;
    pub const Placeholder = c.TOPIC_PH;
    pub const Loglevel = c.TOPIC_LOGLEVEL;
    pub const LogContent = c.TOPIC_CONTENT;
};

const core = @import("core");

const Self = @This();

allocator: std.mem.Allocator,
path: [:0]const u8,

pub fn init(allocator: std.mem.Allocator, file_path: core.Symbol) !*Self {
    const self = try allocator.create(Self);
    self.* = .{
        .allocator = allocator,
        .path = try allocator.dupeZ(u8, file_path),
    };

    return self;
}

pub fn deinit(self: *Self) void {
    self.allocator.free(self.path);
    self.allocator.destroy(self);
}

pub fn run(self: *Self, socket: *zmq.ZSocket) !void {
    var handle: c.CollectorRef = undefined;
    _ = c.initCollector(self.path.ptr, self.path.len, socket.socket_, &handle);
    defer c.deinitCollector(handle);

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

    c.parseDuckDbSQL(handle, query.ptr, query.len);
}