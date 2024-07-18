const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");
const c = @import("./duckdb_worker.zig");

const Self = @This();

allocator: std.mem.Allocator,
path: [:0]const u8,
database: c.DatabaseRef,

pub fn init(allocator: std.mem.Allocator, database: c.DatabaseRef, file_path: core.Symbol) !*Self {
    const self = try allocator.create(Self);
    self.* = .{
        .allocator = allocator,
        .path = try allocator.dupeZ(u8, file_path),
        .database = database,
    };

    return self;
}

pub fn deinit(self: *Self) void {
    self.allocator.free(self.path);
    self.allocator.destroy(self);
}

const ResultSet = struct { core.Symbol, core.Symbol, bool };

pub fn run(self: *Self, socket: *zmq.ZSocket) !void {
    var collector: c.CollectorRef = undefined;
    _ = c.initCollector(self.database, self.path.ptr, self.path.len, socket.socket_, &collector);
    defer c.deinitCollector(collector);

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

    c.executeDescribe(collector, query.ptr, query.len);

    // const result_set: []const ResultSet = &.{
    //     .{"a", "INTEGER", true },
    //     .{"b", "VARCHAR", false },
    // };

    // var writer = try core.CborStream.Writer.init(self.allocator);
    // defer writer.deinit();

    // _ = try writer.writeSlice(ResultSet, result_set);

    // defer socket.deinit();

    // return self.allocator.dupe(u8, writer.buffer.items);
    // const event: core.Event = .{
    //     .worker_result = try core.EventPayload.WorkerResult.init(self.allocator, writer.buffer.items),
    // };
}