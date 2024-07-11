const std = @import("std");
const zmq = @import("zmq");
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

const ResultSet = struct { core.Symbol, core.Symbol, bool };

pub fn run(self: *Self, socket: *zmq.ZSocket) !core.Symbol {
    const result_set: []const ResultSet = &.{
        .{"a", "INTEGER", true },
        .{"b", "VARCHAR", true },
    };

    var writer = try core.CborStream.Writer.init(self.allocator);
    defer writer.deinit();

    _ = try writer.writeSlice(ResultSet, result_set);

    defer socket.deinit();

    return self.allocator.dupe(u8, writer.buffer.items);
    // const event: core.Event = .{
    //     .worker_result = try core.EventPayload.WorkerResult.init(self.allocator, writer.buffer.items),
    // };
}