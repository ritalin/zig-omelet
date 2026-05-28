const std = @import("std");

const BUFFER_SIZE = 4;

buffer: [BUFFER_SIZE]ReapTask = undefined,
select: std.Io.Select(ReapTask),

const Self = @This();

pub fn init(io: std.Io, allocator: std.mem.Allocator) !*Self {
    const self = try allocator.create(Self);

    self.* = .{
        .select = .init(io, &self.buffer),
    };

    return self;
}

pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
    self.select.cancelDiscard();
    allocator.destroy(self);
}

pub fn detach(self: *Self, function: anytype, args: std.meta.ArgsTuple(@TypeOf(function))) !void {
    try self.select.concurrent(.processed, function, args);
}

pub fn tick(self: *Self) !void {
    var results: [BUFFER_SIZE]ReapTask = undefined;
    try self.select.awaitMany(&results, 0);
}

const ReapTask = union(enum) {
    processed: std.Io.Cancelable!void,
};