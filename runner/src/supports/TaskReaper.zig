const std = @import("std");

const CancelatinToken = @import("../tasks/CancelationToken.zig");

const BUFFER_SIZE = 4;

buffer: [BUFFER_SIZE]ReapTask = undefined,
select: std.Io.Select(ReapTask),
shared_token: CancelatinToken = .{},

const Self = @This();

pub fn init(io: std.Io, allocator: std.mem.Allocator) !*Self {
    const self = try allocator.create(Self);

    self.* = .{
        .select = .init(io, &self.buffer),
    };

    return self;
}

pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
    self.shared_token.cancel();
    self.select.cancelDiscard();
    allocator.destroy(self);
}

pub fn detach(self: *Self, function: anytype, args: std.meta.ArgsTuple(@TypeOf(function))) std.Io.ConcurrentError!void {    
    var new_args: std.meta.ArgsTuple(@TypeOf(function)) = undefined;

    inline for (@typeInfo(@TypeOf(args)).@"struct".fields, 0..) |field, i| {
        if (i == 0) {
            if (@hasField(field.type, "cancel_token")) {
                var receiver = args[i];
                receiver.cancel_token = &self.shared_token;
                new_args[i] = receiver;
            }
        }
        else {
            new_args[i] = args[i];
        }
    }

    try self.select.concurrent(.processed, function, new_args);
}

pub fn tick(self: *Self) !void {
    var results: [BUFFER_SIZE]ReapTask = undefined;
    _ = try self.select.awaitMany(&results, 0);
}

pub fn cancel(self: *Self, io: std.Io) void {
    // self.select.group.cancel(io);
    std.debug.print("Reaper canceled\n", .{});
    _ = self;
    _ = io;
}

const ReapTask = union(enum) {
    processed: std.Io.Cancelable!void,
};