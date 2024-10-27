const std = @import("std");
const testing = std.testing;

pub usingnamespace @import("./types.zig");

pub const sockets = .{
    .Connection = @import("./sockets/Connection.zig"),
    .SubscribeSocket = @import("./sockets/SubscribeSocket.zig"),
};

pub const Queue = @import("./Queue.zig").Queue;
pub const Logger = @import("./Logger.zig");

pub const CborStream = @import("./CborStream.zig");

pub usingnamespace @import("./events/events.zig");

pub const settings = struct {
    pub usingnamespace @import("./settings/types.zig");
    pub usingnamespace @import("./settings/help.zig");
    pub usingnamespace @import("./settings/supports.zig");
};
pub const configs = struct {
    pub usingnamespace @import("./configs/types.zig");
    pub usingnamespace @import("./configs/supports.zig");
};

pub const DebugEndPoint = @import("./builder_supports/DebugEndpoint.zig");

test "All tests" {
    testing.refAllDecls(@This());
}