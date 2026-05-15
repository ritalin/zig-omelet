const std = @import("std");

pub const types = @import("./types.zig");
pub const events = @import("./events/event_types.zig");

pub const sockets = struct {
    pub const Connection = struct {
        pub const Server = @import("sockets/connections/server.zig").Server;
    };
    pub const SendChannel = @import("./sockets/channels/SendChannel.zig");
    // .Connection = @import("./sockets/Connection.zig"),
    // .SubscribeSocket = @import("./sockets/SubscribeSocket.zig"),
};


// pub const Queue = @import("./Queue.zig").Queue;
// pub const Logger = @import("./Logger.zig");

// pub const CborStream = @import("./CborStream.zig");

// pub usingnamespace @import("./events/events.zig");

// pub const settings = struct {
//     pub usingnamespace @import("./settings/types.zig");
//     pub usingnamespace @import("./settings/help.zig");
//     pub usingnamespace @import("./settings/supports.zig");
// };
// pub const configs = struct {
//     pub usingnamespace @import("./configs/types.zig");
//     pub usingnamespace @import("./configs/supports.zig");
// };

// pub const DebugEndPoint = @import("./builder_supports/DebugEndpoint.zig");

// test "All tests" {
//     std.ArrayHashMapUnmanaged(comptime K: type, comptime V: type, comptime Context: type, comptime store_hash: bool)testing.refAllDecls(@This());
// }
