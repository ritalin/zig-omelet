const std = @import("std");

pub const types = @import("./types.zig");
pub const events = @import("./events/event_types.zig");

pub const sockets = struct {
    pub const Connection = struct {
        pub const Server = @import("sockets/connections/server.zig").Server;
    };
    pub const SendChannel = @import("./sockets/channels/SendChannel.zig");
    pub const ReceiveEntry = @import("./sockets/channels/ReceiveEntry.zig");
    pub const EventDispatcher = @import("./sockets/dispatcher.zig").EventDispatcher;
};


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

test "All tests" {
    std.testing.refAllDecls(@This());
    std.testing.refAllDecls(sockets);
    std.testing.refAllDecls(sockets.Connection);
}
