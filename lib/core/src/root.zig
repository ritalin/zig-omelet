const std = @import("std");

pub const types = @import("./types.zig");
pub const events = @import("./events/event_types.zig");

pub const sockets = struct {
    pub const Connection = struct {
        pub const Server = @import("sockets/connections/server.zig").Server;
        pub const Client = @import("sockets/connections/client.zig").Client;
    };
    pub const SendChannel = @import("./sockets/channels/send_channel.zig").SendChannel;
    pub const RpcChannel = @import("./sockets/channels/RpcChannel.zig");
    pub const ReceiveEntry = @import("./sockets/channels/ReceiveEntry.zig");
    pub const EventDispatcher = @import("./sockets/Dispatcher.zig");
};

pub const guest_phases = struct {
    pub const BootPhaseState = @import("./guest_phases/boot_phase.zig").BootPhaseState;
};

pub const Logger = @import("./Logger.zig");

// TODO:
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

pub const test_support = @import("./supports/test_support.zig");

test "All tests" {
    std.testing.refAllDecls(@This());

    const run_catch2 = @import("test_runner").run_catch2;
    try std.testing.expectEqual(0, try run_catch2(std.testing.io));
}
