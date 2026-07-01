const std = @import("std");

pub const server = @import("sockets/connections/server.zig");

// comptime {
//     _ = @import("./sockets/connections/server.zig").Server;
// }

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
pub const TaskReaper = @import("./supports/TaskReaper.zig");

pub const settings = struct {
    pub const types = @import("./settings/types.zig");
    pub const supports = @import("./settings/support.zig");
};
pub const configs = struct {
    pub const types =  @import("./configs/types.zig");
    pub const guests = @import("./configs/guest_args.zig");
    pub const supports = @import("./configs/supports.zig");
    pub const Endpoint = @import("./default_config/endpoint_support.zig");
};

pub const help = struct {
    pub const types = @import("./settings/help_renderer.zig").types;
    pub const ArgHelpRenderer = @import("./settings/help_renderer.zig").ArgHelpRenderer;
};

pub const test_supports = @import("./supports/test_support.zig");
pub const log_supports = @import("./supports/log_support.zig");
pub const file_supports = @import("./supports/file_support.zig");

test "All tests" {
    if (@import("test_options").run_as_workspace) {
        const mod_context_name = @import("test_options").mod_context_name;
        std.debug.print(" in `Test/{s}` ", .{mod_context_name});
    }

    std.testing.refAllDecls(@This());
    std.testing.refAllDecls(sockets);
    std.testing.refAllDecls(settings);

    const run_catch2 = @import("test_runner").run_catch2;
    try std.testing.expectEqual(0, try run_catch2(std.testing.io, std.testing.allocator, .{}));
}
