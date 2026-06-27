const std = @import("std");
const builtin = @import("builtin");
const core = @import("core");
const Stage = @import("./Stage.zig");
const Setting = @import("./Setting.zig");

const renderHelp = @import("./help/rendering.zig").render;

pub const std_options: std.Options = .{
    .log_scope_levels = &.{
        core.Logger.AppLevel,
        core.Logger.TraceLevel,
    },
    .logFn = core.Logger.forwardIntegratedLog,
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var setting = switch(Setting.loadFromArgs(allocator, init.minimal.args)) {
        .help => |help| {
            try renderHelp(help);
            std.process.exit(1);
        },
        .success => |setting| setting,
    };
    defer setting.deinit(allocator);

    core.Logger.filterWith(setting.log_level);

    var connection = try Stage.Connection.create(init.io, allocator, setting.endpoints);
    defer connection.deinit();

    var stage = try Stage.create(allocator, &connection, &setting);
    defer stage.deinit();
    try stage.run();
}

test "main" {
    std.testing.refAllDecls(@This());
}
