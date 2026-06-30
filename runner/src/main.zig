const std = @import("std");
const core = @import("core");
const Runner = @import("./Runner.zig");
const Setting = @import("./settings/Setting.zig");
const Config = @import("./configs/Config.zig");
const GuestLaunchTask = @import("./supports/GuestLaunchTask.zig");

const exe_prefix = @import("build_options").exe_prefix;

const renderToStderr = @import("./help/rendering.zig").renderToStderr;

pub const std_options: std.Options = .{
    .log_scope_levels = &.{
        core.Logger.AppLevel,
        core.Logger.TraceLevel,
        // .{ .scope = .nnng, .level = .debug },
    },
    .logFn = core.Logger.forwardIntegratedLog,
};

pub fn main(init: std.process.Init) !void {
    var arena = std.heap.ArenaAllocator.init(init.gpa);
    defer arena.deinit();
    const allocator = arena.allocator();

    var env = try init.minimal.environ.createMap(allocator);
    defer env.deinit();

    var setting: Setting = switch (try Setting.loadFromArgs(init.io, allocator, &env, init.minimal.args)) {
        .help => |help| {
            try renderToStderr(help);
            std.process.exit(2);
        },
        .success => |setting| setting,
    };
    defer setting.deinit(init.io, allocator);

    core.Logger.filterWith(setting.base.log_level);

    var config: Config = switch (try Config.load(init.io, allocator, &env, &setting)) {
        .success => |config| config,
        .help => |help| {
            try renderToStderr(help);
            std.process.exit(2);
        },
    };
    defer config.deinit(allocator);
    
    var process_reaper = try GuestLaunchTask.launch(init.io, allocator, &config.guests, &setting);
    defer process_reaper.deinit(allocator);

    var connection = try Runner.Connection.create(init.io, allocator , config.guests.len, setting.base.endpoints);
    defer connection.deinit();
    connection.enableIntegratedLog();

    var runner = try Runner.create(init.io, allocator, &connection, &config, &setting);
    defer runner.deinit();

    try runner.transitPhase(.launching, .pending);
    try runner.run();

    try process_reaper.wait(init.io);
}

test "main" {
    // TODO:
    // std.testing.refAllDecls(@This());
    std.testing.refAllDecls(@import("./phases/boot_phase.zig"));
    std.testing.refAllDecls(@import("./phases/terminate_phase.zig"));
    std.testing.refAllDecls(@import("./configs/config_loader.zig"));
    std.testing.refAllDecls(@import("./settings/default_args.zig"));
    std.testing.refAllDecls(@import("./supports/GuestLaunchTask.zig"));
}