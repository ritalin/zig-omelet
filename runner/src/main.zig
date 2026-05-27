const std = @import("std");
const core = @import("core");
const Runner = @import("./Runner.zig");
const Setting = @import("./settings/Setting.zig");
// const Config = @import("./configs/Config.zig");

// const log = core.Logger.TraceDirect(@import("build_options").app_context);
const exe_prefix = @import("build_options").exe_prefix;

// TODO:
// const default_log_level = .debug;
// const std_options = .{
//     .scope_levels = &.{
//         .{.scope = .default, .level = default_log_level}, 
//         .{.scope = .trace, .level = default_log_level},
//     },
// };

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    // TODO:
    // var setting = switch (try Setting.loadFromArgs(allocator)) {
    //     .help => |setting| {
    //         try setting.help(std.io.getStdErr().writer());
    //         std.process.exit(2);
    //     },
    //     .success => |setting| setting,
    // };
    // defer setting.deinit();
    const setting: Setting = .{
        .general = .{
            .stage_endpoints = .{
                .req_rep = "ipc:///tmp/omelet/default/req_rep.sock",
                .pub_sub = "ipc:///tmp/omelet/default/pub_sub.sock",
                .push_pull = "ipc:///tmp/omelet/default/push_pull.sock",
            },
        },
    };

    // TODO:
    // core.Logger.filterWith(setting.general.log_level);

    // try core.makeIpcChannelRoot(setting.general.stage_endpoints);
    // defer core.cleanupIpcChannelRoot(setting.general.stage_endpoints);

    const guest_names = &.{
        "ts-generate",
    };

    var connection = try Runner.Connection.create(init.io, allocator , guest_names.len, setting.general.stage_endpoints);
    defer connection.deinit();

    var runner = try Runner.create(&connection, guest_names, &setting);
    errdefer runner.deinit();

    // TODO:
    // var stages = switch (try Config.spawnStages(allocator, setting)) {
    //     .help => |help_setting| {
    //         try help_setting.help(std.io.getStdErr().writer());
    //         std.process.exit(2);
    //     },
    //     .success => |stages| stages,
    // };
    // defer stages.deinit();

    try runner.run();
    runner.deinit();
    
    // TODO:
    // try stages.wait();
}

test "main" {
    // TODO:
    // core.Logger.disable();
    std.testing.refAllDecls(@This());
}