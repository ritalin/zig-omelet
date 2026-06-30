const std = @import("std");
const core = @import("core");
const Stage = @import("./Stage.zig");
const Setting = @import("./Setting.zig");

const renderHelp = @import("./help/rendering.zig").render;

pub const std_options: std.Options = .{
    .logFn = core.Logger.forwardIntegratedLog,
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    // TODO:
    var setting = switch (Setting.loadFromArgs(allocator, init.minimal.args)) {
        .help => |help| {
            try renderHelp(help);
            std.process.exit(1);
        },
        .success => |setting| setting,
    };
    defer setting.deinit(allocator);

    // TODO:
    // const setting: Setting = .{
    //     .log_level = .debug,
    //     // .log_style = .{.integrated = .batch},
    //     .log_style = .stderr,
    //     .no_color = false,
    //     .endpoints = .{
    //         .req_rep = "ipc:///tmp/omelet/default/req_rep.sock",
    //         .pub_sub = "ipc:///tmp/omelet/default/pub_sub.sock",
    //         .push_pull = "ipc:///tmp/omelet/default/push_pull.sock",
    //     },
    //     .scope = "default",
    //     .source_dir_path = "/Users/tamurakazuhiko/work/test/ziglang/_showcase/zig-omelet/runner/.omelet/configs/default",
    //     .output_dir_path = "/Users/tamurakazuhiko/work/test/ziglang/_showcase/zig-omelet/runner/.omelet/configs",
    //     .target_scope = "dummy",
    // };

    core.Logger.filterWith(setting.log_level);
    
    var connection = try Stage.Connection.create(init.io, allocator, setting.endpoints);
    defer connection.deinit();

    var stage = try Stage.create(init.io, allocator, &connection, &setting);
    defer stage.deinit();
    try stage.run();
}

test "All test" {
    std.testing.refAllDecls(@This());
}