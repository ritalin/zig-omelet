const std = @import("std");
const builtin = @import("builtin");
const core = @import("core");
const Stage = @import("./Stage.zig");
const Setting = @import("./Setting.zig");

// const log = core.Logger.TraceDirect(@import("build_options").app_context);

pub const std_options: std.Options = .{
    .logFn = core.Logger.forwardIntegratedLog,
};


pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    // TODO:
    const setting: Setting = .{
        .log_level = .debug,
        .log_style = .stderr,
        .no_color = false,
        .endpoints = .{
            .req_rep = "ipc:///tmp/omelet/default/req_rep.sock",
            .pub_sub = "ipc:///tmp/omelet/default/pub_sub.sock",
            .push_pull = "ipc:///tmp/omelet/default/push_pull.sock",
        },
    };
    // var setting = Setting.loadFromArgs(allocator) catch {
    //     try Setting.help(std.io.getStdErr().writer());
    //     std.process.exit(1);
    // };
    // defer setting.deinit();

    core.Logger.filterWith(setting.log_level);

    var connection = try Stage.Connection.create(init.io, allocator, setting.endpoints);
    defer connection.deinit();
    connection.enableIntegratedLog(setting.log_style == .integrated);

    var stage = try Stage.create(allocator, &connection, &setting);
    defer stage.deinit();

    try stage.run();

    // TODO:
    // log.debug("Finished", .{});
}

test "main" {
    std.testing.refAllDecls(@This());
}
