const std = @import("std");
const core = @import("core");

const Stage = @import("./Stage.zig");
const Setting = @import("./Setting.zig");

pub const std_options: std.Options = .{
    .logFn = core.Logger.forwardIntegratedLog,
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    // TODO:
    // var setting = Setting.loadFromArgs(allocator) catch {
    //     try Setting.help(std.io.getStdErr().writer());
    //     std.process.exit(1);
    // };
    // defer setting.deinit();

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

    core.Logger.filterWith(setting.log_level);
    
    var connection = try Stage.Connection.create(init.io, allocator, setting.endpoints);
    defer connection.deinit();
    connection.enableIntegratedLog(setting.log_style == .integrated);

    var stage = try Stage.create(allocator, &connection, &setting);
    defer stage.deinit();
    try stage.run();
}

test "main" {
    std.testing.refAllDecls(@This());

    const run_catch2 = @import("test_runner").run_catch2;
    try std.testing.expectEqual(0, try run_catch2(std.testing.allocator));
}