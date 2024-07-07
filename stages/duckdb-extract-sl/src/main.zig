const std = @import("std");
const core = @import("core");
const Stage = @import("./Stage.zig");
const Setting = @import("./Setting.zig");

const log = core.Logger.TraceDirect(@import("build_options").app_context);

extern fn ParseDescribeStmt() callconv(.C) void;

pub fn main() !void {
    ParseDescribeStmt();
    // var gpa = (std.heap.GeneralPurposeAllocator(.{.stack_trace_frames = 6, .thread_safe = true, .safety = true}){});
    // defer {
    //     log.debug("Leak? {}", .{gpa.deinit()});
    // }
    // const allocator = gpa.allocator();

    // var setting = Setting.loadFromArgs(allocator) catch {
    //     // try Setting.help(std.io.getStdErr().writer());
    //     std.process.exit(1);
    // };
    // defer setting.deinit();  

    // // core.Logger.filterWith(setting.log_level);

    // var stage = try Stage.init(allocator, setting);
    // defer stage.deinit();

    // try stage.run(setting);

    // log.debug("Finished", .{});
}

test "main" {
    std.testing.refAllDecls(@This());

    const run_catch2 = @import("test_runner").run_catch2;
    try std.testing.expectEqual(0, try run_catch2(std.testing.allocator));
}