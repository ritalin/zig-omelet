const std = @import("std");
const core = @import("core");
const Stage = @import("./Stage.zig");
const Setting = @import("./Setting.zig");

const log = core.Logger.TraceDirect(@import("build_options").app_context);

pub fn main() !void {
    var gpa = (std.heap.GeneralPurposeAllocator(.{.stack_trace_frames = 6, .thread_safe = true, .safety = true}){});
    defer {
        log.debug("Leak? {}", .{gpa.deinit()});
    }
    const allocator = gpa.allocator();

    var setting = Setting.loadFromArgs(allocator) catch {
        // try Setting.help(std.io.getStdErr().writer());
        std.process.exit(1);
    };
    defer setting.deinit();  

    // core.Logger.filterWith(setting.log_level);

    var stage = try Stage.init(allocator, setting);
    defer stage.deinit();

    try stage.run(setting);

    log.debug("Finished", .{});
}

test "main" {
    std.testing.refAllDecls(@This());
}