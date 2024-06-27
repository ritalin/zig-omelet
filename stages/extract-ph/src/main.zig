const std = @import("std");
const core = @import("core");
const Stage = @import("./Stage.zig");
const Setting = @import("./Setting.zig");

pub fn main() !void {
    var gpa = (std.heap.GeneralPurposeAllocator(.{.stack_trace_frames = 6, .thread_safe = true, .safety = true}){});
    defer {
        std.debug.print("Leak? {}\n", .{gpa.deinit()});
    }
    const allocator = gpa.allocator();

    var setting = Setting.loadFromArgs(allocator) catch {
        try Setting.showUsage(std.io.getStdErr().writer());
        std.process.exit(1);
    };
    defer setting.deinit();    

    var stage = try Stage.init(allocator, setting);
    defer stage.deinit();

    try stage.run(setting);

    std.debug.print("Finished\n", .{});
}

test "main" {
    const run_catch2 = @import("./catch2_runner.zig").run_catch2;
    
    try std.testing.expectEqual(0, try run_catch2(std.testing.allocator));
}