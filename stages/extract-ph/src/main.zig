const std = @import("std");
const core = @import("core");
const Stage = @import("./Stage.zig");
const Setting = @import("./Setting.zig");

pub fn main() !void {
    var gpa = (std.heap.GeneralPurposeAllocator(.{}){});
    var arena = std.heap.ArenaAllocator.init(gpa.allocator());
    defer arena.deinit();
    const allocator = arena.allocator();

    var setting = Setting.loadFromArgs(allocator) catch {
        try Setting.showUsage(std.io.getStdErr().writer());
        std.process.exit(1);
    };
    defer setting.deinit();    

    var stage = try Stage.init(arena.allocator(), setting);
    defer stage.deinit();

    try stage.run(setting);
}

test "main" {
    const run_catch2 = @import("./catch2_runner.zig").run_catch2;
    
    try std.testing.expectEqual(0, try run_catch2(std.testing.allocator));
}