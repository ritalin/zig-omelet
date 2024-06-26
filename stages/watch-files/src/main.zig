const std = @import("std");
const core = @import("core");
const Setting = @import("./Setting.zig");
const Stage = @import("./Stage.zig");

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

    var stage = try Stage.init(allocator, setting);
    defer stage.deinit();

    try stage.run(setting);
}

