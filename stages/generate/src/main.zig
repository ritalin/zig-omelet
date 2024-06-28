const std = @import("std");
const core = @import("core");
const Stage = @import("./Stage.zig");
const Setting = @import("./Setting.zig");

pub fn main() !void {
    var gpa = (std.heap.GeneralPurposeAllocator(.{}){});
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
}

test "main" {
    std.testing.refAllDecls(@This());
}
