const std = @import("std");
const core = @import("core");
const Stage = @import("./Stage.zig");
const Setting = @import("./Setting.zig");
const Logger = core.Logger.TraceDirect(Stage.APP_CONTEXT);

pub fn main() !void {
    var gpa = (std.heap.GeneralPurposeAllocator(.{}){});
    defer {
        Logger.debug("Leak? {}", .{gpa.deinit()});
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

    Logger.debug("Finished", .{});
}

test "main" {
    std.testing.refAllDecls(@This());
}
