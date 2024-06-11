const std = @import("std");
const core = @import("core");
const Stage = @import("./Stage.zig");

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    // _ = try core.makeIpcChannelRoot();
    

    const cmd_args = try std.process.argsAlloc(arena.allocator());
    const stand_alone =(cmd_args.len > 1) and std.mem.eql(u8, cmd_args[1], "standalone");

    var stage = try Stage.init(arena.allocator(), .{.stand_alone = stand_alone});
    defer stage.deinit();

    std.time.sleep(100_000);
    try stage.run();
    std.time.sleep(100_000);
}

