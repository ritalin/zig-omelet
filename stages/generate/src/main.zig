const std = @import("std");
const core = @import("core");
const Stage = @import("./Stage.zig");

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    // _ = try core.makeIpcChannelRoot();
    
    var stage = try Stage.init(arena.allocator());
    defer stage.deinit();

    try stage.run();
}

