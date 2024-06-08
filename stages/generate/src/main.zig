const std = @import("std");
const core = @import("core");
const run = @import("./run.zig").run;

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    // _ = try core.makeIpcChannelRoot();
    
    try run(arena.allocator());
}

