const std = @import("std");
const run = @import("./run.zig").run;

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    try run(arena.allocator());
}

