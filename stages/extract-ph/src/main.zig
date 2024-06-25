const std = @import("std");
const core = @import("core");
const Stage = @import("./Stage.zig");

extern fn duckDbParseSQL(query: [*c]const u8, len: usize, socket: ?*anyopaque) callconv(.C) void;

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    var stage = try Stage.init(arena.allocator());
    defer stage.deinit();

    // _ = try core.makeIpcChannelRoot();

    // プレースホルダは、?, $<N> と $<NAME>を混在できない
    // duckdb::NotImplementedExceptionが送出される
    // const que・ry = null;
    // const que・ry = "select $name::varchar as name, xyz, 123 from Foo";
    // const que・ry = "select $name::varchar as name, xyz, 123 from Foo where v = $value::int";
    // const que・ry = "select $name::varchar as name, xyz, 123 from read_json($path::varchar) t(id, v, v2) where v = $value::int and v2 = $value2::bigint";
    // const que・ry = "select $2 as name, xyz, 123 from Foo where v = $1";
    // const que・ry = "SELCT a, b, c";

    // duckDbParseSQL(query.ptr, query.len, null);

    try stage.run();
}

test "main" {
    const run_catch2 = @import("./catch2_runner.zig").run_catch2;
    
    try std.testing.expectEqual(0, try run_catch2(std.testing.allocator));
}