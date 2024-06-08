const std = @import("std");
const core = @import("core");
const Stage = @import("./Stage.zig");

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    var stage = try Stage.init(arena.allocator());
    defer stage.deinit();

    // _ = try core.makeIpcChannelRoot();

    // プレースホルダは、?, $<N> と $<NAME>を混在できない
    // duckdb::NotImplementedExceptionが送出される
    // try p.parse_sql(arena.allocator(), null);
    // try p.parse_sql(arena.allocator(), "select $name::varchar as name, xyz, 123 from Foo");
    // try p.parse_sql(arena.allocator(), "select $name::varchar as name, xyz, 123 from Foo where v = $value::int");
    // try p.parse_sql(arena.allocator(), "select $name::varchar as name, xyz, 123 from read_json($path::varchar) t(id, v, v2) where v = $value::int and v2 = $value2::bigint");
    // try p.parse_sql(arena.allocator(), "select $2 as name, xyz, 123 from Foo where v = $1");

    try stage.run();
}

