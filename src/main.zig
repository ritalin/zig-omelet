const std = @import("std");
const p = @import("./parser.zig");
pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    // プレースホルダは、?, $<N> と $<NAME>を混在できない
    // duckdb::NotImplementedExceptionが送出される
    try p.parse_sql(arena.allocator(), "select $name::varchar as name, xyz, 123 from Foo where v = $value::int");
    // try p.parse_sql(arena.allocator(), "select $name::varchar as name, xyz, 123 from read_json($path::varchar) t(id, v, v2) where v = $value::int and v2 = $value2::bigint");
    // try p.parse_sql(arena.allocator(), "select $2 as name, xyz, 123 from Foo where v = $1");
}

test "simple test" {
    std.testing.refAllDecls(@This());
}

// cli:sql| PUSH (ipc) -> PULL (ipc) |svr
// cli| SUB (ipc) <- XPUB (ipc) - XSUB (inproc) <- PUB (inproc) | svr:ph, sql
