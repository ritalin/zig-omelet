const std = @import("std");
const core = @import("core");
const Stage = @import("./Stage.zig");
const Setting = @import("./Setting.zig");

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

    var stage = try Stage.init(arena.allocator(), setting);
    defer stage.deinit();

    // プレースホルダは、?, $<N> と $<NAME>を混在できない
    // duckdb::NotImplementedExceptionが送出される
    // const que・ry = null;
    // const que・ry = "select $name::varchar as name, xyz, 123 from Foo";
    // const que・ry = "select $name::varchar as name, xyz, 123 from read_json($path::varchar) t(id, v, v2) where v = $value::int and v2 = $value2::bigint";
    // const que・ry = "select $2 as name, xyz, 123 from Foo where v = $1";
    // const que・ry = "SELCT a, b, c";

    // duckDbParseSQL(query.ptr, query.len, null);

    try stage.run(setting);
}

test "main" {
    const run_catch2 = @import("./catch2_runner.zig").run_catch2;
    
    try std.testing.expectEqual(0, try run_catch2(std.testing.allocator));
}