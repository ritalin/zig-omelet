const std = @import("std");
const core = @import("core");
const Stage = @import("./Stage.zig");
const Setting = @import("./Setting.zig");

const log = core.Logger.TraceDirect(@import("build_options").app_context);

extern fn ParseDescribeStmt() callconv(.C) void;

const c = @cImport({
    @cInclude("duckdb_worker.h");
});

pub fn main() !void {
    var gpa = (std.heap.GeneralPurposeAllocator(.{.stack_trace_frames = 6, .thread_safe = true, .safety = true}){});
    defer {
        log.debug("Leak? {}", .{gpa.deinit()});
    }
    const allocator = gpa.allocator();

    const path = try std.fs.cwd().realpathAlloc(allocator, "./_schema-examples");
    // const path = try allocator.dupe(u8, "path/to");
    defer allocator.free(path);

    var db: c.DatabaseRef = undefined;
    _ = c.initDatabase(path.ptr, path.len, &db);
    defer c.deinitDatabase(db);

    const sql = "SELECT $1 || $2::text";

    var collector: c.CollectorRef = undefined;
    _ = c.initCollector(db, "1", 1, null, &collector);
    c.executeDescribe(collector, sql.ptr, sql.len);

    // var setting = Setting.loadFromArgs(allocator) catch {
    //     // try Setting.help(std.io.getStdErr().writer());
    //     std.process.exit(1);
    // };
    // defer setting.deinit();  

    // // core.Logger.filterWith(setting.log_level);

    // var stage = try Stage.init(allocator, setting);
    // defer stage.deinit();

    // try stage.run(setting);

    // log.debug("Finished", .{});
}

test "main" {
    std.testing.refAllDecls(@This());

    const run_catch2 = @import("test_runner").run_catch2;
    try std.testing.expectEqual(0, try run_catch2(std.testing.allocator));
}