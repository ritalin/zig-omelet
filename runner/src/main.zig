const std = @import("std");
const core = @import("core");
const Runner = @import("./Runner.zig");

// const traceLog = std.log;
const traceLog = core.Logger.Server.traceLog;

const default_log_level = .debug;
const std_options = .{
    .scope_levels = &.{
        .{.scope = .default, .level = default_log_level}, 
        .{.scope = .trace, .level = default_log_level},
    },
};

pub fn main() !void {
    var gpa = (std.heap.GeneralPurposeAllocator(.{}){});
    var arena = std.heap.ArenaAllocator.init(gpa.allocator());
    defer arena.deinit();
   
    // const arena = try std.heap.page_allocator.create(std.heap.ArenaAllocator);
    // defer arena.child_allocator.destroy(arena);
    // arena.* = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    // defer arena.deinit();

    const allocator = arena.allocator();

    try core.makeIpcChannelRoot();

    var runner = try Runner.init(allocator);

    const app_dir_path = try std.fs.selfExeDirPathAlloc(allocator);
    var app_dir = try std.fs.openDirAbsolute(app_dir_path, .{});
    defer app_dir.close();
    traceLog.debug("Runner/dir: {s}", .{app_dir_path});

    // launch watch-files
    // var stage_watcher = try launchStage(arena.child_allocator, app_dir, "stage-watch-files", false); 
    // launch extrach-ph
    // var stage_extract_ph = try launchStage(arena.child_allocator, app_dir, "stage-extract-ph", false);
    // launch generate-ts
    // var stage_generate_ts = try launchStage(arena.child_allocator, app_dir, "stage-generate-ts", false);

    try runner.run(.{ .watch = 1, .extract = 1, .generate = 1 });
    runner.deinit();

    traceLog.debug("Waiting stage terminate...", .{});
    // _ = try stage_watcher.wait();
    // _ = try stage_extract_ph.wait();
    // _ = try stage_generate_ts.wait();
    traceLog.debug("Stage terminate done", .{});
}

fn launchStage(allocator: std.mem.Allocator, app_dir: std.fs.Dir, stage_name: []const u8, ignore_stderr: bool) !std.process.Child {
    const stage_path = try app_dir.realpathAlloc(allocator, stage_name);
    traceLog.debug("Stage/path: {s}", .{stage_path});

    var stage_process = std.process.Child.init(
        &[_][]const u8 {
            stage_path
        }, 
        allocator
    );

    if (ignore_stderr) {
        stage_process.stderr_behavior = .Ignore;
    }
    stage_process.stdout_behavior = .Ignore;

    _ = try stage_process.spawn();

    return stage_process;
}

test "main" {
    std.testing.refAllDecls(@This());
}