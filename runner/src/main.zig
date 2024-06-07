const std = @import("std");
const run = @import("./run.zig").run;

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    const app_dir_path = try std.fs.selfExeDirPathAlloc(allocator);
    var app_dir = try std.fs.openDirAbsolute(app_dir_path, .{});
    defer app_dir.close();
    std.debug.print("Runner/dir: {s}\n", .{app_dir_path});

    // launch extrach-ph
    var stage_extract_ph = try launchStage(allocator, app_dir, "stage-extract-ph");
    // launch generate-ts
    var stage_generate_ts = try launchStage(allocator, app_dir, "stage-generate-ts");

    const thread = try std.Thread.spawn(
        .{}, run, 
        .{allocator, .{ .extract = 1, .generate = 1 }}
    );
    thread.join();

    // TODO remove
    _ = try stage_extract_ph.kill();
    _ = try stage_generate_ts.kill();


}

fn launchStage(allocator: std.mem.Allocator, app_dir: std.fs.Dir, stage_name: []const u8) !std.process.Child {
    const stage_path = try app_dir.realpathAlloc(allocator, stage_name);
    std.debug.print("Stage/path: {s}\n", .{stage_path});

    var stage_process = std.process.Child.init(
        &[_][]const u8 {
            stage_path
        }, 
        allocator
    );
    // stage_process.stderr_behavior = .Ignore;
    stage_process.stdout_behavior = .Ignore;

    _ = try stage_process.spawn();

    return stage_process;
}