const std = @import("std");
const core = @import("core");
const run = @import("./run.zig").run;

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    var channel_root = try core.makeIpcChannelRoot();
    defer channel_root.deinit();

    const app_dir_path = try std.fs.selfExeDirPathAlloc(allocator);
    var app_dir = try std.fs.openDirAbsolute(app_dir_path, .{});
    defer app_dir.close();
    std.debug.print("Runner/dir: {s}\n", .{app_dir_path});

    // launch watch-files
    var stage_extract_ph = try launchStage(arena.child_allocator, app_dir, "stage-watch-files", false); 
    // launch extrach-ph
    var stage_generate_ts = try launchStage(arena.child_allocator, app_dir, "stage-extract-ph", false);
    // launch generate-ts
    var stage_watcher = try launchStage(arena.child_allocator, app_dir, "stage-generate-ts", false);

    // const thread = try std.Thread.spawn(
    //     .{}, run, 
    //     .{allocator, .{ .watch = 1, .extract = 1, .generate = 1 }}
    // );
    // thread.join();

    // // TODO remove
    // _ = try stage_extract_ph.kill();
    // _ = try stage_generate_ts.kill();
    // _ = try stage_watcher.kill();

    try run(allocator, .{ .watch = 1, .extract = 1, .generate = 1 });

    std.debug.print("Waiting stage terminate...\n", .{});

    _ = try stage_extract_ph.wait();
    _ = try stage_generate_ts.wait();
    _ = try stage_watcher.wait();
    std.debug.print("Stage terminate done\n", .{});

    // stage_extract_ph = undefined;
    // stage_generate_ts = undefined;
    // stage_watcher = undefined;

}

fn launchStage(allocator: std.mem.Allocator, app_dir: std.fs.Dir, stage_name: []const u8, ignore_stderr: bool) !std.process.Child {
    const stage_path = try app_dir.realpathAlloc(allocator, stage_name);
    std.debug.print("Stage/path: {s}\n", .{stage_path});

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