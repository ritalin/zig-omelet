const std = @import("std");
const core = @import("core");
const Runner = @import("./Runner.zig");
const Setting = @import("./settings/Setting.zig");
const Config = @import("./Config.zig");

// const traceLog = std.log;
const traceLog = core.Logger.TraceDirect(Runner.APP_CONTEXT);

const default_log_level = .debug;
const std_options = .{
    .scope_levels = &.{
        .{.scope = .default, .level = default_log_level}, 
        .{.scope = .trace, .level = default_log_level},
    },
};

pub fn main() !void {
    var gpa = (std.heap.GeneralPurposeAllocator(.{}){});
    defer {
        traceLog.debug("Leak? {}", .{gpa.deinit()});
    }
    const allocator = gpa.allocator();

    const config: Config = .{
        .stage_watch = .{
            .path = "stage-watch-files",
            .extra_args = &.{},
            .managed = true,
        },
        .stage_extract = &.{
            .{
                .path = "stage-extract-ph",
                .extra_args = &.{},
                .managed = true,
            },
        },
        .stage_generate = &.{
            .{
                .path = "stage-generate-ts",
                .extra_args = &.{},
                .managed = true,
            },
        },
    };

    var setting = switch (try Setting.loadFromArgs(allocator)) {
        .help => |setting| {
            try setting.help(std.io.getStdErr().writer());
            std.process.exit(2);
        },
        .success => |setting| setting,
    };
    defer setting.deinit();

    // const channel_root = "ipc:///tmp/duckdb-ext-ph";

    // var arena = std.heap.ArenaAllocator.init(allocator);
    // defer arena.deinit();

    // const setting_allocator = arena.allocator();
    // const setting: Setting = .{
    //     .arena = &arena,
    //     .runner_endpoints = .{
    //         .req_rep = try core.resolveIPCBindPort(setting_allocator, channel_root, core.REQ_PORT),
    //         .pub_sub = try core.resolveIPCBindPort(setting_allocator, channel_root, core.PUBSUB_PORT),
    //     },
    //     .stage_endpoints = .{
    //         .req_rep = try core.resolveIPCConnectPort(setting_allocator, channel_root, core.REQ_PORT),
    //         .pub_sub = try core.resolveIPCConnectPort(setting_allocator, channel_root, core.PUBSUB_PORT),
    //     },
    //     // .source_dir = &.{try std.fs.cwd().realpathAlloc(setting_allocator, "../_sql-examples/Foo.sql")},
    //     .source_dir = &.{try std.fs.cwd().realpathAlloc(setting_allocator, "../_sql-examples")}, 
    //     // .source_dir = &.{try std.fs.cwd().realpathAlloc(setting_allocator, "../_sql")},
    //     .watch = false,
    // };

    try core.makeIpcChannelRoot();

    var runner = try Runner.init(allocator, setting);

    const app_dir_path = try std.fs.selfExeDirPathAlloc(allocator);
    defer allocator.free(app_dir_path);
    var app_dir = try std.fs.openDirAbsolute(app_dir_path, .{});
    defer app_dir.close();
    traceLog.debug("Runner/dir: {s}", .{app_dir_path});

    // launch watch-files
    var stage_watcher: ?std.process.Child = stage: {
        if (config.stage_watch.managed) {
            break :stage try launchStage(
                allocator, 
                app_dir, "stage-watch-files",
                &.{
                    "--request-channel", setting.general.stage_endpoints.req_rep,
                    "--subscribe-channel", setting.general.stage_endpoints.pub_sub,
                    "--source-dir", setting.command.generate.source_dir_paths[0],
                    // "--watch",
                }, 
                false
            ); 
        }
        break :stage null;
    };

    // // launch extrach-ph
    var stage_extract_ph: ?std.process.Child = stage: {
        if (config.stage_extract[0].managed) {
            break :stage try launchStage(
                allocator, 
                app_dir, "stage-extract-ph", 
                &.{
                    "--request-channel", setting.general.stage_endpoints.req_rep,
                    "--subscribe-channel", setting.general.stage_endpoints.pub_sub,
                }, 
                false
            );
        }
        break :stage null;
    };
    // launch generate-ts
    var stage_generate_ts: ?std.process.Child = stage: {
        if (config.stage_generate[0].managed) {
            break :stage try launchStage(
                allocator, 
                app_dir, "stage-generate-ts",
                &.{
                    "--request-channel", setting.general.stage_endpoints.req_rep,
                    "--subscribe-channel", setting.general.stage_endpoints.pub_sub,
                    // "--output-dir", setting.command.generate.output_dir_path,
                }, 
                false
            );
        }
        break :stage @as(?std.process.Child, null);
    };

    try runner.run(config.stageCount(), setting);
    runner.deinit();

    traceLog.debug("Waiting stage terminate...", .{});
    if (stage_watcher) |*stage| _ = try stage.wait();
    if (stage_extract_ph) |*stage| _ = try stage.wait();
    if (stage_generate_ts) |*stage| _ = try stage.wait();
    traceLog.debug("Stage terminate done", .{});
}

fn launchStage(allocator: std.mem.Allocator, app_dir: std.fs.Dir, stage_name: []const u8, args: []const []const u8, ignore_stderr: bool) !std.process.Child {
    std.time.sleep(100_000);

    const stage_path = try app_dir.realpathAlloc(allocator, stage_name);
    defer allocator.free(stage_path);

    var buf = std.ArrayList([]const u8).init(allocator);
    defer buf.deinit();

    try buf.append(stage_path);
    try buf.appendSlice(args);

    var stage_process = std.process.Child.init(buf.items, allocator);

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