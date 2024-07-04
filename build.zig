const std = @import("std");

// Although this function looks imperative, note that its job is to
// declaratively construct a build graph that will be executed by an external
// runner.
pub fn build(b: *std.Build) void {
    // Standard target options allows the person running `zig build` to choose
    // what target to build for. Here we do not override the defaults, which
    // means any target is allowed, and the default is native. Other options
    // for restricting supported target set are available.
    const target = b.standardTargetOptions(.{});

    // Standard optimization options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall. Here we do not
    // set a preferred release mode, allowing the user to decide how to optimize.
    const optimize = b.standardOptimizeOption(.{});

    const exe_prefix: []const u8 = "omret";
    const zmq_prefix: []const u8 = b.option([]const u8, "prefix", "zmq installed path") orelse "/usr/local/opt";
    const duckdb_prefix = b.option([]const u8, "duckdb_prefix", "duckdb installed path") orelse "/usr/local/opt";
    
    stage_watch_files: {
        const dep = b.dependency("stage_watch_files", .{
            .target = target,
            .optimize = optimize,
            .exe_prefix = exe_prefix,
            .zmq_prefix = zmq_prefix,
        });
        const exe_stage = dep.artifact(b.fmt("{s}-{s}", .{exe_prefix, "watch-files"}));
        b.installArtifact(exe_stage);
        break :stage_watch_files;
    }
    stage_duck_db_extract_ph: {
        const dep = b.dependency("stage_duckdb_extract_ph", .{
            .target = target,
            .optimize = optimize,
            .exe_prefix = exe_prefix,
            .zmq_prefix = zmq_prefix,
            .duckdb_prefix = duckdb_prefix,
        });
        const exe_stage = dep.artifact(b.fmt("{s}-{s}", .{exe_prefix, "duckdb-extract-ph"}));
        b.installArtifact(exe_stage);
        break :stage_duck_db_extract_ph;
    }
    const dep_ts_generate = stage: {
        const dep = b.dependency("stage_ts_generate", .{
            .target = target,
            .optimize = optimize,
            .exe_prefix = exe_prefix,
            .zmq_prefix = zmq_prefix,
        });
        const exe_stage = dep.artifact(b.fmt("{s}-{s}", .{exe_prefix, "ts-generate"}));
        b.installArtifact(exe_stage);
        break :stage dep;
    };
    const stage_runner = stage: {
        const dep = b.dependency("stage_runner", .{
            .target = target,
            .optimize = optimize,
            .exe_prefix = exe_prefix,
            .zmq_prefix = zmq_prefix,
        });
        const exe_stage = dep.artifact(b.fmt("{s}-{s}", .{exe_prefix, "runner"}));
        b.installArtifact(exe_stage);
        break :stage exe_stage;
    };

    run_cmd: {
        const cmd = b.addRunArtifact(stage_runner);
        cmd.step.dependOn(b.getInstallStep());
        if (b.args) |args| cmd.addArgs(args);
        const run_step = b.step("run", "Run the app");
        run_step.dependOn(&cmd.step);
        
        break :run_cmd;
    }
    test_fright_cmd: {
        const cmd = b.addRunArtifact(stage_runner);
        cmd.step.dependOn(b.getInstallStep());

        const test_fright_sc = command: {
            if (b.args) |args| {
                if (args.len > 0) {
                    cmd.addArgs(args[1..]);
                    break :command args[0];
                }
            }
            break :command "generate";
        };
        @import("stage_runner").applyRunnerChannel(cmd);
        cmd.addArgs(&.{
            test_fright_sc,
            "--source-dir=./_sql-examples",
            "--output-dir=./_dump/ts",
        });

        const run_step = b.step("test-run", "Run the app as test frighting");
        run_step.dependOn(&cmd.step);

        break :test_fright_cmd;
    }

    const test_all_step = b.step("test-all", "Run all unit tests");
    const s0 = b.addInstallArtifact(dep_ts_generate.artifact("test-ts-generate"), .{.dest_sub_path = "test/test-ts-generate", .dest_dir = .{.override = .prefix}});
    const s1 = b.addSystemCommand(&.{b.pathResolve(&.{b.install_prefix, "test/test-ts-generate"})});
    
    s1.step.dependOn(&s0.step);
    test_all_step.dependOn(&s1.step);
}
