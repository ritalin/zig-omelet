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

    const zmq_prefix = b.option([]const u8, "prefix", "zmq installed path") orelse "/usr/local/opt";
    const dep_zzmq = b.dependency("zzmq", .{ .zmq_prefix = @as([]const u8, zmq_prefix) });
    const dep_clap = b.dependency("clap", .{});

    const duckdb_prefix = b.option([]const u8, "duckdb_prefix", "duckdb installed path") orelse "/usr/local/opt";

    const dep_core = b.dependency("lib_core", .{});

    const APP_CONTEXT = "runner";
    const EXE_NAME = "omret";
    
    const build_options = b.addOptions();
    build_options.addOption([]const u8, "APP_CONTEXT", APP_CONTEXT);
    build_options.addOption([]const u8, "EXE_NAME", EXE_NAME);

    const exe = b.addExecutable(.{
        .name = EXE_NAME,
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    exe.linkSystemLibrary("zmq");
    exe.linkLibCpp();
    exe.linkLibC();
    
    exe.root_module.addImport("zmq", dep_zzmq.module("zzmq"));
    exe.root_module.addImport("clap", dep_clap.module("clap"));
    exe.root_module.addImport("core", dep_core.module("core"));
    exe.root_module.addOptions("build_options", build_options);

    b.installArtifact(exe);

    // watch-files stage
    install_stage: {
        const dep_stage = b.dependency("stage_watch_files", .{
            .target = target,
            .optimize = optimize,
            .zmq_prefix = zmq_prefix,
        });
        const exe_stage = dep_stage.artifact("stage-watch-files");
        b.installArtifact(exe_stage);
        break :install_stage;
    }
    // extract-ph stage
    install_stage: {
        const dep_stage = b.dependency("stage_extract_ph", .{
            .target = target,
            .optimize = optimize,
            .zmq_prefix = zmq_prefix,
            .duckdb_prefix = duckdb_prefix,
        });
        const exe_stage = dep_stage.artifact("stage-extract-ph");
        b.installArtifact(exe_stage);
        break :install_stage;
    }
    // generate-ts stage
    install_stage: {
        const dep_stage = b.dependency("stage_generate_ts", .{
            .target = target,
            .optimize = optimize,
            .zmq_prefix = zmq_prefix,
        });
        const exe_stage = dep_stage.artifact("stage-generate-ts");
        b.installArtifact(exe_stage);
        break :install_stage;
    }

    // This *creates* a Run step in the build graph, to be executed when another
    // step is evaluated that depends on it. The next line below will establish
    // such a dependency.
    const run_cmd = b.addRunArtifact(exe);

    // By making the run step depend on the install step, it will be run from the
    // installation directory rather than directly from within the cache directory.
    // This is not necessary, however, if the application depends on other installed
    // files, this ensures they will be present and in the expected location.
    run_cmd.step.dependOn(b.getInstallStep());

    // This allows the user to pass arguments to the application in the build
    // command itself, like this: `zig build run -- arg1 arg2 etc`
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    // This creates a build step. It will be visible in the `zig build --help` menu,
    // and can be selected like this: `zig build run`
    // This will evaluate the `run` step rather than the default, which is "install".
    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    const test_fright_cmd = b.addRunArtifact(exe);
    test_fright_cmd.step.dependOn(b.getInstallStep());

    const test_fright_sc = command: {
        if (b.args) |args| {
            if (args.len > 0) break :command args[0];
        }
        break :command "generate";
    };

    @import("lib_core").DebugEndpoint.applyRunnerChannel(test_fright_cmd);
    test_fright_cmd.addArgs(&.{
        test_fright_sc,
        "--source-dir=../_sql-examples",
        "--output-dir=../_dump/ts",
    });

    const test_fright_step = b.step("test-run", "Run the app as test frighting");
    test_fright_step.dependOn(&test_fright_cmd.step);

    const exe_unit_tests = b.addTest(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    exe_unit_tests.addIncludePath(.{ .cwd_relative = b.pathResolve(&[_][]const u8 {duckdb_prefix, "duckdb/include" }) });
    exe_unit_tests.addLibraryPath(.{ .cwd_relative = b.pathResolve(&[_][]const u8 {duckdb_prefix, "duckdb/lib"}) });
    exe_unit_tests.linkSystemLibrary("duckdb");

    exe_unit_tests.linkSystemLibrary("zmq");

    exe_unit_tests.addIncludePath(b.path("../../vendor/magic-enum/include"));
    exe_unit_tests.addIncludePath(b.path("../../vendor/json/include"));
    exe_unit_tests.linkLibCpp();
    exe_unit_tests.linkLibC();

    exe_unit_tests.root_module.addImport("zmq", dep_zzmq.module("zzmq"));
    exe_unit_tests.root_module.addImport("core", dep_core.module("core"));

    const run_exe_unit_tests = b.addRunArtifact(exe_unit_tests);

    // Similar to creating the run step earlier, this exposes a `test` step to
    // the `zig build --help` menu, providing a way for the user to request
    // running the unit tests.
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_exe_unit_tests.step);
}
