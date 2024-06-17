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

    const duckdb_prefix = b.option([]const u8, "duckdb_prefix", "duckdb installed path") orelse "/usr/local/opt";

    const dep_core = b.dependency("lib_core", .{});

    const exe = b.addExecutable(.{
        .name = "extract-sql-placeholder",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    exe.linkSystemLibrary("zmq");
    exe.linkLibCpp();
    exe.linkLibC();
    
    exe.root_module.addImport("zmq", dep_zzmq.module("zzmq"));
    exe.root_module.addImport("core", dep_core.module("core"));

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


///////////

    const exe2 = b.addExecutable(.{
        .name = "stage-extract-sql-placeholder",
        .root_source_file = b.path("src/main2.zig"),
        .target = target,
        .optimize = optimize,
    });

    exe2.linkSystemLibrary("zmq");
    exe2.linkLibCpp();
    exe2.linkLibC();
    
    exe2.root_module.addImport("zmq", dep_zzmq.module("zzmq"));
    exe2.root_module.addImport("core", dep_core.module("core"));

    b.installArtifact(exe2);

    // This *creates* a Run step in the build graph, to be executed when another
    // step is evaluated that depends on it. The next line below will establish
    // such a dependency.
    const run_cmd2 = b.addRunArtifact(exe2);

    // By making the run step depend on the install step, it will be run from the
    // installation directory rather than directly from within the cache directory.
    // This is not necessary, however, if the application depends on other installed
    // files, this ensures they will be present and in the expected location.
    run_cmd2.step.dependOn(b.getInstallStep());

    // This allows the user to pass arguments to the application in the build
    // command itself, like this: `zig build run -- arg1 arg2 etc`
    if (b.args) |args| {
        run_cmd2.addArgs(args);
    }

    // This creates a build step. It will be visible in the `zig build --help` menu,
    // and can be selected like this: `zig build run`
    // This will evaluate the `run` step rather than the default, which is "install".
    const run_step2 = b.step("run2", "Run the app");
    run_step2.dependOn(&run_cmd2.step);

}
