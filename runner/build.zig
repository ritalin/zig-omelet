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

    const exe_prefix = b.option([]const u8, "exe_prefix", "product name") orelse "stage";
    const zmq_prefix = b.option([]const u8, "zmq_prefix", "zmq installed path") orelse "/usr/local/opt";
    const dep_zzmq = b.dependency("zzmq", .{ .zmq_prefix = @as([]const u8, zmq_prefix) });
    const dep_clap = b.dependency("clap", .{});

    const duckdb_prefix = b.option([]const u8, "duckdb_prefix", "duckdb installed path") orelse "/usr/local/opt";

    const dep_core = b.dependency("lib_core", .{});

    const app_context = "runner";
    const exe_name = b.fmt("{s}-{s}", .{exe_prefix, app_context});
    
    const build_options = b.addOptions();
    build_options.addOption([]const u8, "APP_CONTEXT", app_context);
    build_options.addOption([]const u8, "exe_name", exe_name);
    build_options.addOption([]const u8, "exe_prefix", exe_prefix);

    const exe = b.addExecutable(.{
        .name = exe_name,
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
    exe_unit_tests.root_module.addImport("clap", dep_clap.module("clap"));
    exe_unit_tests.root_module.addImport("core", dep_core.module("core"));
    exe_unit_tests.root_module.addOptions("build_options", build_options);

    const run_exe_unit_tests = b.addRunArtifact(exe_unit_tests);

    // Similar to creating the run step earlier, this exposes a `test` step to
    // the `zig build --help` menu, providing a way for the user to request
    // running the unit tests.
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_exe_unit_tests.step);
}

// re-exports
pub const applyRunnerChannel = @import("lib_core").DebugEndpoint.applyRunnerChannel;