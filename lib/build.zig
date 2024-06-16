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
    const dep_zzmq = b.dependency("zzmq", .{ .prefix = @as([]const u8, zmq_prefix) });

    const mod = b.addModule("core", .{
        .root_source_file = b.path("src/root.zig"),
    });
    mod.addImport("zmq", dep_zzmq.module("zzmq"));

    mod.addIncludePath(b.path("../vendor/cbor/include"));
    mod.addCSourceFiles(.{
        .root = b.path("../vendor/cbor/src/"),
        .files = &.{
            "encoder.c",
            "common.c",
            "decoder.c",
            "parser.c",
            "ieee754.c",
        }
    });

    const mod_unit_tests = b.addTest(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    mod_unit_tests.root_module.addImport("zmq", dep_zzmq.module("zzmq"));
    mod_unit_tests.addLibraryPath(.{ .cwd_relative = b.pathResolve(&.{zmq_prefix, "zmq/lib"}) });
    mod_unit_tests.linkSystemLibrary("zmq");
    mod_unit_tests.linkLibCpp();
    mod_unit_tests.linkLibC();

    mod_unit_tests.addIncludePath(b.path("../vendor/cbor/include"));
    mod_unit_tests.addCSourceFiles(.{
        .root = b.path("../vendor/cbor/src/"),
        .files = &.{
            "encoder.c",
            "common.c",
            "decoder.c",
            "parser.c",
            "ieee754.c",
        }
    });

    const run_mod_unit_tests = b.addRunArtifact(mod_unit_tests);

    // Similar to creating the run step earlier, this exposes a `test` step to
    // the `zig build --help` menu, providing a way for the user to request
    // running the unit tests.
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_mod_unit_tests.step);

// ===================

//     const exe = b.addExecutable(.{
//         .name = "lib-main",
//         .root_source_file = b.path("src/main.zig"),
//         .target = target,
//         .optimize = optimize,
//     });
//     exe.root_module.addImport("zmq", dep_zzmq.module("zzmq"));
//     exe.addLibraryPath(.{ .cwd_relative = b.pathResolve(&.{zmq_prefix, "zmq/lib"}) });
//     exe.linkSystemLibrary("zmq");
//     exe.linkLibCpp();
//     exe.linkLibC();

//     exe.addIncludePath(.{ .cwd_relative = b.pathResolve(&.{"../vendor/cbor/include"})});
//     exe.addCSourceFiles(.{
//         .root = .{ .cwd_relative = b.pathResolve(&.{"../vendor/cbor/src/"}) },
//         .files = &.{
//             "encoder.c",
//             "common.c",
//             "decoder.c",
//             "parser.c",
//             "ieee754.c",
//         }
//     });

//     b.installArtifact(exe);
//     const run_exe = b.addRunArtifact(exe);
//     run_exe.step.dependOn(b.getInstallStep());
//     const run_step = b.step("run", "Run exe");
//     run_step.dependOn(&run_exe.step);
}
