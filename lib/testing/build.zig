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

    const catch2_prefix = b.option([]const u8, "catch2_prefix", "catch2 installed path") orelse "/usr/local/opt";

    lib_module: {
        const mod = b.addModule("runner", .{
            .root_source_file = b.path("src/root.zig"),
            .target = target,
            .optimize = optimize,
        });
        catch2_native_config: {
            mod.addCSourceFiles(.{
                .root = b.path("src/c"),
                .files = &.{
                    "catch2_session_run.cpp",
                }
            });
            mod.addLibraryPath(.{.cwd_relative = b.pathResolve(&.{catch2_prefix, "catch2/lib"})});
            mod.addIncludePath(.{.cwd_relative = b.pathResolve(&.{catch2_prefix, "catch2/include"})});
            mod.linkSystemLibrary("catch2", .{});
            break:catch2_native_config;
        }
        break:lib_module;
    }

    test_module: {
        break:test_module;
    }

// ===================

    // const exe = b.addExecutable(.{
    //     .name = "lib-main",
    //     .root_source_file = b.path("src/main.zig"),
    //     .target = target,
    //     .optimize = optimize,
    // });
    // exe.root_module.addImport("zmq", dep_zzmq.module("zzmq"));
    // exe.addLibraryPath(.{ .cwd_relative = b.pathResolve(&.{zmq_prefix, "zmq/lib"}) });
    // exe.linkSystemLibrary("zmq");
    // exe.linkLibCpp();
    // exe.linkLibC();

    // exe.addIncludePath(.{ .cwd_relative = b.pathResolve(&.{"../vendor/cbor/include"})});
    // exe.addCSourceFiles(.{
    //     .root = .{ .cwd_relative = b.pathResolve(&.{"../vendor/cbor/src/"}) },
    //     .files = &.{
    //         "encoder.c",
    //         "common.c",
    //         "decoder.c",
    //         "parser.c",
    //         "ieee754.c",
    //     }
    // });

    // b.installArtifact(exe);
    // const run_exe = b.addRunArtifact(exe);
    // run_exe.step.dependOn(b.getInstallStep());
    // const run_step = b.step("run", "Run exe");
    // run_step.dependOn(&run_exe.step);
}
