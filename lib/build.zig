const std = @import("std");

pub const DebugEndpoint = @import("./src/DebugEndpoint.zig");

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

    lib_module: {
        const mod = b.addModule("core", .{
            .root_source_file = b.path("src/root.zig"),
        });

        cbor_native_config: {
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
            break:cbor_native_config;
        }
        import_modules: {
            mod.addImport("zmq", dep_zzmq.module("zzmq"));
            mod.addImport("clap", dep_clap.module("clap"));
            break:import_modules;
        }
        break:lib_module;
    }

    test_module: {
        const mod_unit_tests = b.addTest(.{
            .root_source_file = b.path("src/root.zig"),
            .target = target,
            .optimize = optimize,
        });

        native_config: {
            mod_unit_tests.linkLibC();
            mod_unit_tests.linkLibCpp();
            break:native_config;
        }
        zmq_native_config: {
            mod_unit_tests.addLibraryPath(.{ .cwd_relative = b.pathResolve(&.{zmq_prefix, "zmq/lib"}) });
            mod_unit_tests.linkSystemLibrary("zmq");
            break:zmq_native_config;
        }
        cbor_native_config: {
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
            break:cbor_native_config;
        }
        import_modules: {
            mod_unit_tests.root_module.addImport("zmq", dep_zzmq.module("zzmq"));
            break:import_modules;
        }
        test_runner: {
            const run_mod_unit_tests = b.addRunArtifact(mod_unit_tests);

            // Similar to creating the run step earlier, this exposes a `test` step to
            // the `zig build --help` menu, providing a way for the user to request
            // running the unit tests.
            const test_step = b.step("test", "Run unit tests");
            test_step.dependOn(&run_mod_unit_tests.step);
            break:test_runner;
        }
        test_artifact: {
            // b.getInstallStep().dependOn(&b.addInstallArtifact(exe_unit_tests, .{.dest_sub_path = "../test/" ++ app_context}).step);
            break:test_artifact;
        }
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
