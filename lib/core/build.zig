const std = @import("std");

pub const builder_supports = struct {
    pub const DebugEndpoint = @import("./src/builder_supports/DebugEndpoint.zig");
    pub const LazyPath = @import("./src/builder_supports/LazyPath.zig");
};

pub const settings = struct {
    pub usingnamespace @import("./src/settings/types.zig");
};
pub usingnamespace @import("./src/types.zig");

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

    const zmq_prefix = b.option([]const u8, "zmq_prefix", "zmq installed path") orelse "/usr/local/opt/zmq";
    // const catch2_prefix = b.option([]const u8, "catch2_prefix", "catch2 installed path") orelse "/usr/local/opt";

    std.debug.print("**** lib_core/zmq_prefix {s}\n", .{zmq_prefix});

    const dep_zzmq = b.dependency("zzmq", .{ .zmq_prefix = @as([]const u8, zmq_prefix) });
    const dep_clap = b.dependency("clap", .{});

    const mod_context = "lib_core";

    const mod_cbor = lib_module_cbor: {
        const mod = b.addModule("cbor", .{
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        });
        cbor_native_config: {
            mod.addCSourceFiles(.{
                .root = b.path("../../vendor/cbor/src/"),
                .files = &.{
                    "encoder.c",
                    "common.c",
                    "decoder.c",
                    "parser.c",
                    "ieee754.c",
                }
            });
            mod.addIncludePath(b.path("../../vendor/cbor/include"));
            break:cbor_native_config;
        }
        break:lib_module_cbor mod;
    };
    lib_module_cbor_support: {
        const mod = b.addModule("cbor_cpp_support", .{
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .link_libcpp = true,
        });

        native_config: {
            mod.addIncludePath(b.path("src/c"));
            mod.addCSourceFiles(.{
                .root = b.path("src/c"),
                .files = &.{
                    "cbor_encode.cpp",
                },
                .flags = &.{"-std=c++20", if (optimize == .Debug) "-Werror" else ""},
            });
            mod.addIncludePath(b.path("../../vendor/cbor/include"));
            break:native_config;
        }
        import_modules: {
            mod.addImport("cbor", mod_cbor);
            break:import_modules;
        }
        break:lib_module_cbor_support;
    }

    lib_module: {
        const mod = b.addModule("core", .{
            .root_source_file = b.path("src/root.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .link_libcpp = true,
        });

        native_config: {
            mod.addIncludePath(b.path("../../vendor/cbor/include"));
            break:native_config;
        }
        import_modules: {
            mod.addImport("zmq", dep_zzmq.module("zzmq"));
            mod.addImport("clap", dep_clap.module("clap"));
            mod.addImport("cbor", mod_cbor);
            break:import_modules;
        }
        break:lib_module;
    }

    test_module: {
        const mod_unit_tests = b.addTest(.{
            .name = "test-lib-core",
            .root_source_file = b.path("src/root.zig"),
            .target = target,
            .optimize = optimize,
        });

        native_config: {
            mod_unit_tests.addIncludePath(b.path("../../vendor/cbor/include"));
            mod_unit_tests.linkLibC();
            mod_unit_tests.linkLibCpp();
            break:native_config;
        }
        zmq_native_config: {
            mod_unit_tests.addLibraryPath(.{ .cwd_relative = b.pathResolve(&.{zmq_prefix, "lib"}) });
            mod_unit_tests.linkSystemLibrary("zmq");
            break:zmq_native_config;
        }
        import_modules: {
            mod_unit_tests.root_module.addImport("zmq", dep_zzmq.module("zzmq"));
            mod_unit_tests.root_module.addImport("cbor", mod_cbor);
            break:import_modules;
        }
        test_runner: {
            const run_mod_unit_tests = b.addRunArtifact(mod_unit_tests);

            // Similar to creating the run step earlier, this exposes a `test` step to
            // the `zig build --help` menu, providing a way for the user to request
            // running the unit tests.
            const test_step = b.step("test", "Run unit tests");
            test_step.dependOn(&run_mod_unit_tests.step);

            test_artifact: {
                test_step.dependOn(&b.addInstallArtifact(mod_unit_tests, .{.dest_sub_path = "../test/" ++ mod_context}).step);
                break:test_artifact;
            }
            break:test_runner;
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
