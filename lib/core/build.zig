const std = @import("std");

pub const builder_supports = struct {
    pub const DebugEndpoint = @import("./src/builder_supports/DebugEndpoint.zig");
    // pub const LazyPath = @import("./src/builder_supports/LazyPath.zig");
};

// pub const settings = struct {
//     pub const types = @import("./src/settings/types.zig");
// };
pub const types = @import("./src/types.zig");

// Although this function looks imperative, note that its job is to
// declaratively construct a build graph that will be executed by an external
// runner.
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const nng_prefix =
        b.option([]const u8, "NNG_PREFIX", "NNG path prefix")
        orelse b.graph.environ_map.get("NNG_PREFIX").?
    ;

    // const dep_clap = b.dependency("clap", .{});
    const dep_known_folders = b.dependency("known_folders", .{});
    const dep_nnng = b.dependency("nnng", .{ .NNG_PREFIX = nng_prefix });
    const dep_cbor = b.dependency("cbor_stream", .{});

    const mod_context = "lib_core";

    const mod_interop_c = b.addTranslateC(.{
        .target = target,
        .optimize = optimize,
        .root_source_file = b.path("src/include/omelet_c_types.h"),
    });

    // const mod_cbor = lib_module_cbor: {
    //     const mod = b.addModule("cbor", .{
    //         .target = target,
    //         .optimize = optimize,
    //         .link_libc = true,
    //     });
    //     cbor_native_config: {
    //         mod.addCSourceFiles(.{
    //             .root = b.path("../../vendor/cbor/src/"),
    //             .files = &.{
    //                 "encoder.c",
    //                 "common.c",
    //                 "decoder.c",
    //                 "parser.c",
    //                 "ieee754.c",
    //             }
    //         });
    //         mod.addIncludePath(b.path("../../vendor/cbor/include"));
    //         break:cbor_native_config;
    //     }
    //     break:lib_module_cbor mod;
    // };
    // lib_module_cbor_support: {
    //     const mod = b.addModule("cbor_cpp_support", .{
    //         .target = target,
    //         .optimize = optimize,
    //         .link_libc = true,
    //         .link_libcpp = true,
    //     });

    //     native_config: {
    //         mod.addIncludePath(b.path("src/c"));
    //         mod.addCSourceFiles(.{
    //             .root = b.path("src/c"),
    //             .files = &.{
    //                 "cbor_encode.cpp",
    //             },
    //             .flags = &.{"-std=c++20", if (optimize == .Debug) "-Werror" else ""},
    //         });
    //         mod.addIncludePath(b.path("../../vendor/cbor/include"));
    //         break:native_config;
    //     }
    //     import_modules: {
    //         mod.addImport("cbor", mod_cbor);
    //         break:import_modules;
    //     }
    //     break:lib_module_cbor_support;
    // }
    // const mod_omelet_c = lib_module_omelet_c: {
    //     const mod = b.addModule("omelet_c_support", .{
    //         .target = target,
    //         .optimize = optimize,
    //         .link_libc = true,
    //     });
    //     mod.addCSourceFiles(.{
    //         .root = b.path("src/omelet_c/c"),
    //         .files = &.{
    //             // "dummy.c",
    //         },
    //         .flags = &.{if (optimize == .Debug) "-Werror" else ""},
    //     });

    //     native_config: {
    //         mod.addIncludePath(b.path("src/omelet_c/include"));
    //         break:native_config;
    //     }
    //     break:lib_module_omelet_c mod;
    // };

    const mod_core = lib_module: {
        const mod = b.addModule("core", .{
            .root_source_file = b.path("src/root.zig"),
            .target = target,
            .optimize = optimize,
            // .link_libc = true,
            // .link_libcpp = true,
        });

        native_config: {
        //     mod.addIncludePath(b.path("../../vendor/cbor/include"));
            mod.addImport("interop_c", mod_interop_c.createModule());
            break:native_config;
        }
        import_modules: {
            // mod.addImport("clap", dep_clap.module("clap"));
            mod.addImport("known_folders", dep_known_folders.module("known-folders"));
            mod.addImport("cbor", dep_cbor.module("cbor-stream"));
            mod.addImport("nnng", dep_nnng.module("nnng"));
            break:import_modules;
        }
        break:lib_module mod;
    };

    test_module: {
        const mod_unit_tests = b.addTest(.{
            .name = "test-lib-core",
            .root_module = mod_core,
        });
        mod_unit_tests.use_llvm = true;

        // TODO:
        // native_config: {
        //     mod_unit_tests.addIncludePath(b.path("../../vendor/cbor/include"));
        //     mod_unit_tests.addIncludePath(b.path("src/omelet_c/include"));
        //     mod_unit_tests.linkLibC();
        //     mod_unit_tests.linkLibCpp();
        //     break:native_config;
        // }
        // import_modules: {
        //     mod_unit_tests.root_module.addImport("zmq", dep_zzmq.module("zzmq"));
        //     mod_unit_tests.root_module.addImport("cbor", mod_cbor);
        //     mod_unit_tests.root_module.addImport("omelet_c", mod_omelet_c);
        //     break:import_modules;
        // }
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
}
