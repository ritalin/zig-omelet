const std = @import("std");
const builtin = @import("builtin");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const nng_prefix =
        b.option([]const u8, "NNG_PREFIX", "NNG path prefix")
        orelse b.graph.environ_map.get("NNG_PREFIX").?
    ;
    const deps = b.dependency("third_parties", .{
        .NNG_PREFIX = nng_prefix,
        .target = target,
        .optimize = optimize,  
    });

    const mod_context = "lib_core";

    lib_module_cbor_support: {
        const mod = createCborCppModule(b, .{
            .name = "cbor-cpp",
            .target = target,
            .optimize = optimize,
            .dependencies = &.{
                deps.module("cbor-core"),
                deps.module("nng-core"),
            },
        });

        const lib = b.addLibrary(.{
            .name = "cborcpp",
            .root_module = mod,
        });
        b.installArtifact(lib);
        break:lib_module_cbor_support;
    }
    const mod_omelet_c = lib_module_omelet_c: {
        const mod_interop_c = b.addTranslateC(.{
            .target = target,
            .optimize = optimize,
            .root_source_file = b.path("include/omelet_c_types.h"),
        });

        const mod = b.addModule("omelet_c", .{
            .root_source_file = mod_interop_c.getOutput(),
            .target = target,
            .optimize = optimize,
            .link_libc = false,
        });

        native_config: {
            mod.addIncludePath(b.path("include"));
            break:native_config;
        }
        break:lib_module_omelet_c mod;
    };

    const mod_core_config: std.Build.Module.CreateOptions = .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{.name = "omelet_c", .module = mod_omelet_c},
            .{.name = "clap", .module = deps.module("clap")},
            .{.name = "cbor", .module = deps.module("cbor")},
            .{.name = "nnng", .module = deps.module("nnng")},
            .{.name = "known_folders", .module = deps.module("known-folders")},
        },
    };
    _ = b.addModule("core", mod_core_config);

    test_module: {
        const run_as_workspace = b.option(bool, "workspace", "Run test as workspace") orelse false;
        const catch2_prefix = 
            b.option([]const u8, "CATCH2_PREFIX", "catch2 installed path") orelse
            b.graph.environ_map.get("CATCH2_PREFIX").?
        ;

        const dep_testing = b.lazyDependency("lib_testing", .{
            .target = target,
            .optimize = optimize,
        });

        const mod_test_root = b.createModule(mod_core_config);
        mod_test_root.addImport("test_runner", dep_testing.?.module("runner"));
        mod_test_root.addImport("cbor_cpp", createCborCppModule(b, .{
            .name = "cbor-cpp",
            .target = target,
            .optimize = optimize,
            .prefixes = .{
                .catch2 = catch2_prefix,
            },
            .dependencies = &.{
                deps.module("cbor-core"),
                deps.module("nng-core"),
            },
            .use_catch2 = true,
        }));

        const mod_unit_tests = b.addTest(.{
            .name = "test-lib-core",
            .root_module = mod_test_root,
            .test_runner = .{ .mode = .simple, .path = b.path("../tools/zig_runner.zig")},
        });
        mod_unit_tests.use_llvm = true;

        const test_options = b.addOptions();
        test_options.addOption([]const u8, "mod_context_name", mod_context);
        test_options.addOption(bool, "run_as_workspace", run_as_workspace);
        mod_unit_tests.root_module.addImport("test_options", test_options.createModule());

        test_runner: {
            const run_mod_unit_tests = b.addRunArtifact(mod_unit_tests);
            run_mod_unit_tests.has_side_effects = true;

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

fn createCborCppModule(
    b: *std.Build,
    config: struct {
        name: []const u8,
        target: std.Build.ResolvedTarget,
        optimize: std.builtin.OptimizeMode,
        prefixes: struct {
            catch2: ?[]const u8 = null,
        } = .{},
        dependencies: []const *std.Build.Module,
        use_catch2: bool = false,
    }) *std.Build.Module 
{
    const mod = b.addModule(config.name, .{
        .target = config.target,
        .optimize = config.optimize,
        .link_libc = true,
    });

    if (builtin.os.tag == .linux) {
            mod.addIncludePath(.{.cwd_relative = "/usr/include/c++/15"});
            mod.addIncludePath(.{.cwd_relative = "/usr/include/x86_64-linux-gnu/c++/15"});
            mod.addIncludePath(.{.cwd_relative = "/usr/include"});
            mod.addIncludePath(.{.cwd_relative = "/usr/include/x86_64-linux-gnu"});
            mod.addObjectFile(.{.cwd_relative = "/usr/lib/gcc/x86_64-linux-gnu/15/libstdc++.so"});
            mod.addObjectFile(.{.cwd_relative = "/usr/lib/x86_64-linux-gnu/libgcc_s.so.1"});
    }
    else {
        mod.link_libcpp = true;
    }

    native_config: {
        mod.addIncludePath(b.path("src/c"));
        mod.addCSourceFiles(.{
            .root = b.path("src/c"),
            .files = &.{
                "cbor_encode.cpp",
            },
            .flags = &.{
                "-std=c++20", 
                if ((config.optimize == .Debug) and (builtin.os.tag != .linux)) "-Werror" else "",
            },
        });

        for (config.dependencies) |dep| {
            for (dep.include_dirs.items) |dir| {
                if (std.meta.activeTag(dir) == .path) {
                    mod.addIncludePath(dir.path);
                }
            }
        }

        break:native_config;
    }
 
    if (config.use_catch2) {
        if (config.prefixes.catch2) |prefix| {
            mod.addIncludePath(.{.cwd_relative = b.pathResolve(&.{prefix, "include"})});
        }
    }
    else {
        mod.addCMacro("DISABLE_CATCH2_TEST", "1");
    }

    return mod;
}
