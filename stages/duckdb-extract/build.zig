const std = @import("std");
const builtin = @import("builtin");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const workspace = b.option(bool, "workspace", "Run test as workspace") orelse false;
    const exe_prefix = b.option([]const u8, "exe_prefix", "product name") orelse "stage";
    const nng_prefix =
        b.option([]const u8, "NNG_PREFIX", "NNG path prefix")
        orelse b.graph.environ_map.get("NNG_PREFIX").?
    ;
    const duckdb_prefix = 
        b.option([]const u8, "DUCKDB_PREFIX", "duckdb installed path") 
        orelse b.graph.environ_map.get("DUCKDB_PREFIX").?
    ;
    const catch2_prefix = 
        b.option([]const u8, "CATCH2_PREFIX", "catch2 installed path") 
        orelse b.graph.environ_map.get("CATCH2_PREFIX").?
    ;

    const deps = b.dependency("third_parties", .{
        .NNG_PREFIX = nng_prefix,
        .target = target,
        .optimize = optimize,
    });
    const dep_lib_core = b.dependency("lib_core", .{ .NNG_PREFIX = nng_prefix, .workspace = workspace });
    const dep_lib_testing = b.dependency("lib_testing", .{.CATCH2_PREFIX = catch2_prefix});

    const app_context = "duckdb-extract";
    const exe_name = b.fmt("{s}-{s}", .{exe_prefix, app_context}); // for displaying help

    const mod_interop_c = b.addTranslateC(.{
        .root_source_file = b.path("./src/c/include/duckdb_worker.h"),
        .target = target,
        .optimize = optimize,
    });
    for (deps.module("nng-core").include_dirs.items) |dir| {
        mod_interop_c.addIncludePath(dir.path);
    }
    
    const app_root_module = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "core", .module = dep_lib_core.module("core") },
            .{ .name = "clap", .module = deps.module("clap") },
            .{ .name = "nnng", .module = deps.module("nnng") },
            .{ .name = "nng_core", .module = deps.module("nng-core") },
            .{ .name = "c", .module = mod_interop_c.createModule() },
        },
    });

    app_module: {
        const build_options = b.addOptions();
        build_options.addOption([]const u8, "app_context", exe_name);
        build_options.addOption([]const u8, "exe_name", exe_name);
        build_options.addOption([]const u8, "worker_stage", exe_name);
        build_options.addOption(bool, "forward_worker", true);

        const app_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{.name = "root_module", .module = app_root_module},
                .{.name = "build_options", .module = build_options.createModule()},
            },
        });
        
        native_config: {
            const mod_worker = worker: {
                const mod = createWorkerModule(b, .{
                    .target = target,
                    .optimize = optimize,
                    .duckdb_prefix = duckdb_prefix,
                    .nng_prefix = nng_prefix,
                    .catch2_prefix = catch2_prefix,
                    .root_source = mod_interop_c.getOutput(),
                    .dep_modules = &.{
                        .{.name = "cbor_cpp", .mod = dep_lib_core.module("cbor-cpp")},
                        .{.name = "omelet_c", .mod = dep_lib_core.module("omelet_c")},
                        .{.name = "nng_core", .mod = deps.module("nng-core") },
                    },
                    .dep_libraries = &.{
                        dep_lib_core.artifact("cborcpp"),
                    },
                    .use_catch2 = false
                });
                mod.addIncludePath(dep_lib_core.path("include"));
                break:worker mod;
            };
            app_module.addImport("worker_runtime", mod_worker);
            break:native_config;
        }
        app_runner: {
            const exe = b.addExecutable(.{
                .name = exe_name,
                .root_module = app_module,
            });
            b.installArtifact(exe);
            
            const run_cmd = b.addRunArtifact(exe);
            run_cmd.step.dependOn(b.getInstallStep());

            if (b.args) |args| {
                run_cmd.addArgs(args);
            }

            run_cmd.addArgs(&.{
                "--log-level=trace",
                "--schema-dir=./_schema-examples"
            });

            const run_step = b.step("run", "Run the app");
            run_step.dependOn(&run_cmd.step);
            break:app_runner;
        }
        break:app_module;
    }
    test_module: {
        const is_test_separated = b.option(bool, "SEP_TEST", "Separate tests for zig and C++") orelse false;

        const build_options = b.addOptions();
        build_options.addOption([]const u8, "app_context", exe_name);
        build_options.addOption([]const u8, "exe_name", exe_name);
        build_options.addOption([]const u8, "worker_stage", app_context);
        build_options.addOption(bool, "is_test_separated", is_test_separated);
        build_options.addOption(bool, "forward_worker", false);

        const test_prefix = "test";
        const test_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{.name = "test_root", .module = app_root_module},
                .{.name = "test_runner", .module = dep_lib_testing.module("runner") },
                .{.name = "build_options", .module = build_options.createModule()},
            }
        });

        const mod_worker = native_config: {
            const mod_worker = worker: {
                const mod = createWorkerModule(b, .{
                    .target = target,
                    .optimize = optimize,
                    .duckdb_prefix = duckdb_prefix,
                    .nng_prefix = nng_prefix,
                    .catch2_prefix = catch2_prefix,
                    .root_source = mod_interop_c.getOutput(),
                    .dep_modules = &.{
                        .{.name = "cbor_cpp", .mod = dep_lib_core.module("cbor-cpp")},
                        .{.name = "omelet_c", .mod = dep_lib_core.module("omelet_c")},
                        .{.name = "nng_core", .mod = deps.module("nng-core")},
                    },
                    .dep_libraries = &.{
                        dep_lib_core.artifact("cborcpp"),
                    },
                    .use_catch2 = true
                });
                break:worker mod;
            };
            break:native_config mod_worker;
        };
        test_module.addImport("c", mod_worker);

        const mod_test_options = b.addOptions();
        mod_test_options.addOption([]const u8, "source_asset_dir", b.path("./test_assets/configs/default").getPath(b));
        mod_test_options.addOption(bool, "run_as_workspace", workspace);
        test_module.addImport("test_options", mod_test_options.createModule());

        const exe_unit_tests = test_runner: {
            const exe_unit_tests = b.addTest(.{
                .name = b.fmt("{s}-{s}", .{test_prefix, app_context}),
                .root_module = test_module,
            });
            const run_exe_unit_tests = b.addRunArtifact(exe_unit_tests);
            run_exe_unit_tests.has_side_effects = true;

            const test_step = b.step("test", "Run unit tests");
            test_step.dependOn(&run_exe_unit_tests.step);

            test_artifact: {
                test_step.dependOn(&b.addInstallArtifact(exe_unit_tests, .{.dest_sub_path = "../test/" ++ app_context}).step);
                break:test_artifact;
            }
            break:test_runner exe_unit_tests;
        };
        cpp_test_runner: {
            if (!is_test_separated) break:cpp_test_runner;

            const cpp_unit_tests = b.addExecutable(.{
                .name = b.fmt("test-cpp-{s}", .{app_context}),
                .root_module = b.createModule(.{
                    .root_source_file = b.path("tools/cpp_test_bootstrap.zig"),
                    .target = target,
                    .optimize = optimize,
                    .imports = &.{
                        .{.name = "test_root", .module = app_root_module},
                        .{.name = "c", .module = mod_worker},
                        .{.name = "test_runner", .module = dep_lib_testing.module("runner") },
                        .{.name = "build_options", .module = build_options.createModule()},
                    }
                }),
            });
            cpp_unit_tests.use_llvm = true;
            const cpp_test_runner = b.addInstallArtifact(cpp_unit_tests, .{
                .dest_sub_path = b.pathJoin(&.{"../test/", cpp_unit_tests.name}),
            });
            exe_unit_tests.step.dependOn(&cpp_test_runner.step);
        }
        break:test_module;
    }
}

fn createWorkerModule(
    b: *std.Build, 
    config: struct {
        target: std.Build.ResolvedTarget,
        optimize: std.builtin.OptimizeMode,
        duckdb_prefix: []const u8,
        nng_prefix: []const u8,
        catch2_prefix: []const u8, 
        root_source: std.Build.LazyPath,
        dep_modules: []const (struct {name: []const u8, mod: *std.Build.Module}),
        dep_libraries: []const *std.Build.Step.Compile,
        use_catch2: bool
    }
) *std.Build.Module {
    const mod = b.createModule(.{
        .root_source_file = config.root_source,
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
        mod.addIncludePath(b.path("src/c/include"));
        mod.addIncludePath(.{.cwd_relative = b.pathResolve(&.{config.duckdb_prefix, "include"})});
        mod.addCSourceFiles(.{ 
            .root = b.path("src/c"),
            .files = &.{
                "parse_query.cpp",
                "parse_schema.cpp",
                "duckdb_database.cpp",
                "duckdb_params_collector.cpp",
                "statement_walker/select_statement.cpp",
                "statement_walker/delete_statement.cpp",
                "statement_walker/update_statement.cpp",
                "statement_walker/insert_statement.cpp",
                "resolver/resolve_params_type.cpp",
                "resolver/resolve_select_list.cpp",
                "resolver/resolve_column_binding.cpp",
                "resolver/resolve_select_statement_nullable.cpp",
                "resolver/resolve_user_type.cpp",
                "supports/worker_support.cpp",
                "supports/user_type_support.cpp",
                "supports/statement_walker_support.cpp",
                "supports/response_encode_support.cpp",
            },
            .flags = &.{
                "-std=c++20", 
                if ((config.optimize == .Debug) and (builtin.os.tag != .linux)) "-Werror" else "",
                "-Wno-error=invalid-constexpr", // TODO: workaround math::floor() constexpr failed for clang++
            },
        });
        mod.addIncludePath(b.path("../../vendor/magic-enum/include"));
        for (config.dep_modules) |dep_mod| {
            for (dep_mod.mod.include_dirs.items) |dir| {
                mod.addIncludePath(dir.path);
            }
            // mod.addImport(dep_mod.name, dep_mod.mod);
        }
        break:native_config;
    }
    link_module: {
        for (config.dep_libraries) |lib| {
            mod.linkLibrary(lib);
        }
        break:link_module;
    }
    catch2_config: {
        if (config.use_catch2) {
            mod.addIncludePath(.{.cwd_relative = b.pathResolve(&.{config.catch2_prefix, "include"})});
            mod.addCSourceFiles(.{
                .root = b.path("src/c"),
                .files = &.{
                    "resolver.param_type/test_select_statement.cpp",
                    "resolver.param_type/test_delete_statement.cpp",
                    "resolver.param_type/test_update_statement.cpp",
                    "resolver.param_type/test_insert_statement.cpp",
                    "resolver.select_statement_nullable/test_select_statement.cpp",
                    "resolver.select_statement_nullable/test_delete_statement.cpp",
                    "resolver.select_statement_nullable/test_update_statement.cpp",
                    "resolver.select_statement_nullable/test_insert_statement.cpp",
                    "resolver.select_list/test_select_statement.cpp",
                    "resolver.select_list/test_delete_statement.cpp",
                    "resolver.select_list/test_update_statement.cpp",
                    "resolver.select_list/test_insert_statement.cpp",
                },
                .flags = &.{
                    "-std=c++20", 
                    if ((config.optimize == .Debug) and (builtin.os.tag != .linux)) "-Werror" else "",
                    "-Wno-error=invalid-constexpr",
                },
            });
        }
        else {
            mod.addCMacro("DISABLE_CATCH2_TEST", "1");
        }
        break:catch2_config;
    }
    duckdb_native_config: {
        mod.addIncludePath(.{ .cwd_relative = b.pathResolve(&.{config.duckdb_prefix, "include" }) });
        mod.addLibraryPath(.{ .cwd_relative = b.pathResolve(&.{config.duckdb_prefix, "lib"}) });
        
        if (builtin.os.tag != .linux) {
            mod.linkSystemLibrary("duckdb_static", .{});
        }
        else {
            mod.linkSystemLibrary("duckdb", .{.preferred_link_mode = .dynamic});
        }

        if (builtin.os.tag != .linux) {
            mod.linkSystemLibrary("duckdb_generated_extension_loader", .{});
            mod.linkSystemLibrary("core_functions_extension", .{});
            mod.linkSystemLibrary("autocomplete_extension", .{});
            mod.linkSystemLibrary("icu_extension", .{});
            mod.linkSystemLibrary("parquet_extension", .{});
            mod.linkSystemLibrary("json_extension", .{});
            // mod.linkSystemLibrary("jemalloc_extension", .{});
            mod.linkSystemLibrary("duckdb_zstd", .{});
        }
        break:duckdb_native_config;
    }
    
    return mod;
}
