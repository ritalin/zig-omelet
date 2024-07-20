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
    const zmq_prefix = b.option([]const u8, "zmq_prefix", "zmq installed path") orelse "/usr/local/opt/zmq";
    const duckdb_prefix = b.option([]const u8, "duckdb_prefix", "duckdb installed path") orelse "/usr/local/opt/duckdb";
    const catch2_prefix = b.option([]const u8, "catch2_prefix", "catch2 installed path") orelse "/usr/local/opt/catch2";

    std.debug.print("**** extract/zmq_prefix {s}\n", .{zmq_prefix});
    std.debug.print("**** extract/duckdb {s}\n", .{duckdb_prefix});
    std.debug.print("**** extract/catch2_prefix {s}\n", .{catch2_prefix});

    const dep_zzmq = b.dependency("zzmq", .{ .zmq_prefix = @as([]const u8, zmq_prefix) });
    const dep_clap = b.dependency("clap", .{});
    const dep_lib_core = b.dependency("lib_core", .{.zmq_prefix = zmq_prefix});
    const dep_lib_testing = b.dependency("lib_testing", .{.catch2_prefix = catch2_prefix});

    const app_context = "extract";
    const exe_name = b.fmt("{s}-{s}-{s}", .{exe_prefix, "duckdb", app_context}); // for displaying help

    const build_options = b.addOptions();
    build_options.addOption([]const u8, "app_context", app_context);
    build_options.addOption([]const u8, "exe_name", exe_name);

    app_module: {
        const exe = b.addExecutable(.{
            .name = exe_name,
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        });

        native_config: {
            // exe.addIncludePath(b.path("src/c/include"));
            exe.root_module.addImport("worker_runtime", mod_worker: {
                break:mod_worker createWorkerModule(b, .{
                    .name = "worker_extract",
                    .target = target,
                    .optimize = optimize,
                    .duckdb_prefix = duckdb_prefix,
                    .zmq_prefix = zmq_prefix,
                    .catch2_prefix = catch2_prefix,
                    .dep_module = dep_lib_core.module("cbor_cpp_support"),
                    .use_catch2 = false
                });
            });
            break:native_config;
        }
        import_modules: {
            exe.root_module.addImport("zmq", dep_zzmq.module("zzmq"));
            exe.root_module.addImport("clap", dep_clap.module("clap"));
            exe.root_module.addImport("core", dep_lib_core.module("core"));
            exe.root_module.addOptions("build_options", build_options);
            break:import_modules;
        }
        app_runner: {
            // This declares intent for the executable to be installed into the
            // standard location when the user invokes the "install" step (the default
            // step when running `zig build`).
            b.installArtifact(exe);

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

            // Apply zmq communication cannel
            try @import("lib_core").builder_supports.DebugEndpoint.applyStageChannel(run_cmd);

            run_cmd.addArgs(&.{
                "--log-leven=trace",
                "--schema-dir=./_schema-examples"
            });
            // This creates a build step. It will be visible in the `zig build --help` menu,
            // and can be selected like this: `zig build run`
            // This will evaluate the `run` step rather than the default, which is "install".
            const run_step = b.step("run", "Run the app");
            run_step.dependOn(&run_cmd.step);
            break:app_runner;
        }
        break:app_module;
    }

    test_module: {
        const test_prefix = "test";
        const exe_unit_tests = b.addTest(.{
            .name = b.fmt("{s}-{s}", .{test_prefix, app_context}),
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        });

        native_config: {
            // exe_unit_tests.addIncludePath(b.path("src/c/include"));
            exe_unit_tests.root_module.addImport("worker_runtime", mod_worker: {
                break:mod_worker createWorkerModule(b, .{
                    .name = "worker_extract_test",
                    .target = target,
                    .optimize = optimize,
                    .duckdb_prefix = duckdb_prefix,
                    .zmq_prefix = zmq_prefix,
                    .catch2_prefix = catch2_prefix,
                    .dep_module = dep_lib_core.module("cbor_cpp_support"),
                    .use_catch2 = true
                });
            });
            break:native_config;
        }
        import_modules: {
            exe_unit_tests.root_module.addImport("zmq", dep_zzmq.module("zzmq"));
            exe_unit_tests.root_module.addImport("clap", dep_clap.module("clap"));
            exe_unit_tests.root_module.addImport("core", dep_lib_core.module("core"));
            exe_unit_tests.root_module.addImport("test_runner", dep_lib_testing.module("runner"));
            exe_unit_tests.root_module.addOptions("build_options", build_options);
            break:import_modules;
        } 
        test_runner: {
            const run_exe_unit_tests = b.addRunArtifact(exe_unit_tests);

            // Similar to creating the run step earlier, this exposes a `test` step to
            // the `zig build --help` menu, providing a way for the user to request
            // running the unit tests.
            const test_step = b.step("test", "Run unit tests");
            test_step.dependOn(&run_exe_unit_tests.step);
            break:test_runner;
        }
        test_artifact: {
            b.getInstallStep().dependOn(&b.addInstallArtifact(exe_unit_tests, .{.dest_sub_path = "../test/" ++ app_context}).step);
            break:test_artifact;
        }
        break:test_module;
    }
}

fn createWorkerModule(
    b: *std.Build, 
    config: struct {
        name: []const u8,
        target: std.Build.ResolvedTarget,
        optimize: std.builtin.OptimizeMode,
        duckdb_prefix: []const u8,
        zmq_prefix: []const u8,
        catch2_prefix: []const u8, 
        dep_module: *std.Build.Module,
        use_catch2: bool
    }
) *std.Build.Module {
    const mod = b.addModule(config.name, .{
        .root_source_file = b.path("src/duckdb_worker.zig"),
        .target = config.target,
        .optimize = config.optimize,
        .link_libc = true,
        .link_libcpp = true,
    });

    native_config: {
        mod.addIncludePath(b.path("src/c/include"));
        mod.addCSourceFiles(.{ 
            .root = b.path("src/c"),
            .files = &.{
                "parser.cpp",
                "duckdb_database.cpp",
                "zmq_worker_support.cpp",
                "duckdb_params_collector.cpp",
                "sql_statement/select_statement.cpp",
            },
            .flags = &.{"-std=c++20", if (config.optimize == .Debug) "-Werror" else ""},
        });
        mod.addIncludePath(b.path("../../vendor/magic-enum/include"));
        @import("lib_core").builder_supports.LazyPath.mergeIncludePath(mod, config.dep_module);
        break:native_config;
    }
    catch2_config: {
        if (config.use_catch2) {
            mod.addIncludePath(.{.cwd_relative = b.pathResolve(&.{config.catch2_prefix, "include"})});
        }
        else {
            mod.addCMacro("DISABLE_CATCH2_TEST", "1");
        }
        break:catch2_config;
    }
    zmq_native_config: {
        mod.addIncludePath(.{ .cwd_relative = b.pathResolve(&.{config.zmq_prefix, "include" }) });
        mod.addLibraryPath(.{ .cwd_relative = b.pathResolve(&.{config.zmq_prefix, "lib"}) });
        mod.linkSystemLibrary("zmq", .{});
        break:zmq_native_config;
    }
    duckdb_native_config: {
        mod.addIncludePath(.{ .cwd_relative = b.pathResolve(&.{config.duckdb_prefix, "include" }) });
        mod.addLibraryPath(.{ .cwd_relative = b.pathResolve(&.{config.duckdb_prefix, "lib"}) });
        mod.linkSystemLibrary("duckdb", .{});

        mod.addIncludePath(b.path("../../vendor/duckdb/third_party/yyjson/include"));
        mod.addIncludePath(b.path("../../vendor/duckdb/third_party/fmt/include"));
        mod.addIncludePath(b.path("../../vendor/duckdb/extension/json/include"));
        break:duckdb_native_config;
    }
    import_modules: {
        mod.addImport("cbor_cpp_support", config.dep_module);
        break:import_modules;
    
    }
    
    return mod;
}
