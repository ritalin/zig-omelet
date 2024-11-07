const std = @import("std");

// Although this function looks imperative, note that its job is to
// declaratively construct a build graph that will be executed by an external
// runner.
pub fn build(b: *std.Build) !void {
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
    const dep_zzmq = b.dependency("zzmq", .{ .zmq_prefix = @as([]const u8, zmq_prefix) });
    const dep_clap = b.dependency("clap", .{});

    const duckdb_prefix = b.option([]const u8, "duckdb_prefix", "duckdb installed path") orelse "/usr/local/opt/duckdb";

    std.debug.print("**** runner/zmq_prefix {s}\n", .{zmq_prefix});
    std.debug.print("**** runner/duckdb_prefix {s}\n", .{duckdb_prefix});

    const dep_core = b.dependency("lib_core", .{ .zmq_prefix = zmq_prefix });

    const app_context = "runner";
    const exe_name = exe_prefix;
    
    const build_options = b.addOptions();
    build_options.addOption([]const u8, "app_context", app_context);
    build_options.addOption([]const u8, "exe_name", exe_name);
    build_options.addOption([]const u8, "exe_prefix", exe_prefix);

    app_module: {
        const exe = b.addExecutable(.{
            .name = exe_name,
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        });

        zmq_native_config: {
            exe.addLibraryPath(.{ .cwd_relative = b.pathResolve(&.{zmq_prefix, "lib"}) });
            exe.linkSystemLibrary("zmq");
            exe.linkLibC();
            break:zmq_native_config;
        }
        import_modules: {
            exe.root_module.addImport("zmq", dep_zzmq.module("zzmq"));
            exe.root_module.addImport("clap", dep_clap.module("clap"));
            exe.root_module.addImport("core", dep_core.module("core"));
            exe.root_module.addImport("known_folders", dep_core.module("core").import_table.get("known_folders").?);
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

            // This creates a build step. It will be visible in the `zig build --help` menu,
            // and can be selected like this: `zig build run`
            // This will evaluate the `run` step rather than the default, which is "install".
            const run_step = b.step("run", "Run the app");
            run_step.dependOn(&run_cmd.step);            break:app_runner;
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
        zmq_native_config: {
            exe_unit_tests.addLibraryPath(.{ .cwd_relative = b.pathResolve(&.{zmq_prefix, "lib"}) });
            exe_unit_tests.linkSystemLibrary("zmq");
            break:zmq_native_config;
        }
        import_modules: {
            exe_unit_tests.root_module.addImport("zmq", dep_zzmq.module("zzmq"));
            exe_unit_tests.root_module.addImport("clap", dep_clap.module("clap"));
            exe_unit_tests.root_module.addImport("core", dep_core.module("core"));
            exe_unit_tests.root_module.addImport("known_folders", dep_core.module("core").import_table.get("known_folders").?);
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

            test_artifact: {
                test_step.dependOn(&b.addInstallArtifact(exe_unit_tests, .{.dest_sub_path = "../test/" ++ app_context}).step);
                break:test_artifact;
            }
            break:test_runner;
        }
        break:test_module;
    }
}

// re-exports
pub const applyRunnerChannel = @import("lib_core").builder_supports.DebugEndpoint.applyRunnerChannel;
