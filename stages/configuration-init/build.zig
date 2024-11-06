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

    const dep_zzmq = b.dependency("zzmq", .{ .zmq_prefix = @as([]const u8, zmq_prefix) });
    const dep_clap = b.dependency("clap", .{});
    const dep_lib_core = b.dependency("lib_core", .{ .zmq_prefix = zmq_prefix});

    const app_context = "configuration-init";

    const exe_name = b.fmt("{s}-{s}", .{exe_prefix, app_context}); // for displaying help


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
        zmq_native_config: {
            exe.addLibraryPath(.{ .cwd_relative = b.pathResolve(&.{zmq_prefix, "lib"}) });
            exe.linkSystemLibrary("zmq");
            break:zmq_native_config;
        }
        import_modules: {
            exe.root_module.addImport("zmq", dep_zzmq.module("zzmq"));
            exe.root_module.addImport("clap", dep_clap.module("clap"));
            exe.root_module.addImport("core", dep_lib_core.module("core"));
            exe.root_module.addOptions("build_options", build_options);
            break:import_modules;
        }
        app_runner: {
            b.installArtifact(exe);

            const run_cmd = b.addRunArtifact(exe);
            run_cmd.step.dependOn(b.getInstallStep());

            if (b.args) |args| {
                run_cmd.addArgs(args);
            }

            try @import("lib_core").builder_supports.DebugEndpoint.applyStageChannel(run_cmd);
            run_cmd.addArgs(&.{

            });

            const run_step = b.step("run", "Run the app");
            run_step.dependOn(&run_cmd.step);
            break:app_runner;
        }
        break:app_module;
    }
}
