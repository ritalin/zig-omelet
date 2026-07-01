const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const workspace = b.option(bool, "workspace", "Run test as workspace") orelse false;
    const exe_prefix = b.option([]const u8, "exe_prefix", "product name") orelse "stage";
    const nng_prefix =
        b.option([]const u8, "NNG_PREFIX", "NNG path prefix")
        orelse b.graph.environ_map.get("NNG_PREFIX").?
    ;

    const deps = b.dependency("third_parties", .{
        .NNG_PREFIX = nng_prefix,
        .target = target,
        .optimize = optimize,
    });
    const dep_lib_core = b.dependency("lib_core", .{ .NNG_PREFIX = nng_prefix, .workspace = workspace });

    const app_context = "configuration-init";
    const exe_name = b.fmt("{s}-{s}", .{exe_prefix, app_context}); // for displaying help


    const build_options = b.addOptions();
    build_options.addOption([]const u8, "app_context", exe_name);
    build_options.addOption([]const u8, "exe_name", exe_name);

    const app_module_config: std.Build.Module.CreateOptions = .{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "core", .module = dep_lib_core.module("core") },
            .{ .name = "clap", .module = deps.module("clap") },
            .{ .name = "build_options", .module = build_options.createModule()},
        },
    };

    app_runner: {
        const exe = b.addExecutable(.{
            .name = exe_name,
            .root_module = b.createModule(app_module_config),
        });
        b.installArtifact(exe);

        const run_cmd = b.addRunArtifact(exe);
        run_cmd.step.dependOn(b.getInstallStep());

        if (b.args) |args| {
            run_cmd.addArgs(args);
        }

        run_cmd.addArgs(&.{});

        const run_step = b.step("run", "Run the app");
        run_step.dependOn(&run_cmd.step);
        break:app_runner;
    }
    test_runner: {
        const mod_test_options = b.addOptions();
        mod_test_options.addOption([]const u8, "source_asset_dir", b.path("./test_assets/configs/default").getPath(b));
        mod_test_options.addOption(bool, "run_as_workspace", workspace);

        const mod_test = b.addTest(.{
            .root_module = b.createModule(app_module_config),
        });
        mod_test.root_module.addImport("test_options", mod_test_options.createModule());

        const run_exe_unit_tests = b.addRunArtifact(mod_test);
        run_exe_unit_tests.has_side_effects = true;

        const test_step = b.step("test", "Run unit tests");
        test_step.dependOn(&run_exe_unit_tests.step);

        test_artifact: {
            test_step.dependOn(&b.addInstallArtifact(mod_test, .{.dest_sub_path = "../test/" ++ app_context}).step);
            break:test_artifact;
        }        
        break:test_runner;
    }
}
