const std = @import("std");
const builtin = @import("builtin");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const exe_prefix: []const u8 = "omelet";
    const nng_prefix =
        b.option([]const u8, "NNG_PREFIX", "NNG path prefix")
        orelse b.graph.environ_map.get("NNG_PREFIX").?
    ;
    const catch2_prefix = 
        b.option([]const u8, "CATCH2_PREFIX", "catch2 installed path") 
        orelse b.graph.environ_map.get("CATCH2_PREFIX").?
    ;
    const duckdb_prefix = 
        b.option([]const u8, "DUCKDB_PREFIX", "duckdb installed path") 
        orelse b.graph.environ_map.get("DUCKDB_PREFIX").?
    ;

    const output_dir: std.Build.InstallDir = .{
        .custom = b.pathJoin(&.{target.result.linuxTriple(b.allocator) catch @panic("OOM"), "bin"}),
    };
    const output_config_dir: std.Build.InstallDir = .{
        .custom = target.result.linuxTriple(b.allocator) catch @panic("OOM"),
    };

    stage: {
        const dep = b.dependency("stage_watch_files", .{
            .target = target,
            .optimize = optimize,
            .exe_prefix = exe_prefix,
            .NNG_PREFIX = nng_prefix,
            .workspace = true,
        });
        const exe_stage = dep.artifact(b.fmt("{s}-{s}", .{exe_prefix, "watch-files"}));
        const artifact = b.addInstallArtifact(exe_stage, .{
            .dest_dir = .{ .override = output_dir }
        });
        b.getInstallStep().dependOn(&artifact.step);
        break :stage;
    }
    stage: {
        std.log.info("> duckdb_extract", .{});
        const dep = b.dependency("stage_duckdb_extract", .{
            .target = target,
            .optimize = optimize,
            .exe_prefix = exe_prefix,
            .NNG_PREFIX = nng_prefix,
            .DUCKDB_PREFIX = duckdb_prefix,
            .CATCH2_PREFIX = catch2_prefix,
            .workspace = true,
        });
        const exe_stage = dep.artifact(b.fmt("{s}-{s}", .{exe_prefix, "duckdb-extract"}));
        const artifact = b.addInstallArtifact(exe_stage, .{
            .dest_dir = .{ .override = output_dir }
        });
        b.getInstallStep().dependOn(&artifact.step);
        break :stage;
    }
    stage: {
        const dep = b.dependency("stage_ts_generate", .{
            .target = target,
            .optimize = optimize,
            .exe_prefix = exe_prefix,
            .NNG_PREFIX = nng_prefix,
            .workspace = true,
        });
        const exe_stage = dep.artifact(b.fmt("{s}-{s}", .{exe_prefix, "ts-generate"}));
        const artifact = b.addInstallArtifact(exe_stage, .{
            .dest_dir = .{ .override = output_dir }
        });
        b.getInstallStep().dependOn(&artifact.step);
        break :stage;
    }
    stage: {
        const dep = b.dependency("stage_initialize", .{
            .target = target,
            .optimize = optimize,
            .exe_prefix = exe_prefix,
            .NNG_PREFIX = nng_prefix,
            .workspace = true,
        });
        const exe_stage = dep.artifact(b.fmt("{s}-{s}", .{exe_prefix, "configuration-init"}));
        const artifact = b.addInstallArtifact(exe_stage, .{
            .dest_dir = .{ .override = output_dir }
        });
        b.getInstallStep().dependOn(&artifact.step);
        break :stage;
    }
    const stage_runner = stage: {
        const dep = b.dependency("stage_runner", .{
            .target = target,
            .optimize = optimize,
            .exe_prefix = exe_prefix,
            .NNG_PREFIX = nng_prefix,
            .INI_SCOPE = "default",
            .workspace = true,
        });
        const exe_stage = dep.artifact(exe_prefix);
        const artifact = b.addInstallArtifact(exe_stage, .{
            .dest_dir = .{ .override = output_dir }
        });
        b.getInstallStep().dependOn(&artifact.step);
        break :stage exe_stage;
    };
    install_configs: {
        b.installDirectory(.{
            .source_dir = b.path("./runner/assets/configs"),
            .install_dir = output_config_dir,
            .install_subdir = "configs",
            .include_extensions = &.{".zon"},
        });
        break:install_configs;
    }
    install_defaults: {
        b.installDirectory(.{
            .source_dir = b.path("./runner/assets/defaults"),
            .install_dir = output_config_dir,
            .install_subdir = "defaults",
            .include_extensions = &.{".zon"},
        });
        break:install_defaults;
    }
    run_cmd: {
        const cmd = b.addRunArtifact(stage_runner);
        cmd.step.dependOn(b.getInstallStep());
        if (b.args) |args| cmd.addArgs(args);
        const run_step = b.step("run", "Run the app");
        run_step.dependOn(&cmd.step);
        
        break :run_cmd;
    }
    test_fright_cmd: {
        const run_step = b.step("test-all", "Run the app as test frighting");
        addTestAll(b, run_step);
        break :test_fright_cmd;
    }
}

fn addTestAll(b: *std.Build, parent_step: *std.Build.Step) void {
    std.log.info("Collect unit test", .{});

    var visited = std.BufSet.init(b.allocator);
    defer visited.deinit();

    const prefx = b.pathFromRoot(".");
    var deps_iter = b.graph.dependency_cache.valueIterator();

    while (deps_iter.next()) |dep| {
        var tls_iter = dep.*.builder.top_level_steps.iterator();
        while (tls_iter.next()) |entry| {
            const tls = entry.value_ptr.*;
            if (! std.mem.eql(u8, tls.step.name, "test")) continue;

            for (tls.step.dependencies.items) |dep_step| {
                if (dep_step.id != .install_artifact) continue;

                const inst: *std.Build.Step.InstallArtifact = dep_step.cast(std.Build.Step.InstallArtifact) orelse continue;

                if (inst.artifact.kind == .@"test") {
                    const pkg_prefix = inst.step.owner.pathFromRoot(".");
                    if (! std.mem.startsWith(u8, pkg_prefix, prefx)) continue;
                    if (std.mem.containsAtLeast(u8, pkg_prefix, 1, "zig-pkg")) continue;
                    if (visited.contains(pkg_prefix)) continue;

                    visited.insert(pkg_prefix) catch @panic("OOM");

                    const path = b.pathResolve(&.{"test/", inst.artifact.name});
                    std.log.info("Test found: {s} (path: {s})", .{path, pkg_prefix});
                    // install test artifact
                    const install_step = b.addInstallArtifact(
                        inst.artifact, 
                        .{
                            .dest_sub_path = path, 
                            .dest_dir = .{.override = .prefix}
                        }
                    );
                    // invoke test
                    const invoke_step = b.addSystemCommand(&.{b.pathResolve(&.{b.install_prefix, path})});
                    invoke_step.step.dependOn(&install_step.step);
                    parent_step.dependOn(&invoke_step.step);
                }
            }
        }
    }
}
