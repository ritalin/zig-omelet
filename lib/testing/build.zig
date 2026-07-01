const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const catch2_prefix = 
        b.option([]const u8, "CATCH2_PREFIX", "catch2 installed path") 
        orelse b.graph.environ_map.get("CATCH2_PREFIX").?
    ;
    
    lib_module: {
        const mod = b.addModule("runner", .{
            .root_source_file = b.path("src/root.zig"),
            .target = target,
            .optimize = optimize,
            .link_libcpp = true,
        });
        catch2_native_config: {
            mod.addCSourceFiles(.{
                .root = b.path("src/c"),
                .files = &.{
                    "catch2_session_run.cpp",
                }
            });
            mod.addLibraryPath(.{.cwd_relative = b.pathResolve(&.{catch2_prefix, "lib"})});
            mod.addIncludePath(.{.cwd_relative = b.pathResolve(&.{catch2_prefix, "include"})});
            mod.linkSystemLibrary("Catch2", .{});
            break:catch2_native_config;
        }
        break:lib_module;
    }

    test_module: {
        break:test_module;
    }
}
