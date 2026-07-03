const std = @import("std");
const builtin = @import("builtin");

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

        catch2_native_config: {
            mod.addCSourceFiles(.{
                .root = b.path("src/c"),
                .files = &.{
                    "catch2_session_run.cpp",
                },
                .flags = &.{
                    "-std=c++20", 
                    if ((optimize == .Debug) and (builtin.os.tag != .linux)) "-Werror" else "",
                },
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
