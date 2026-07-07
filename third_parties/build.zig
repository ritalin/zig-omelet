const std = @import("std");
const builtin = @import("builtin");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const dep_clap = b.lazyDependency("clap", .{
        .target = target,
        .optimize = optimize,    
    });
    if (dep_clap) |dep| {
        intoWorkspaceModule(b, "clap", dep.module("clap"));
    }
    
    const dep_known_folders = b.dependency("known_folders", .{
        .target = target,
        .optimize = optimize,    

    });
    {
        intoWorkspaceModule(b, "known-folders", dep_known_folders.module("known-folders"));
    }

    const dep_cbor = b.dependency("cbor_stream", .{
        .target = target,
        .optimize = optimize,
    });
    {
        intoWorkspaceModule(b, "cbor", dep_cbor.module("cbor-stream"));
        intoWorkspaceModule(b, "cbor-core", dep_cbor.module("cbor-core"));
    }

    const nng_prefix =
        b.option([]const u8, "NNG_PREFIX", "NNG path prefix")
        orelse b.graph.environ_map.get("NNG_PREFIX").?
    ;
    const dep_nnng = b.dependency("nnng", .{ 
        .NNG_PREFIX = nng_prefix,  
        .target = target,
        .optimize = optimize,
    });
    {
        intoWorkspaceModule(b, "nnng", dep_nnng.module("nnng"));
        intoWorkspaceModule(b, "nng-core", dep_nnng.module("nng-core"));
    }

    const dep_efsw = b.lazyDependency("efsw", .{
        .target = target,
        .optimize = optimize,
    });
    if (dep_efsw) |dep| {
        const mod = dep.module("efsw");

        if (builtin.os.tag == .linux) {
                mod.link_libc = true;
                mod.addIncludePath(.{.cwd_relative = "/usr/include/c++/15"});
                mod.addIncludePath(.{.cwd_relative = "/usr/include/x86_64-linux-gnu/c++/15"});
                mod.addIncludePath(.{.cwd_relative = "/usr/include"});
                mod.addIncludePath(.{.cwd_relative = "/usr/include/x86_64-linux-gnu"});
                mod.addObjectFile(.{.cwd_relative = "/usr/lib/gcc/x86_64-linux-gnu/15/libstdc++.so"});
                mod.addObjectFile(.{.cwd_relative = "/usr/lib/x86_64-linux-gnu/libgcc_s.so.1"});
        }
        else {
            mod.link_libc = true;
            mod.link_libcpp = true;
        }

        intoWorkspaceModule(b, "efsw", dep.module("efsw"));
    }
}

fn intoWorkspaceModule(b: *std.Build, name: []const u8, mod: *std.Build.Module) void {
    b.modules.put(b.allocator, b.dupe(name), mod) catch @panic("OOM");
}