const std = @import("std");

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
    
    const dep_known_folders = b.lazyDependency("known_folders", .{
        .target = target,
        .optimize = optimize,    

    });
    if (dep_known_folders) |dep| {
        intoWorkspaceModule(b, "known-folders", dep.module("known-folders"));
    }

    const dep_cbor = b.lazyDependency("cbor_stream", .{
        .target = target,
        .optimize = optimize,    
    });
    if (dep_cbor) |dep| {
        intoWorkspaceModule(b, "cbor", dep.module("cbor-stream"));
        intoWorkspaceModule(b, "cbor-core", dep.module("cbor-core"));
    }

    const nng_prefix =
        b.option([]const u8, "NNG_PREFIX", "NNG path prefix")
        orelse b.graph.environ_map.get("NNG_PREFIX").?
    ;
    const dep_nnng = b.lazyDependency("nnng", .{ 
        .NNG_PREFIX = nng_prefix,  
        .target = target,
        .optimize = optimize,
    });
    if (dep_nnng) |dep| {
        intoWorkspaceModule(b, "nnng", dep.module("nnng"));
        intoWorkspaceModule(b, "nng-core", dep.module("nng-core"));
    }

    const dep_efsw = b.lazyDependency("efsw", .{
        .target = target,
        .optimize = optimize,
    });
    if (dep_efsw) |dep| {
        intoWorkspaceModule(b, "efsw", dep.module("efsw"));
    }
}

fn intoWorkspaceModule(b: *std.Build, name: []const u8, mod: *std.Build.Module) void {
    b.modules.put(b.allocator, b.dupe(name), mod) catch @panic("OOM");
}