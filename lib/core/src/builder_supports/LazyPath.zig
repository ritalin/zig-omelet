const std = @import("std");

pub fn mergeIncludePath(m: *std.Build.Module, other_module: *std.Build.Module) void {
    for (other_module.include_dirs.items) |dir| {
        switch (dir) {
            .path => |path| {
                const path_abs = path.getPath(other_module.owner);
                std.debug.print("*** include path: {s}\n", .{path_abs});
                m.addIncludePath(.{.cwd_relative = path_abs});
            },
            else => {},
        }
    }
}