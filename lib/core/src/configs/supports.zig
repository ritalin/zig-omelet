const std = @import("std");
const known_folders = @import("known_folders");

const core_types = @import("../types.zig");
const Symbol = core_types.Symbol;
const FilePath = core_types.FilePath;

pub const ConfigFileCandidates = std.enums.EnumFieldStruct(enum {current_dir, home_dir, executable_dir}, ?FilePath, @as(?FilePath, null));

pub fn resolveFileCandidate(allocator: std.mem.Allocator, command: Symbol, candidates: ConfigFileCandidates) !?std.fs.File {
    const file_name = try std.fmt.allocPrint(allocator, "{s}.zon", .{command});
    defer allocator.free(file_name);
    
    path: {
        if (candidates.current_dir) |dir_path| {
            const path = try std.fs.path.join(allocator, &.{dir_path, file_name});
            defer allocator.free(path);

            return std.fs.cwd().openFile(path, .{}) catch |err| switch (err) {
                error.FileNotFound => break:path,
                else => return err,
            };
        }
    }
    path: {
        if (candidates.home_dir) |dir_path| {
            var dir_ = try known_folders.open(allocator, .home, .{});
            if (dir_) |*dir| {
                defer dir.close();

                const path = try std.fs.path.join(allocator, &.{dir_path, file_name});
                defer allocator.free(path);

                return dir.openFile(path, .{}) catch |err| switch (err) {
                    error.FileNotFound => break:path,
                    else => return err,
                };
            }
        }
    }
    path: {
        if (candidates.executable_dir) |dir_path| {
            var dir_ = try known_folders.open(allocator, .executable_dir, .{});
            if (dir_) |*dir| {
                defer dir.close();

                const path = try std.fs.path.join(allocator, &.{"..", dir_path, file_name});
                defer allocator.free(path);

                return dir.openFile(path, .{}) catch |err| switch (err) {
                    error.FileNotFound => break:path,
                    else => return err,
                };
            }
        }
    }

    return null;
}

const ArrayNodes = std.enums.EnumSet(std.zig.Ast.Node.Tag).init(.{
    .struct_init_one = true,
    .struct_init_one_comma = true,
    .struct_init_dot_two = true,
    .struct_init_dot_two_comma = true,
    .struct_init_dot = true,
    .struct_init_dot_comma = true,
    .struct_init = true,
    .struct_init_comma = true,
});

pub fn isItemsEmpty(tag: std.zig.Ast.Node.Tag) bool {
    return ArrayNodes.contains(tag);
}
