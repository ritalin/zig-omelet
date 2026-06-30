const std = @import("std");
const known_folders = @import("known_folders");

const root = @import("../root.zig");

const Symbol = root.types.Symbol;
const FilePath = root.types.FilePath;
const ConfigFileCandidates = root.configs.types.ConfigFileCandidates;
const ConfigCategory = root.configs.types.ConfigCategory;

pub fn resolveStageKind(name: root.types.Symbol) !root.configs.types.StageKind {
    return StageKindMap.get(name) orelse error.UnexpectGuestStage;
}

const StageKindMap: std.StaticStringMap(root.configs.types.StageKind) = .initComptime(.{
    .{"stage_watch", .watch},
    .{"stage_extract", .extract},
    .{"stage_generate", .generate},
    .{"stage_init", .init},
});

pub fn confirmToStrategy(kind: root.configs.types.StageStrategyKind, n: usize) bool {
    return switch (kind) {
        .one => n == 1,
        .many => n > 0,
        .optional => n <= 1,
    };
}

pub const FileResolveOptions = struct {
    command: Symbol, 
    scope: Symbol, 
    category: ConfigCategory,
    root: ConfigFileCandidates,
};

pub fn resolveFileCandidate(io: std.Io, allocator: std.mem.Allocator, options: FileResolveOptions) !?std.Io.File {
    return try resolveFileCandidateInternal(io, allocator, options.command, options.root, options.scope, options.category) orelse {
        return try resolveFileCandidateInternal(io, allocator, options.command, options.root, "default", options.category);
    };
}

fn resolveFileCandidateInternal(io: std.Io, allocator: std.mem.Allocator, command: Symbol, candidates: ConfigFileCandidates, scope: Symbol, category: ConfigCategory) !?std.Io.File {
    const file_name = try std.fmt.allocPrint(allocator, "{s}.zon", .{command});
    defer allocator.free(file_name);

    var env: std.process.Environ.Map = .init(allocator);
    defer env.deinit();

    path: {
        if (candidates.current_dir) |dir_path| {
            const path = try std.fs.path.join(allocator, &.{dir_path, category.destPath(), scope, file_name});
            defer allocator.free(path);

            return std.Io.Dir.cwd().openFile(io, path, .{}) catch |err| switch (err) {
                error.FileNotFound => break:path,
                else => return err,
            };
        }
    }
    path: {
        if (candidates.home_dir) |dir_path| {
            var dir_ = try known_folders.open(io, allocator, &env, .home, .{});
            if (dir_) |*dir| {
                defer dir.close(io);

                const path = try std.fs.path.join(allocator, &.{dir_path, category.destPath(), scope, file_name});
                defer allocator.free(path);

                return dir.openFile(io, path, .{}) catch |err| switch (err) {
                    error.FileNotFound => break:path,
                    else => return err,
                };
            }
        }
    }

    if (! std.mem.eql(u8, scope, "default")) return null;

    path: {
        if (candidates.executable_dir) |dir_path| {
            const exe_dir_path = try std.process.executableDirPathAlloc(io, allocator);
            defer allocator.free(exe_dir_path);

            const path_abs = try std.fs.path.join(allocator, &.{exe_dir_path, "..", dir_path, category.templateDir(), file_name});
            defer allocator.free(path_abs);

            return std.Io.Dir.openFileAbsolute(io, path_abs, .{})
            catch |err| switch (err) {
                error.FileNotFound => break:path,
                else => return err,
            };
        }
    }

    return null;
}

pub fn resolveConfigDirPath(io: std.Io, allocator: std.mem.Allocator, scope: Symbol, category: ConfigCategory, candidates: ConfigFileCandidates) !?FilePath {
    path: {
        if (candidates.current_dir) |dir_path| {
            const path = try std.fs.path.join(allocator, &.{dir_path, category.destPath(), scope});
            errdefer allocator.free(path);

            const config_dir = std.Io.Dir.cwd().openDir(io, path, .{}) 
            catch |err| switch (err) {
                error.FileNotFound => break:path,
                else => return err,
            };
            defer config_dir.close(io);
            return path;
        }
    }
    path: {
        if (candidates.home_dir) |dir_path| {
            var env: std.process.Environ.Map = .init(allocator);
            errdefer env.deinit();

            const dir_ = try known_folders.open(io, allocator, &env, .home, .{});
            if (dir_) |dir| {
                defer dir.close(io);

                const path = try std.fs.path.join(allocator, &.{dir_path, category.destPath(), scope});
                defer allocator.free(path);

                const config_dir = dir.openDir(io, path, .{}) 
                catch |err| switch (err) {
                    error.FileNotFound => break:path,
                    else => return err,
                };
                defer config_dir.close(io);
                return path;
            }
        }
    }
    path: {
        if (candidates.executable_dir) |dir_path| {
            const exe_dir_path = try std.process.executableDirPathAlloc(io, allocator);
            defer allocator.free(exe_dir_path);

            const path_abs = try std.fs.path.join(allocator, &.{exe_dir_path, "..", dir_path, category.templateDir()});
            defer allocator.free(path_abs);

            const config_dir = std.Io.Dir.openDirAbsolute(io, path_abs, .{})
            catch |err| switch (err) {
                error.FileNotFound => break:path,
                else => return err,
            };
            defer config_dir.close(io);
            return path_abs;
        }
    }

    return null;
}

pub fn formatConfigRootDirPath(allocator: std.mem.Allocator, category: ConfigCategory, root_path: FilePath) !FilePath {
    const path_alt = std.fs.path.fmtJoin(&.{root_path, category.destPath()});
    return std.fmt.allocPrint(allocator, "{f}", .{path_alt});
}