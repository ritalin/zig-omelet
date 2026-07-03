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
    default_scope: Symbol,
};

pub fn resolveFileCandidate(io: std.Io, allocator: std.mem.Allocator, env: *const std.process.Environ.Map, options: FileResolveOptions) !?std.Io.File {
    return try resolveFileCandidateInternal(io, allocator, env, options.command, options.root, options.scope, options.category) orelse {
        if (std.mem.eql(u8, options.scope, options.default_scope)) return null;
        return try resolveFileCandidateInternal(io, allocator, env, options.command, options.root, options.default_scope, options.category);
    };
}

fn resolveFileCandidateInternal(io: std.Io, allocator: std.mem.Allocator, env: *const std.process.Environ.Map, command: Symbol, candidates: ConfigFileCandidates, scope: Symbol, category: ConfigCategory) !?std.Io.File {
    const file_name = try std.fmt.allocPrint(allocator, "{s}.zon", .{command});
    defer allocator.free(file_name);

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
            var dir_ = known_folders.open(io, allocator, env, .local_configuration, .{}) catch |err| switch (err) {
                error.FileNotFound => break:path,
                else => return err,
            };
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

pub fn resolveConfigDirPath(io: std.Io, allocator: std.mem.Allocator, env: *const std.process.Environ.Map, scope: Symbol, category: ConfigCategory, candidates: ConfigFileCandidates) !?FilePath {
    path: {
        if (candidates.current_dir) |dir_path| {
            const path = try std.fs.path.join(allocator, &.{dir_path, category.destPath(), scope});
            defer allocator.free(path);

            return std.Io.Dir.cwd().realPathFileAlloc(io, path, allocator) 
            catch |err| switch (err) {
                error.FileNotFound => break:path,
                else => return err,
            };
        }
    }
    path: {
        if (candidates.home_dir) |dir_path| {
            const dir_ = try known_folders.open(io, allocator, env, .local_configuration, .{});
            if (dir_) |dir| {
                defer dir.close(io);

                const path = try std.fs.path.join(allocator, &.{dir_path, category.destPath(), scope});
                defer allocator.free(path);

                return dir.realPathFileAlloc(io, path, allocator) 
                catch |err| switch (err) {
                    error.FileNotFound => break:path,
                    else => return err,
                };
            }
        }
    }
    path: {
        if (candidates.executable_dir) |dir_path| {
            const exe_dir_path = try std.process.executableDirPathAlloc(io, allocator);
            defer allocator.free(exe_dir_path);

            const path_abs = try std.fs.path.join(allocator, &.{exe_dir_path, "..", dir_path, category.templateDir()});
            errdefer allocator.free(path_abs);

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

pub const RootPathCandidate = union(enum) {
    current_dir: FilePath, 
    home_dir: FilePath, 
    executable_dir: FilePath,
};

pub fn formatConfigRootDirPath(io: std.Io, allocator: std.mem.Allocator, env: *const std.process.Environ.Map, category: ConfigCategory, candidates: RootPathCandidate) !FilePath {
    switch (candidates) {
        .current_dir => |dir_path| {
            if (std.fs.path.isAbsolute(dir_path)) return std.fs.path.join(allocator, &.{dir_path, category.destPath()});

            const root_path = try std.Io.Dir.cwd().realPathFileAlloc(io, ".", allocator);
            defer allocator.free(root_path);
            return std.fs.path.join(allocator, &.{root_path, dir_path, category.destPath()});
        },
        .home_dir => |dir_path| {
            if (std.fs.path.isAbsolute(dir_path)) return std.fs.path.join(allocator, &.{dir_path, category.destPath()});

            const root_path = 
                try known_folders.getPath(io, allocator, env, .local_configuration)
                orelse return formatConfigRootDirPath(io, allocator, env, category, .{.current_dir = dir_path})
            ;
            defer allocator.free(root_path);
            return std.fs.path.join(allocator, &.{root_path, dir_path, category.destPath()});
        },
        .executable_dir => |dir_path| {
            const root_path = try std.process.executableDirPathAlloc(io, allocator);
            defer allocator.free(root_path);
            return std.fs.path.join(allocator, &.{root_path, "..", dir_path, category.destPath()});
        },
    }

}