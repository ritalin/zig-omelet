const std = @import("std");
const core = @import("core");

const types = core.types;
const events = core.events;

const PathMatcher = @import("./PathMatcher.zig").PathMatcher(u21);
const toUnicodeString = @import("./PathMatcher.zig").toUnicodeString;

pub fn FileIterateWorker(comptime GuestStage: type) type {
    return struct {
        pub fn run(stage: *GuestStage) !void {
            const sources = stage.setting.sources;
            const filter = stage.setting.filter;
            const default_dialect = stage.setting.default_dialect;
            const cwd = std.Io.Dir.cwd();

            for (sources) |src| {
                const file_stat = try cwd.statFile(stage.io, src.dir_path, .{});
                if (file_stat.kind == .file) {
                    const file_name = try resolveSourceName("", std.fs.path.basename(src.dir_path));

                    try postFile(stage, src.category, cwd, src.dir_path, file_name.name, &filter, file_name.dialect orelse default_dialect);
                }
                else if (file_stat.kind == .directory) {
                    const base_dir = try cwd.openDir(stage.io, src.dir_path, .{});
                    try postFileOfDir(stage, src.category, base_dir, src.dir_path, base_dir, &filter, default_dialect);
                }
            }

            try stage.dispatcher.queue.post(.finish_source_path, try stage.connection.dataChannel());
        }

        fn postFileOfDir(stage: *GuestStage, category: events.TopicCategory, base_dir: std.Io.Dir, base_dir_path: types.FilePath, sub_dir: std.Io.Dir, filter: *const PathMatcher, default_dialect: types.Symbol) !void {
            var iter = try sub_dir.walk(stage.allocator);
            defer iter.deinit();

            while (try iter.next(stage.io)) |entry| {
                if (entry.kind == .file) {
                    const file_path_abs = try entry.dir.realPathFileAlloc(stage.io, entry.basename, stage.allocator);
                    defer stage.allocator.free(file_path_abs);
                    const file_name = try resolveSourceName(base_dir_path, file_path_abs);

                    try postFile(stage, category, base_dir, file_path_abs, file_name.name, filter, file_name.dialect orelse default_dialect);
                }
            }
        }

        fn postFile(stage: *GuestStage, category: events.TopicCategory, base_dir: std.Io.Dir, file_path_abs: types.FilePath, name: types.FilePath, filter: *const PathMatcher, dialect: types.Symbol) !void {
            if (!try isFileAccepted(stage.allocator, filter, file_path_abs)) {
                return;
            }

            try stage.log(.debug, "Sending source file/path: `{s}`", .{file_path_abs});

            var hasher = Hasher.init(.{});
            try makeHash(stage.io, &hasher, base_dir, file_path_abs);

            const source_path: events.Event.Payload.SourcePath = .{
                .category = category,
                .name = name,
                .path = file_path_abs,
                .dialect = dialect,
                .hash = &hasher.finalResult(),
                .item_count = 1, // TODO: needed?
            };
            try stage.dispatcher.queue.post(.{.source_path = source_path}, try stage.connection.dataChannel());
        }
    };
}

fn isFileAccepted(allocator: std.mem.Allocator, filter: *const PathMatcher, file_path: types.FilePath) !bool {
    const path_u = try toUnicodeString(allocator, file_path);
    defer allocator.free(path_u);

    if (filter.matchByExclude(path_u).exclude) {
        return false;
    }
    if (! filter.matchByInclude(path_u).include) {
        return false;
    }

    return true;
}

fn resolveSourceName(base_dir_path: types.FilePath, path: types.FilePath) !(struct { name: types.FilePath, dialect: ?types.Symbol }) {
    if (base_dir_path.len + 1 > path.len) {
        return error.FileNotFound;
    }
    if (base_dir_path.len > 0) {
        if (! std.mem.startsWith(u8, path, base_dir_path)) {
            return error.FileNotFound;
        }
    }

    const prefix_len = len: {
        const len = base_dir_path.len;
        if (std.fs.path.isSep(path[len])) break:len len + 1 else break:len len;
    };
    const file_path = path[prefix_len..];

    const ext = std.fs.path.extension(file_path);
    if (ext.len == 0) {
        return .{
            .name = file_path,
            .dialect = null,
        };
    }
    var name_len = file_path.len - ext.len;

    const dialect = std.fs.path.extension(std.fs.path.stem(file_path));
    if (dialect.len <= 1) {
        return .{
            .name = file_path[0..name_len],
            .dialect = null,
        };
    }
    name_len -= dialect.len;

    return .{
        .name = file_path[0..name_len],
        .dialect = dialect[1..],
    };
}

test "resolve file path" {
    const io = std.testing.io;
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const tmp_dir_path = try tmp_dir.dir.realPathFileAlloc(io, ".", allocator);
    defer allocator.free(tmp_dir_path);

    const dir = try tmp_dir.dir.createDirPathOpen(io, "foo/bar", .{});
    defer dir.close(io);
    const file = try dir.createFile(io, "baz.sql", .{});
    defer file.close(io);
    const path_abs = try dir.realPathFileAlloc(io, "baz.sql", allocator);
    defer allocator.free(path_abs);

    const file_name = try resolveSourceName(tmp_dir_path, path_abs);

    try std.testing.expectEqualStrings("foo/bar/baz", file_name.name);
    try std.testing.expectEqual(null, file_name.dialect);
}

test "resolve file path with dialect" {
    const io = std.testing.io;
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const tmp_dir_path = try tmp_dir.dir.realPathFileAlloc(io, ".", allocator);
    defer allocator.free(tmp_dir_path);

    const dir = try tmp_dir.dir.createDirPathOpen(io, "foo/bar", .{});
    defer dir.close(io);
    const file = try dir.createFile(io, "baz.sqlite.sql", .{});
    defer file.close(io);
    const path_abs = try dir.realPathFileAlloc(io, "baz.sqlite.sql", allocator);
    defer allocator.free(path_abs);

    const file_name = try resolveSourceName(tmp_dir_path, path_abs);

    try std.testing.expectEqualStrings("foo/bar/baz", file_name.name);
    try std.testing.expectEqualStrings("sqlite", file_name.dialect.?);
}

const Hasher = std.crypto.hash.sha2.Sha256;

fn makeHash(io: std.Io, hasher: *Hasher, base_dir: std.Io.Dir, file_path: types.FilePath) !void {
    hasher.update(file_path);

    var read_buf: [8192]u8 = undefined;
    var hash_block: [8192]u8 = undefined;

    var file = try base_dir.openFile(io, file_path, .{});
    defer file.close(io);

    var reader = file.readerStreaming(io, &read_buf);
    var hash = reader.interface.hashed(hasher, &hash_block);

    var size: usize = 0;
    while (true) {
        const len = hash.reader.discard(.unlimited) catch |err| switch (err) {
            error.EndOfStream => break,
            else => return err,
        };
        if (len == 0) break;
        size += len;
    }
}
