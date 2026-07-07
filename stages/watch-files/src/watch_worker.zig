const std = @import("std");
const core = @import("core");
const nnng = @import("nnng");
const efsw = @import("efsw");

const types = core.types;
const events = core.events;

const Setting = @import("./Setting.zig");
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
                    const base_dir = try cwd.openDir(stage.io, src.dir_path, .{.iterate = true});
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

            var hasher = core.file_supports.Hasher.init(.{});
            try core.file_supports.makeFileHash(stage.io, &hasher, base_dir, file_path_abs);

            const source_path: events.Event.Payload.SourcePath = .{
                .category = category,
                .name = name,
                .path = file_path_abs,
                .dialect = dialect,
                .hash = &hasher.finalResult(),
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

pub fn FileWatchWorker(comptime GuestStage: type) type {
    return struct {
        io: std.Io,
        allocator: std.mem.Allocator,
        states: []const State,
        watcher: efsw.Watcher,
        pipe: nnng.Pipe.Sync,

        const Worker = @This();

        pub fn init(io: std.Io, setting: *const Setting, pipe: nnng.Pipe.Sync) !Worker {
            const allocator = std.heap.c_allocator;
            const states = try allocator.alloc(State, setting.sources.len);

            for (states, 0..) |*state, i| {
                state.* = .{
                    .io = io,
                    .dir = &setting.sources[i],
                    .filter = &setting.filter,
                    .default_dialect = setting.default_dialect,
                    .pipe = pipe,                    
                };
            }     

            return .{
                .io = io,
                .allocator = allocator,
                .states = states,
                .watcher = try createWatcher(allocator, states),
                .pipe = pipe,
            };
        }

        pub fn deinit(self: *Worker) void {
            self.allocator.free(self.states);
        }

        pub fn run(self: Worker) void {
            var worker = self;
            defer worker.deinit();

            worker.watcher.start();

            const err_msg = efsw.Watcher.LastError.get();
            if (err_msg.len > 0) {
                sendFatalError(worker.allocator, err_msg, worker.pipe) catch {};
                return;
            }

            var barrier: std.Io.Event = .unset;
            const timeout: std.Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(100), .clock = .awake } };

            while (true) {
                barrier.waitTimeout(worker.io, timeout) 
                catch |err| switch (err) {
                    error.Timeout => continue,
                    error.Canceled => return,
                };
            }
        }

        fn createWatcher(allocator: std.mem.Allocator, states: []State) !efsw.Watcher {
            var watcher = try efsw.Watcher.init(allocator, false);

            for (0..states.len) |i| {
                const dir_path = states[i].dir.dir_path;
                if (watcher.isWatching(dir_path)) continue;

                _ = try watcher.addWatch(
                    dir_path,
                    .{
                        .on_add = State.notifyChanged,
                        .on_modified = State.notifyChanged,
                        .on_renamed = State.notifyRenbame,
                        .on_error = State.notifyWatchError,
                        .mac_modified_exclude_filter = .{.finder_info = true, .inode = true},
                        .recursive = true,
                        .user_data = &states[i],
                    }
                );
            }

            return watcher;
        }

        fn sendFatalError(allocator: std.mem.Allocator, err: types.Symbol, pipe: nnng.Pipe.Sync) !void {
            var channel = try core.sockets.SendChannel.init(allocator, pipe.item.id, GuestStage.stage_name, pipe.item.sender());
            defer channel.deinit();

            const log: events.Event.Payload.Log = .{
                .level = .err,
                .content = err,
            };

            try channel.encode(.{.report_fatal = log});
            try channel.submit(.{});
        }

        fn sendSourcePath(state: *const State, allocator: std.mem.Allocator, dir_path: types.FilePath, sub_path: types.FilePath) !void {
            const path_abs = try std.fs.path.join(allocator, &.{dir_path, sub_path});
            defer allocator.free(path_abs);

            if (! try isFileAccepted(allocator, state.filter, path_abs)) return;

            var channel = try core.sockets.SendChannel.init(allocator, state.pipe.item.id, GuestStage.stage_name, state.pipe.item.sender());
            defer channel.deinit();

            const info = try resolveSourceName(dir_path, path_abs);

            var hasher = core.file_supports.Hasher.init(.{});
            try core.file_supports.makeFileHash(state.io, &hasher, std.Io.Dir.cwd(), path_abs);

            const source: events.Event.Payload.SourcePath = .{
                .category = state.dir.category,
                .name = info.name,
                .dialect = info.dialect orelse "duckdb",
                .path = path_abs,
                .hash = &hasher.finalResult(),
            };

            try channel.encode(.{.source_path = source});
            try channel.submit(.{});
        }

        fn sendWatchLog(state: *const State, allocator: std.mem.Allocator, err: types.Symbol, action: efsw.Watcher.Action) !void {
            var channel = try core.sockets.SendChannel.init(allocator, state.pipe.item.id, GuestStage.stage_name, state.pipe.item.sender());
            defer channel.deinit();

            const content = try std.fmt.allocPrint(allocator, "{s}/action: {s}, category: {s}, dir: {s}", .{err,@tagName(action), @tagName(state.dir.category), state.dir.dir_path});
            defer allocator.free(content);

            const log: events.Event.Payload.Log = .{
                .level = .err,
                .content = content,
            };

            try channel.encode(.{.log = log});
            try channel.submit(.{});
        }

        pub const State = struct {
            io: std.Io,
            dir: *const Setting.SourceDir,
            filter: *const PathMatcher,
            default_dialect: types.Symbol,
            pipe: nnng.Pipe.Sync,

            fn notifyChanged(watcher: *efsw.Watcher, id: efsw.Watcher.WatchId, dir_path: []const u8, sub_path: []const u8, user_data: ?*anyopaque) !void {
                _ = watcher;
                _ = id;

                if (user_data == null) return;

                const state: *State = @ptrCast(@alignCast(user_data.?));

                try sendSourcePath(state, std.heap.c_allocator, dir_path, sub_path);
            }

            fn notifyRenbame(watcher: *efsw.Watcher, id: efsw.Watcher.WatchId, dir_path: []const u8, new_name: []const u8, old_name: []const u8, user_data: ?*anyopaque) !void {
                _ = watcher;
                _ = id;
                _ = old_name;

                if (user_data == null) return;
                const state: *State = @ptrCast(@alignCast(user_data.?));

                try sendSourcePath(state, std.heap.c_allocator, dir_path, new_name);
            }

            fn notifyWatchError(watcher: *efsw.Watcher, id: efsw.Watcher.WatchId, action: efsw.Watcher.Action, _: anyerror, user_data: ?*anyopaque) !void {
                _ = watcher;
                _ = id;

                if (user_data == null) return;
                const state: *State = @ptrCast(@alignCast(user_data.?));

                const allocator = std.heap.c_allocator;
                const err = efsw.Watcher.LastError.get();

                try sendWatchLog(state, allocator, err, action);
            }

        };
    };
}