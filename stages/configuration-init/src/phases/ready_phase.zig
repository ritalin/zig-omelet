const std = @import("std");
const core = @import("core");

const types = core.types;

const ReceiveEntry = core.sockets.ReceiveEntry;
const EventDispatcher = core.sockets.EventDispatcher;
const StructView = core.events.StructView;
const Event = core.events.Event;

pub fn NewConfigurationState(comptime GuestStage: type) type {
    return struct {
        const Self = @This();

        pub const create: Self = .{};
        pub fn deinit(_: *Self) void {}

        pub fn handle(self: *const Self, stage: *GuestStage, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
            _ = self;

            switch (entry.event) {
                .probe => |phase| {
                    if ((phase == .ready) and (std.meta.eql(stage.dispatcher.phase, .{.kind = .ready, .agreement = .pending}))) {
                        var channel = try stage.connection.requestChannel();
                        try channel.submit(stage.connection.context.io, .ready, .{});
                        return;
                    }
                },
                .ready_source_path => {
                    validate: {
                        const out_root_dir = try std.Io.Dir.openDirAbsolute(stage.io, stage.setting.output_dir_path, .{});
                        defer out_root_dir.close(stage.io);

                        const dir = out_root_dir.openDir(stage.io, stage.setting.scope, .{})
                            catch |err| switch (err) {
                                error.FileNotFound => break:validate,
                                else => return err,
                            };
                        defer dir.close(stage.io);

                        // Already generated
                        const res: Event.Payload.GenerateResponse = .{
                            .desc = .{
                                .category = .source,
                                .name = stage.setting.scope,
                                .dialect = "",
                                .offset = 0,
                            },
                            .status = .noop,
                            .message = "Already generated",
                        };
                        try stage.dispatcher.queue.post(.{.finish_generate = res}, try stage.connection.dataChannel());
                        return;
                    }

                    var hasher = core.file_supports.Hasher.init(.{});
                    try core.file_supports.makeDirHash(stage.io, &hasher, stage.setting.source_dir_path);

                    const source: Event.Payload.SourcePath = .{
                        .category = .source,
                        .name = stage.setting.scope,
                        .path = stage.setting.source_dir_path,
                        .dialect = "",
                        .hash = &hasher.finalResult(),
                    };

                    try stage.dispatcher.queue.post(.{.source_path = source}, try stage.connection.dataChannel());
                    return;
                },
                .source_path => |payload| {
                    const res: Event.Payload.GenerateResponse = generate: {
                        if (handleGenerate(stage.io, stage.allocator, payload.path, stage.setting.output_dir_path, stage.setting.target_scope)) |_| {
                            break:generate .{
                                .desc = .{
                                    .category = payload.category,
                                    .name = stage.setting.scope,
                                    .dialect = payload.dialect,
                                    .offset = 0,
                                },
                                .status = .new_file,
                                .message = "New scope generated",
                            };
                        }
                        else |_| {
                            break:generate .{
                                .desc = .{
                                    .category = payload.category,
                                    .name = stage.setting.scope,
                                    .dialect = payload.dialect,
                                    .offset = 0,
                                },
                                .status = .generate_failed,
                                .message = "Generate scope failed",
                            };
                        }
                    };
                    try stage.dispatcher.queue.post(.{.finish_generate = res}, try stage.connection.dataChannel());
                    return;
                },
                else => {}
            }
            try stage.defaultHandler(entry, dirty);
        }
    };
}

fn handleGenerate(io: std.Io, allocator: std.mem.Allocator, source_path: core.types.FilePath, out_root_path: core.types.FilePath, target_scope: types.Symbol) !void {
    const source_base_dir = try std.Io.Dir.openDirAbsolute(io, source_path, .{});
    defer source_base_dir.close(io);

    const out_root_dir = try std.Io.Dir.openDirAbsolute(io, out_root_path, .{});
    defer out_root_dir.close(io);
    const out_base_dir = try out_root_dir.createDirPathOpen(io, target_scope, .{});
    defer out_base_dir.close(io);


    var visited = std.BufSet.init(allocator);
    defer visited.deinit();

    try visited.insert(source_path);

    handleGenerateInternal(io, allocator, source_base_dir, out_base_dir, &visited) catch |err| {
        // rollback
        try out_root_dir.deleteTree(io, target_scope);
        return err;
    };
}

fn handleGenerateInternal(io: std.Io, allocator: std.mem.Allocator, source_base_dir: std.Io.Dir, out_base_dir: std.Io.Dir, visited: *std.BufSet) !void {
    var walker = try source_base_dir.walk(allocator);
    defer walker.deinit();

    while (try walker.next(io)) |e| {
        switch (e.kind) {
            .directory => {
                if (visited.contains(e.path)) return error.VisitedDir;
                try visited.insert(e.path);
            },
            .file => {
                if (std.fs.path.dirname(e.path)) |dir_path| {
                    const out_dir = try out_base_dir.createDirPathOpen(io, dir_path, .{});
                    defer out_dir.close(io);
                    try source_base_dir.copyFile(e.path, out_dir, std.fs.path.basename(e.path), io, .{});
                }
                else {
                    try source_base_dir.copyFile(e.path, out_base_dir, std.fs.path.basename(e.path), io, .{});
                }
            },
            .sym_link => {
                var buffer: [std.posix.PATH_MAX]u8 = undefined;
                const len = try e.dir.readLink(io, e.path, &buffer);
                const resolve_path = buffer[0..len];
                
                if (e.dir.openDir(io, resolve_path, .{})) |src_dir| {
                    defer src_dir.close(io);

                    const out_dir = try out_base_dir.createDirPathOpen(io, e.path, .{});
                    defer out_dir.close(io);

                    const next_src_path = try src_dir.realPathFileAlloc(io, ".", allocator);
                    defer allocator.free(next_src_path);

                    if (visited.contains(next_src_path)) return error.VisitedDir;
                    try visited.insert(next_src_path);

                    try handleGenerateInternal(io, allocator, src_dir, out_dir, visited);
                }
                else |err| switch (err) {
                    error.NotDir => {
                        if (std.fs.path.dirname(e.path)) |dir_path| {
                            const out_dir = try out_base_dir.createDirPathOpen(io, dir_path, .{});
                            defer out_dir.close(io);
                            try source_base_dir.copyFile(e.path, out_dir, std.fs.path.basename(e.path), io, .{});
                        }
                        else {
                            try source_base_dir.copyFile(e.path, out_base_dir, std.fs.path.basename(e.path), io, .{});
                        }
                    },
                    else => return err,
                }
            },
            else => {
                return error.UnsupportedStorateType;
            },
        }
    }
}

test "configuration-init test" {
    std.testing.refAllDecls(@This());
}

pub const tests = struct {
    const source_asset_dir_path = @import("test_optons").source_asset_dir;

    fn collectPathSetInternal(io: std.Io, allocator: std.mem.Allocator, dir: std.Io.Dir, sub_path: []const u8, visited: *std.BufSet, path_set: *std.BufSet) !void {
        const sub_path_abs = try dir.realPathFileAlloc(io, sub_path, allocator);
        defer allocator.free(sub_path_abs);
        
        if (visited.contains(sub_path_abs)) return;
        try visited.insert(sub_path_abs);

        var walker = try dir.walk(allocator);
        defer walker.deinit();

        while (try walker.next(io)) |e| {
            try path_set.insert(e.path);

            switch (e.kind) {
                .sym_link => {
                    var resolve_path: [std.posix.PATH_MAX]u8 = undefined;
                    const len = try e.dir.readLink(io, e.basename, &resolve_path);

                    if (e.dir.openDir(io, resolve_path[0..len], .{})) |next_dir| {
                        defer next_dir.close(io);

                        const next_sub_path = try std.fmt.allocPrint(allocator, "{f}", .{std.fs.path.fmtJoin(&.{resolve_path[0..len], e.path})});
                        defer allocator.free(next_sub_path);
                        try collectPathSetInternal(io, allocator, next_dir, next_sub_path, visited, path_set);
                    }
                    else |err| switch (err) {
                        error.NotDir => {},
                        else => return err,
                    }
                },
                else => {},
            }
        }
    }

    fn collectPathSet(io: std.Io, allocator: std.mem.Allocator, root_path: []const u8) !std.BufSet {
        var path_set = std.BufSet.init(allocator);

        var dir = std.Io.Dir.openDirAbsolute(io, root_path, .{}) catch |err| switch (err) {
            error.FileNotFound => return path_set,
            else => return err,
        };
        defer dir.close(io);

        var visited = std.BufSet.init(allocator);
        defer visited.deinit();

        try collectPathSetInternal(io, allocator, dir, ".", &visited, &path_set);

        return path_set;
    }

    fn expectRealFile(io: std.Io, allocator: std.mem.Allocator, root_path: []const u8) !void {
        var dir = try std.Io.Dir.openDirAbsolute(io, root_path, .{});
        defer dir.close(io);

        var walker = try dir.walk(allocator);
        defer walker.deinit();

        while (try walker.next(io)) |e| {
            try std.testing.expectEqual(true, e.kind != .sym_link);
        }
    }

    fn expectPathSetEqual(allocator: std.mem.Allocator, src_path_set: *const std.BufSet, out_path_set: *const std.BufSet) !void {
        try std.testing.expectEqual(src_path_set.count(), out_path_set.count());

        var diff = std.BufSet.init(allocator);
        defer diff.deinit();

        iter_src: {
            var iter = src_path_set.iterator();
            while (iter.next()) |path| {
                if (! out_path_set.contains(path.*)) {
                    try diff.insert(path.*);
                }
            }
            break:iter_src;
        }
        iter_out: {
            var iter = out_path_set.iterator();
            while (iter.next()) |path| {
                if (! src_path_set.contains(path.*)) {
                    try diff.insert(path.*);
                }
            }
            break:iter_out;
        }

        try std.testing.expectEqual(0, diff.count());
    }

    test "Copy directory" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var tmp_dir = std.testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        const out_root_path = try tmp_dir.dir.realPathFileAlloc(io, ".", allocator);
        defer allocator.free(out_root_path);

        try handleGenerate(io, allocator, source_asset_dir_path, out_root_path, "out");

        var src_path_set = try collectPathSet(io, allocator, source_asset_dir_path);
        defer src_path_set.deinit();

        const out_dir_path = try std.fmt.allocPrint(allocator, "{f}", .{std.fs.path.fmtJoin(&.{out_root_path, "out"})});
        defer allocator.free(out_dir_path);
        var out_path_set = try collectPathSet(io, allocator, out_dir_path);
        defer out_path_set.deinit();

        try std.testing.expectEqual(true, src_path_set.count() > 0);
        try expectRealFile(io, allocator, out_dir_path);
        try expectPathSetEqual(allocator, &src_path_set, &out_path_set);
    }

    test "Copy directory with absolute dir path symlink" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var tmp_dir = std.testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        try tmp_dir.dir.symLink(io, source_asset_dir_path, "foo", .{});
        const source_dir_path = try tmp_dir.dir.realPathFileAlloc(io, "foo", allocator);
        defer allocator.free(source_dir_path);

        const out_root_path = try tmp_dir.dir.realPathFileAlloc(io, ".", allocator);
        defer allocator.free(out_root_path);

        try handleGenerate(io, allocator, source_dir_path, out_root_path, "out");

        var src_path_set = try collectPathSet(io, allocator, source_dir_path);
        defer src_path_set.deinit();

        const out_dir_path = try std.fmt.allocPrint(allocator, "{f}", .{std.fs.path.fmtJoin(&.{out_root_path, "out"})});
        defer allocator.free(out_dir_path);
        var out_path_set = try collectPathSet(io, allocator, out_dir_path);
        defer out_path_set.deinit();

        try std.testing.expectEqual(true, src_path_set.count() > 0);
        try expectRealFile(io, allocator, out_dir_path);
        try expectPathSetEqual(allocator, &src_path_set, &out_path_set);
    }

    test "Copy directory with absolute file path symlink" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var tmp_dir = std.testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        const source_dir_path = source_path: {
            const dir = try tmp_dir.dir.createDirPathOpen(io, "foo", .{});
            defer dir.close(io);
            
            const asset_dir = try std.Io.Dir.openDirAbsolute(io, source_asset_dir_path, .{});
            defer asset_dir.close(io);
            const asset_link_target = try asset_dir.realPathFileAlloc(io, "generate.zon", allocator);
            defer allocator.free(asset_link_target);

            try asset_dir.copyFile("base.zon", dir, "base.zon", io, .{});
            try dir.symLink(io, asset_link_target, "generate.zon", .{});
            break:source_path try dir.realPathFileAlloc(io, ".", allocator);
        };
        defer allocator.free(source_dir_path);

        const out_root_path = try tmp_dir.dir.realPathFileAlloc(io, ".", allocator);
        defer allocator.free(out_root_path);

        try handleGenerate(io, allocator, source_dir_path, out_root_path, "out");

        var src_path_set = try collectPathSet(io, allocator, source_dir_path);
        defer src_path_set.deinit();

        const out_dir_path = try std.fmt.allocPrint(allocator, "{f}", .{std.fs.path.fmtJoin(&.{out_root_path, "out"})});
        defer allocator.free(out_dir_path);
        var out_path_set = try collectPathSet(io, allocator, out_dir_path);
        defer out_path_set.deinit();

        try std.testing.expectEqual(true, src_path_set.count() > 0);
        try expectRealFile(io, allocator, out_dir_path);
        try expectPathSetEqual(allocator, &src_path_set, &out_path_set);
    }


    test "Copy directory with relatve file path symlink" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var tmp_dir = std.testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        const source_dir_path = source_path: {
            const dir = try tmp_dir.dir.createDirPathOpen(io, "foo", .{});
            defer dir.close(io);
            
            const asset_dir = try std.Io.Dir.openDirAbsolute(io, source_asset_dir_path, .{});
            defer asset_dir.close(io);

            try asset_dir.copyFile("base.zon", dir, "base.zon", io, .{});
            try dir.symLink(io, "./base.zon", "generate.zon", .{});
            break:source_path try dir.realPathFileAlloc(io, ".", allocator);
        };
        defer allocator.free(source_dir_path);

        const out_root_path = try tmp_dir.dir.realPathFileAlloc(io, ".", allocator);
        defer allocator.free(out_root_path);

        try handleGenerate(io, allocator, source_dir_path, out_root_path, "out");

        var src_path_set = try collectPathSet(io, allocator, source_dir_path);
        defer src_path_set.deinit();

        const out_dir_path = try std.fmt.allocPrint(allocator, "{f}", .{std.fs.path.fmtJoin(&.{out_root_path, "out"})});
        defer allocator.free(out_dir_path);
        var out_path_set = try collectPathSet(io, allocator, out_dir_path);
        defer out_path_set.deinit();

        try std.testing.expectEqual(true, src_path_set.count() > 0);
        try expectRealFile(io, allocator, out_dir_path);
        try expectPathSetEqual(allocator, &src_path_set, &out_path_set);
    }

    test "Copy directory with broken symlink" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var tmp_dir = std.testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        const source_dir_path = source_path: {
            const dir = try tmp_dir.dir.createDirPathOpen(io, "foo", .{});
            defer dir.close(io);
            
            const asset_dir = try std.Io.Dir.openDirAbsolute(io, source_asset_dir_path, .{});
            defer asset_dir.close(io);

            try asset_dir.copyFile("base.zon", dir, "base.zon", io, .{});
            try dir.symLink(io, "./broken.zon", "generate.zon", .{});
            break:source_path try dir.realPathFileAlloc(io, ".", allocator);
        };
        defer allocator.free(source_dir_path);

        const out_root_path = try tmp_dir.dir.realPathFileAlloc(io, ".", allocator);
        defer allocator.free(out_root_path);

        handleGenerate(io, allocator, source_dir_path, out_root_path, "out") catch {};

        var src_path_set = std.BufSet.init(allocator);
        defer src_path_set.deinit();

        const out_dir_path = try std.fmt.allocPrint(allocator, "{f}", .{std.fs.path.fmtJoin(&.{out_root_path, "out"})});
        defer allocator.free(out_dir_path);
        var out_path_set = try collectPathSet(io, allocator, out_dir_path);
        defer out_path_set.deinit();

        try expectPathSetEqual(allocator, &src_path_set, &out_path_set);
    }

    test "Copy directory with cyclic symlink" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var tmp_dir = std.testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        const source_dir_path = source_path: {
            const dir = try tmp_dir.dir.createDirPathOpen(io, "foo", .{});
            defer dir.close(io);
            
            const asset_dir = try std.Io.Dir.openDirAbsolute(io, source_asset_dir_path, .{});
            defer asset_dir.close(io);

            try asset_dir.copyFile("base.zon", dir, "base.zon", io, .{});
            try dir.symLink(io, ".", "generate.zon", .{});
            break:source_path try dir.realPathFileAlloc(io, ".", allocator);
        };
        defer allocator.free(source_dir_path);

        const out_root_path = try tmp_dir.dir.realPathFileAlloc(io, ".", allocator);
        defer allocator.free(out_root_path);

        handleGenerate(io, allocator, source_dir_path, out_root_path, "out") catch {};

        var src_path_set = std.BufSet.init(allocator);
        defer src_path_set.deinit();

        const out_dir_path = try std.fmt.allocPrint(allocator, "{f}", .{std.fs.path.fmtJoin(&.{out_root_path, "out"})});
        defer allocator.free(out_dir_path);
        var out_path_set = try collectPathSet(io, allocator, out_dir_path);
        defer out_path_set.deinit();

        try expectPathSetEqual(allocator, &src_path_set, &out_path_set);
    }
};