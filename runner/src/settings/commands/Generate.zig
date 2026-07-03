const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const FilePath = core.types.FilePath;
const FilterKind = core.types.FilterKind;

const ArgScanner = core.settings.types.ArgScanner;
const ArgParserPair = core.settings.types.ArgParserPair;

const BaseSetting = @import("./BaseSetting.zig");

const BaseSettingArgId = BaseSetting.ArgId(.{});
const GenerateArgId = ArgId(.{});
const Defaults = @import("../default_args.zig").Defaults(std.meta.FieldEnum(GenerateArgId));

source_dir_set: []const FilePath,
schema_dir_set: []const FilePath,
include_filter_set: []const FilePath,
exclude_filter_set: []const FilePath,
output_dir_path: FilePath,

const Setting = @This();

pub fn deinit(self: *Setting, allocator: std.mem.Allocator) void {
    _ = self;
    _ = allocator;
} 

pub fn ArgId(comptime descriptions: core.settings.types.DescriptionMap) type {
    return enum {
        source_dir_set,
        schema_dir_set,
        include_filter_set,
        exclude_filter_set,
        output_dir_path,

        pub const Decls: []const clap.Param(@This()) = &.{
            .{.id = .source_dir_set, .names = .{.long = "source-dir", .short = 'i'}, .takes_value = .many},
            .{.id = .schema_dir_set, .names = .{.long = "schema-dir"}, .takes_value = .many},
            .{.id = .include_filter_set, .names = .{.long = "include-filter"}, .takes_value = .many},
            .{.id = .exclude_filter_set, .names = .{.long = "exclude-filter"}, .takes_value = .many},
            .{.id = .output_dir_path, .names = .{.long = "output-dir", .short = 'o'}, .takes_value = .one},
            // .{.id = ., .names = .{}, .takes_value = },
        };

        const arg_view = core.settings.types.ArgHelp(@This(), descriptions);
        pub const description = arg_view.description;
        pub const value = arg_view.value;
    };
}

pub const strategies = core.configs.types.StageStrategy.init(.{
    .watch = .one,
    .extract = .one,
    .generate = .many,
});

const PathFilter = struct {
    kind: FilterKind,
    path: FilePath,
};

pub fn Builder(comptime ArgIterator: type) type {
    return struct {
        build_log_style: core.Logger.LogStyle,
        source_dir_set: std.ArrayListUnmanaged(?FilePath) = .empty,
        schema_dir_set: std.ArrayListUnmanaged(?FilePath) = .empty,
        filter_set: std.ArrayListUnmanaged(PathFilter) = .empty,
        filter_set_counts: std.enums.EnumArray(FilterKind, usize) = std.enums.EnumArray(FilterKind, usize).initFill(0),
        output_dir_path: ?FilePath = null,

        pub fn deinit(self: *Builder(ArgIterator), allocator: std.mem.Allocator) void {
            self.source_dir_set.deinit(allocator);
            self.schema_dir_set.deinit(allocator);
            self.filter_set.deinit(allocator);
        }

        pub fn fromArgs(
            allocator: std.mem.Allocator, 
            scanner: *ArgScanner(ArgIterator), 
            base_builder: *BaseSetting.Builder(ArgIterator), 
            log_style: core.Logger.LogStyle) !Builder(ArgIterator) 
        {
            var diag: clap.Diagnostic = .{}; 
            var parsers = ArgParserPair(GenerateArgId, BaseSettingArgId, ArgIterator).init(scanner, &diag);

            var builder: Builder(ArgIterator) = .{ .build_log_style = log_style };

            while (scanner.scan()) {
                const next_arg = parsers.next(scanner) catch |err| {
                    if (log_style == .stderr) {
                        try core.log_supports.reportClapError(&diag, err);
                    }
                    return err;
                };
                if (next_arg == null) break;

                switch (next_arg.?) {
                    .base => |arg| {
                        try builder.handleArg(allocator, arg);
                    },
                    .extra => |arg| {
                        base_builder.handleArg(allocator, arg) catch return error.ShowCommandHelp;
                    }
                }
            }

            return builder;
        }

        fn handleArg(self: *Builder(ArgIterator), allocator: std.mem.Allocator, arg: clap.streaming.Arg(GenerateArgId)) !void {
            switch (arg.param.id) {
                .source_dir_set => try self.source_dir_set.append(allocator, arg.value),
                .schema_dir_set => try self.schema_dir_set.append(allocator, arg.value),
                .include_filter_set => {
                    if (arg.value) |v| try self.filter_set.append(allocator, .{.kind = .include , .path = v});
                    self.filter_set_counts.getPtr(.include).* += 1;
                },
                .exclude_filter_set => {
                    if (arg.value) |v| try self.filter_set.append(allocator, .{.kind = .exclude , .path = v});
                    self.filter_set_counts.getPtr(.exclude).* += 1;
                },
                .output_dir_path => self.output_dir_path = arg.value,
            }
        }

        fn applyDefaults(ptr: *anyopaque, allocator: std.mem.Allocator, defaults: *Defaults) !void {
            var self: *Builder(ArgIterator) = @ptrCast(@alignCast(ptr));
            var iter = defaults.iterator();

            while (iter.next()) |entry| {
                switch (entry.key) {
                    .source_dir_set => if (entry.value.tag() == .values) {
                        if (self.source_dir_set.items.len == 0) {
                            for (entry.value.values) |value| {
                                try self.source_dir_set.append(allocator, value);
                            }
                        }
                    },
                    .schema_dir_set => if (entry.value.tag() == .values) {
                        if (self.schema_dir_set.items.len == 0) {
                            for (entry.value.values) |value| {
                                try self.schema_dir_set.append(allocator, value);
                            }
                        }
                    },
                    .include_filter_set => if (entry.value.tag() == .values) {
                        if (self.filter_set_counts.get(.include) == 0) {
                            for (entry.value.values) |value| {
                                try self.filter_set.append(allocator, .{.kind = .include, .path = value});
                            }
                        }
                    },
                    .exclude_filter_set => if (entry.value.tag() == .values) {
                        if (self.filter_set_counts.get(.exclude) == 0) {
                            for (entry.value.values) |value| {
                                try self.filter_set.append(allocator, .{.kind = .exclude, .path = value});
                            }
                        }
                    },
                    .output_dir_path => if (entry.value.tag() == .values) {
                        if ((self.output_dir_path == null) and (entry.value.values.len > 0)) {
                            self.output_dir_path = try allocator.dupe(u8, entry.value.values[0]);
                        }
                    },
                }
            }
        }

        pub fn build(self: *Builder(ArgIterator), io: std.Io, allocator: std.mem.Allocator, env: *const std.process.Environ.Map, options: core.configs.supports.FileResolveOptions) !Setting {
            var arena = std.heap.ArenaAllocator.init(allocator);
            defer arena.deinit();
            
            var builder_default: Builder(ArgIterator) = .{ .build_log_style = self.build_log_style };            

            if (try core.configs.supports.resolveFileCandidate(io, arena.allocator(), env, options)) |file| {
                defer file.close(io);

                const callback: Defaults.ApplyDefaultHandler = .{ 
                    .ptr = &builder_default, 
                    .handler = Builder(ArgIterator).applyDefaults 
                };
                try Defaults.loadFromFile(io, arena.allocator(), file, self.build_log_style, callback);
            }

            const base_dir = std.Io.Dir.cwd();
            var has_err = false;
            
            const sources = sources: {
                const source_dir_set = if (self.source_dir_set.items.len > 0) self.source_dir_set.items else builder_default.source_dir_set.items;
        
                const slice = try allocator.alloc(FilePath, source_dir_set.len);        
                for (source_dir_set, 0..) |path, i| {
                    if (base_dir.realPathFileAlloc(io, path.?, allocator)) |path_abs| {
                        slice[i] = path_abs;
                    }
                    else |err| {
                        has_err = true;
                        if (self.build_log_style == .stderr) {
                            std.log.warn("Cannot access source folder/name: {s}, err: {}", .{path.?, err});
                        }
                    }
                }
                break:sources slice;
            };
            if (sources.len == 0) {
                has_err = true;
                if (self.build_log_style == .stderr) {
                    std.log.warn("Need to specify SQL source and/or schema folder at least one", .{});
                }
            }

            const schemas = schemas: {
                const schema_dir_set = if (self.schema_dir_set.items.len > 0) self.schema_dir_set.items else builder_default.schema_dir_set.items;
                const slice = try allocator.alloc(FilePath, schema_dir_set.len);
                for (schema_dir_set, 0..) |path, i| {
                    if (base_dir.realPathFileAlloc(io, path.?, allocator)) |path_abs| {
                        slice[i] = path_abs;
                    }
                    else |err| {
                        has_err = true;
                        if (self.build_log_style == .stderr) {
                            std.log.warn("Cannot access schema folder/name: {s}, err: {}", .{path.?, err});
                        }
                    }
                }
                break:schemas slice;
            };

            var include_filters: std.ArrayListUnmanaged(core.types.FilePath) = .empty;
            var exclude_filters: std.ArrayListUnmanaged(core.types.FilePath) = .empty;
            filters: {
                const filter_set = if (self.filter_set.items.len > 0) self.filter_set.items else builder_default.filter_set.items;
                for (filter_set) |filter| {
                    switch (filter.kind) {
                        .include => {
                            try include_filters.append(allocator, try allocator.dupe(u8, filter.path));
                        },
                        .exclude => {
                            try exclude_filters.append(allocator, try allocator.dupe(u8, filter.path));
                        }
                    }
                }
                break:filters;
            }
            const output_dir_path = path: {
                if (self.output_dir_path orelse builder_default.output_dir_path) |path| {
                    try base_dir.createDirPath(io, path);
                    break :path try base_dir.realPathFileAlloc(io, path, allocator);
                }
                else {
                    has_err = true;
                    if (self.build_log_style == .stderr) {
                        std.log.warn("Need to specify output folder", .{});
                    }
                    break:path null;
                }
            };

            if (has_err) {
                return error.ShowCommandHelp;
            }

            return .{
                .source_dir_set = sources,
                .schema_dir_set = schemas,
                .include_filter_set = try include_filters.toOwnedSlice(allocator),
                .exclude_filter_set = try exclude_filters.toOwnedSlice(allocator),
                .output_dir_path = output_dir_path.?,
            };
        }
    };
}

test "generate setting test" {
    std.testing.refAllDecls(@This());
}

pub const tests = struct {
    const ConfigFileCandidates = core.configs.types.ConfigFileCandidates;
    const FileResolveOptions = core.configs.supports.FileResolveOptions;
    const writeAssetFile = @import("../../supports/test_support.zig").writeAssetFile;
    const TetsArgsIterator = clap.args.SliceIterator;

    test "All explicit args" {
        const io = std.testing.io;
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        var env = try std.testing.environ.createMap(allocator);
        defer env.deinit();

        var tmp_dir = std.testing.tmpDir(.{});
        defer tmp_dir.cleanup();
        const tmp_dir_path_abs = try tmp_dir.dir.realPathFileAlloc(io, ".", allocator);

        const e1_path = dir: {
            const d = try tmp_dir.dir.createDirPathOpen(io, "explicit-src-1", .{});
            defer d.close(io);
            break:dir try d.realPathFileAlloc(io, ".", allocator);
        };
        const e2_path = dir: {
            const d = try tmp_dir.dir.createDirPathOpen(io, "explicit-src-2", .{});
            defer d.close(io);
            break:dir try d.realPathFileAlloc(io, ".", allocator);
        };
        const e3_path = dir: {
            const d = try tmp_dir.dir.createDirPathOpen(io, "explicit-schema-1", .{});
            defer d.close(io);
            break:dir try d.realPathFileAlloc(io, ".", allocator);
        };
        const e4_path = dir: {
            const d = try tmp_dir.dir.createDirPathOpen(io, "explicit-schema-2", .{});
            defer d.close(io);
            break:dir try d.realPathFileAlloc(io, ".", allocator);
        };
        const e5_path = try std.fmt.allocPrint(allocator, "{f}", .{std.fs.path.fmtJoin(&.{tmp_dir_path_abs, "explicit-out"})});

        var args: std.ArrayListUnmanaged(core.types.Symbol) = .empty;
        try args.appendSlice(allocator, &.{
            "generate",
            "--source-dir", e1_path,
            "--source-dir", e2_path,
            "--schema-dir", e3_path,
            "--schema-dir", e4_path,
            "--include-filter=foo",
            "--include-filter=bar",
            "--exclude-filter=baz",
            "--exclude-filter=quax",
            "--output-dir", e5_path,
            "--watch",
        });

        const defaults_source = ".{}";

        const file_candidates: ConfigFileCandidates = .{ .current_dir = try tmp_dir.dir.realPathFileAlloc(io, ".", allocator), };
        const options: FileResolveOptions = .{
            .command = "generate", .scope = "default", .category = .defaults, .root = file_candidates, .default_scope = "default"
        };

        try writeAssetFile(&tmp_dir, options, defaults_source);

        var iter: TetsArgsIterator = .{.args = args.items};
        var scanner = ArgScanner(TetsArgsIterator).init(&iter);

        var base_bulder_res = try BaseSetting.Builder(TetsArgsIterator).fromArgs(allocator, &scanner, .discard);
        var builder = try Builder(TetsArgsIterator).fromArgs(allocator, &scanner, &base_bulder_res.builder, .discard);
        defer builder.deinit(allocator);

        const setting: Setting = try builder.build(io, allocator, &env, options);

        try std.testing.expectEqualDeep(&[_][]const u8{e1_path, e2_path}, setting.source_dir_set);
        try std.testing.expectEqualDeep(&[_][]const u8{e3_path, e4_path}, setting.schema_dir_set);
        try std.testing.expectEqualDeep(&[_][]const u8{"foo", "bar"}, setting.include_filter_set);
        try std.testing.expectEqualDeep(&[_][]const u8{"baz", "quax"}, setting.exclude_filter_set);
        try std.testing.expectEqualStrings(e5_path, setting.output_dir_path);
    }

    test "All default args" {
        const io = std.testing.io;
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        var env = try std.testing.environ.createMap(allocator);
        defer env.deinit();

        var tmp_dir = std.testing.tmpDir(.{});
        defer tmp_dir.cleanup();
        const tmp_dir_path_abs = try tmp_dir.dir.realPathFileAlloc(io, ".", allocator);

        const d1_path = dir: {
            const d = try tmp_dir.dir.createDirPathOpen(io, "default-src-1", .{});
            defer d.close(io);
            break:dir try d.realPathFileAlloc(io, ".", allocator);
        };
        const d2_path = dir: {
            const d = try tmp_dir.dir.createDirPathOpen(io, "default-src-2", .{});
            defer d.close(io);
            break:dir try d.realPathFileAlloc(io, ".", allocator);
        };
        const d3_path = dir: {
            const d = try tmp_dir.dir.createDirPathOpen(io, "default-schema-1", .{});
            defer d.close(io);
            break:dir try d.realPathFileAlloc(io, ".", allocator);
        };
        const d4_path = dir: {
            const d = try tmp_dir.dir.createDirPathOpen(io, "default-schema-2", .{});
            defer d.close(io);
            break:dir try d.realPathFileAlloc(io, ".", allocator);
        };
        const d5_path = try std.fmt.allocPrint(allocator, "{f}", .{std.fs.path.fmtJoin(&.{tmp_dir_path_abs, "default-out"})});

        var buffer: std.Io.Writer.Allocating = .init(allocator);
        try buffer.writer.print(
            \\.{{
            \\    .source_dir_set = .{{ .values = .{{ "{s}", "{s}" }} }},
            \\    .schema_dir_set = .{{ .values = .{{ "{s}", "{s}" }} }},
            \\    .include_filter_set = .{{ .values = .{{ "{s}", "{s}" }} }},
            \\    .exclude_filter_set = .{{ .values = .{{ "{s}", "{s}" }} }},
            \\    .output_dir_path = .{{ .values = .{{ "{s}" }} }},
            \\}}
            , .{ d1_path, d2_path, d3_path, d4_path, "quax", "baz", "bar", "foo", d5_path }
        );
        try buffer.writer.flush();

        const file_candidates: ConfigFileCandidates = .{ .current_dir = try tmp_dir.dir.realPathFileAlloc(io, ".", allocator), };
        const options: FileResolveOptions = .{
            .command = "generate", .scope = "default", .category = .defaults, .root = file_candidates, .default_scope = "default"
        };

        try writeAssetFile(&tmp_dir, options, buffer.written());
        
        var iter: TetsArgsIterator = .{.args = &.{"generate"}};
        var scanner = ArgScanner(TetsArgsIterator).init(&iter);

        var base_bulder_res = try BaseSetting.Builder(TetsArgsIterator).fromArgs(allocator, &scanner, .discard);
        var builder = try Builder(TetsArgsIterator).fromArgs(allocator, &scanner, &base_bulder_res.builder, .discard);
        defer builder.deinit(allocator);

        const setting: Setting = try builder.build(io, allocator, &env, options);

        try std.testing.expectEqualDeep(&[_][]const u8{d1_path, d2_path}, setting.source_dir_set);
        try std.testing.expectEqualDeep(&[_][]const u8{d3_path, d4_path}, setting.schema_dir_set);
        try std.testing.expectEqualDeep(&[_][]const u8{"quax", "baz"}, setting.include_filter_set);
        try std.testing.expectEqualDeep(&[_][]const u8{"bar", "foo"}, setting.exclude_filter_set);
        try std.testing.expectEqualStrings(d5_path, setting.output_dir_path);
    }

    test "Explict + default args" {
        const io = std.testing.io;
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        var env = try std.testing.environ.createMap(allocator);
        defer env.deinit();

        var tmp_dir = std.testing.tmpDir(.{});
        defer tmp_dir.cleanup();
        const tmp_dir_path_abs = try tmp_dir.dir.realPathFileAlloc(io, ".", allocator);

        const d1_path = dir: {
            const d = try tmp_dir.dir.createDirPathOpen(io, "default-src-1", .{});
            defer d.close(io);
            break:dir try d.realPathFileAlloc(io, ".", allocator);
        };
        const d2_path = dir: {
            const d = try tmp_dir.dir.createDirPathOpen(io, "default-src-2", .{});
            defer d.close(io);
            break:dir try d.realPathFileAlloc(io, ".", allocator);
        };
        const d3_path = dir: {
            const d = try tmp_dir.dir.createDirPathOpen(io, "default-schema-1", .{});
            defer d.close(io);
            break:dir try d.realPathFileAlloc(io, ".", allocator);
        };
        const d4_path = dir: {
            const d = try tmp_dir.dir.createDirPathOpen(io, "default-schema-2", .{});
            defer d.close(io);
            break:dir try d.realPathFileAlloc(io, ".", allocator);
        };
        const d5_path = try std.fmt.allocPrint(allocator, "{f}", .{std.fs.path.fmtJoin(&.{tmp_dir_path_abs, "default-out"})});

        var args: std.ArrayListUnmanaged(core.types.Symbol) = .empty;
        try args.appendSlice(allocator, &.{
            "generate",
            "--source-dir", d1_path,
            "--source-dir", d2_path,
            "--schema-dir", d3_path,
            "--schema-dir", d4_path,
            "--include-filter=foo",
            "--include-filter=bar",
            "--exclude-filter=baz",
            "--exclude-filter=quax",
            "--output-dir", d5_path,
        });

        var buffer: std.Io.Writer.Allocating = .init(allocator);
        try buffer.writer.print(
            \\.{{
            \\    .source_dir_set = .{{ .values = .{{ "{s}", "{s}" }} }},
            \\    .schema_dir_set = .{{ .values = .{{ "{s}", "{s}" }} }},
            \\    .include_filter_set = .{{ .values = .{{ "{s}", "{s}" }} }},
            \\    .exclude_filter_set = .{{ .values = .{{ "{s}", "{s}" }} }},
            \\    .output_dir_path = .{{ .values = .{{ "{s}" }} }},
            \\}}
            , .{ "/path/to/default-src-1", "/path/to/default-src-2", "/path/to/default-schema-1", "/path/to/default-schema-2", "quax", "baz", "bar", "foo", "explicit-out" }
        );
        try buffer.writer.flush();

        const file_candidates: ConfigFileCandidates = .{ .current_dir = try tmp_dir.dir.realPathFileAlloc(io, ".", allocator), };
        const options: FileResolveOptions = .{
            .command = "generate", .scope = "default", .category = .defaults, .root = file_candidates, .default_scope = "default"
        };

        try writeAssetFile(&tmp_dir, options, buffer.written());
        
        var iter: TetsArgsIterator = .{.args = args.items};
        var scanner = ArgScanner(TetsArgsIterator).init(&iter);

        var base_bulder_res = try BaseSetting.Builder(TetsArgsIterator).fromArgs(allocator, &scanner, .discard);
        var builder = try Builder(TetsArgsIterator).fromArgs(allocator, &scanner, &base_bulder_res.builder, .stderr);
        defer builder.deinit(allocator);

        const setting: Setting = try builder.build(io, allocator, &env, options);

        try std.testing.expectEqualDeep(&[_][]const u8{d1_path, d2_path}, setting.source_dir_set);
        try std.testing.expectEqualDeep(&[_][]const u8{d3_path, d4_path}, setting.schema_dir_set);
        try std.testing.expectEqualDeep(&[_][]const u8{"foo", "bar"}, setting.include_filter_set);
        try std.testing.expectEqualDeep(&[_][]const u8{"baz", "quax"}, setting.exclude_filter_set);
        try std.testing.expectEqualStrings(d5_path, setting.output_dir_path);
    }
};