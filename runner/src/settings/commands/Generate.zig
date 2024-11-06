const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const log = core.Logger.SystemDirect(@import("build_options").app_context);
const help = @import("../help.zig");

const FilePath = core.FilePath;
const FilterKind = core.FilterKind;

source_dir_set: []FilePath,
schema_dir_set: []FilePath,
include_filter_set: []FilePath,
exclude_filter_set: []FilePath,
output_dir_path: FilePath,
watch: bool,

const Self = @This();
const DefaultArgs = @import("../default_args.zig").Defaults(std.meta.FieldEnum(Self));

pub fn ArgId(comptime descriptions: core.settings.DescriptionMap) type {
    return enum {
        source_dir,
        schema_dir,
        include_filter,
        exclude_filter,
        output_dir,
        watch,

        pub const Decls: []const clap.Param(@This()) = &.{
            .{.id = .source_dir, .names = .{.long = "source-dir", .short = 'i'}, .takes_value = .many},
            .{.id = .schema_dir, .names = .{.long = "schema-dir"}, .takes_value = .one},
            .{.id = .include_filter, .names = .{.long = "include-filter"}, .takes_value = .many},
            .{.id = .exclude_filter, .names = .{.long = "exclude-filter"}, .takes_value = .many},
            .{.id = .output_dir, .names = .{.long = "output-dir", .short = 'o'}, .takes_value = .one},
            .{.id = .watch, .names = .{.long = "watch"}, .takes_value = .none},
            // .{.id = ., .names = .{}, .takes_value = },
        };
        pub usingnamespace core.settings.ArgHelp(@This(), descriptions);
        pub const options: core.settings.ArgHelpOption = .{.category_name = "generate"};
    };
}

pub const strategy = core.configs.StageStrategy.init(.{
    .stage_watch = .one,
    .stage_extract = .one,
    .stage_generate = .many,
});

const PathFilter = struct {
    kind: FilterKind,
    path: FilePath,
};

pub const Builder = struct {
    allocator: std.mem.Allocator,
    source_dir_set: std.ArrayList(?FilePath),
    schema_dir_set: std.ArrayList(?FilePath),
    filter_set: std.ArrayList(PathFilter),
    filter_set_counts: std.enums.EnumArray(FilterKind, usize),
    output_dir_path: ?FilePath = null,
    watch: ?bool = null,
    defaults: DefaultArgs,

    pub fn init(allocator: std.mem.Allocator, defaults_file: ?std.fs.File) !Builder {
        const defaults = defaults: {
            if (defaults_file != null) {
                var file = defaults_file.?;
                break:defaults try DefaultArgs.loadFromFile(allocator, &file);
            }
            else {
                break:defaults try DefaultArgs.init(allocator, DefaultArgs.Map.init(.{}));
            }
        };

        return .{
            .allocator = allocator,
            .source_dir_set = std.ArrayList(?FilePath).init(allocator),
            .schema_dir_set = std.ArrayList(?FilePath).init(allocator),
            .filter_set = std.ArrayList(PathFilter).init(allocator),
            .filter_set_counts = std.enums.EnumArray(FilterKind, usize).initFill(0),
            .defaults = defaults,
       };
    }

    pub fn deinit(self: *Builder) void {
        self.source_dir_set.deinit();
        self.schema_dir_set.deinit();
        self.filter_set.deinit();
    }

    pub fn loadArgs(self: *Builder, comptime Iterator: type, iter: *Iterator) !Self {
        var diag: clap.Diagnostic = .{};
        var parser = clap.streaming.Clap(ArgId(.{}), std.process.ArgIterator){
            .params = ArgId(.{}).Decls,
            .iter = iter,
            .diagnostic = &diag,
        };

        while (true) {
            const arg_ = parser.next() catch |err| {
                try diag.report(std.io.getStdErr().writer(), err);
                return error.ShowCommandHelp;
            };
            if (arg_ == null) {
                try self.applyDefaults();
                return try self.build();
            }

            if (arg_) |arg| {
                switch (arg.param.id) {
                    .source_dir => try self.source_dir_set.append(arg.value),
                    .schema_dir => try self.schema_dir_set.append(arg.value),
                    .include_filter => {
                        if (arg.value) |v| try self.filter_set.append(.{.kind = .include , .path = v});
                        self.filter_set_counts.getPtr(.include).* += 1;
                    },
                    .exclude_filter => {
                        if (arg.value) |v| try self.filter_set.append(.{.kind = .exclude , .path = v});
                        self.filter_set_counts.getPtr(.exclude).* += 1;
                    },
                    .output_dir => self.output_dir_path = arg.value,
                    .watch => self.watch = true,
                }
            }
        }
    }

    fn applyDefaults(self: *Builder) !void {
        var iter = self.defaults.iterator();

        while (iter.next()) |entry| {
            switch (entry.key) {
                .source_dir_set => if (entry.value.tag() == .values) {
                    if (self.source_dir_set.items.len == 0) {
                        for (entry.value.values) |value| {
                            try self.source_dir_set.append(value);
                        }
                    }
                },
                .schema_dir_set => if (entry.value.tag() == .values) {
                    if (self.schema_dir_set.items.len == 0) {
                        for (entry.value.values) |value| {
                            try self.schema_dir_set.append(value);
                        }
                    }
                },
                .include_filter_set => if (entry.value.tag() == .values) {
                    if (self.filter_set_counts.get(.include) == 0) {
                        for (entry.value.values) |value| {
                            try self.filter_set.append(.{.kind = .include, .path = value});
                        }
                    }
                },
                .exclude_filter_set => if (entry.value.tag() == .values) {
                    if (self.filter_set_counts.get(.exclude) == 0) {
                        for (entry.value.values) |value| {
                            try self.filter_set.append(.{.kind = .exclude, .path = value});
                        }
                    }
                },
                .output_dir_path => if (entry.value.tag() == .values) {
                    if ((self.output_dir_path == null) and (entry.value.values.len > 0)) {
                        self.output_dir_path = entry.value.values[0];
                    }
                },
                .watch => if (entry.value.tag() == .enabled) {
                    if (self.watch == null) {
                        self.watch = entry.value.enabled;
                    }
                },
            }
        }
    }

    fn build(self: Builder) !Self {
        var base_dir = std.fs.cwd();

        var sources = std.ArrayList(FilePath).init(self.allocator);
        defer sources.deinit();
        for (self.source_dir_set.items) |path_| {
            if (path_) |path| {
                _ = base_dir.statFile(path) catch {
                    log.warn("Cannot access source folder: {s}", .{path});
                    return error.SettingLoadFailed;
                };

                try sources.append(try base_dir.realpathAlloc(self.allocator, path));
            }
        }

        var schemas = try std.ArrayList(FilePath).initCapacity(self.allocator, self.schema_dir_set.items.len);
        defer schemas.deinit();
        for (self.schema_dir_set.items) |path_| {
            if (path_) |path| {
                _ = base_dir.statFile(path) catch |err| {
                    log.warn("Cannot access source folder/file: {s} ({s})", .{path, @errorName(err)});
                    return error.SettingLoadFailed;
                };

                try schemas.append(try base_dir.realpathAlloc(self.allocator, path));
            }
        }

        if ((sources.items.len == 0) and (schemas.items.len == 0)) {
            log.warn("Need to specify SQL source and/or schema folder at least one", .{});
            return error.SettingLoadFailed;
        }

        var include_filters = try std.ArrayList(core.FilePath).initCapacity(self.allocator, self.filter_set.items.len);
        defer include_filters.deinit();
        var exclude_filters = try std.ArrayList(core.FilePath).initCapacity(self.allocator, self.filter_set.items.len);
        defer exclude_filters.deinit();

        for (self.filter_set.items) |filter| {
            switch (filter.kind) {
                .include => {
                    try include_filters.append(try self.allocator.dupe(u8, filter.path));
                },
                .exclude => {
                    try exclude_filters.append(try self.allocator.dupe(u8, filter.path));
                }
            }
        }

        const output_dir_path = path: {
            if (self.output_dir_path == null) {
                log.warn("Need to specify output folder", .{});
                return error.SettingLoadFailed;
            }
            else {
                try base_dir.makePath(self.output_dir_path.?);
                break :path try base_dir.realpathAlloc(self.allocator, self.output_dir_path.?);
            }
        };

        return .{
            .source_dir_set = try sources.toOwnedSlice(),
            .schema_dir_set = try schemas.toOwnedSlice(),
            .include_filter_set = try include_filters.toOwnedSlice(),
            .exclude_filter_set = try exclude_filters.toOwnedSlice(),
            .output_dir_path = output_dir_path,
            .watch = self.watch orelse false,
        };
    }
};
