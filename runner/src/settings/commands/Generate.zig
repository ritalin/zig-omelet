const std = @import("std");
const clap = @import("clap");
const core = @import("core");

// TODO:
// const log = core.Logger.SystemDirect(@import("build_options").app_context);
// const help = @import("../help.zig");

const FilePath = core.types.FilePath;
const FilterKind = core.types.FilterKind;

const GenerateArgId = ArgId(.{});
const Defaults = @import("../default_args.zig").Defaults(std.meta.FieldEnum(GenerateArgId));

source_dir_set: []FilePath,
schema_dir_set: []FilePath,
include_filter_set: []FilePath,
exclude_filter_set: []FilePath,
output_dir_path: FilePath,
watch: bool,

const Self = @This();

pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
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
        watch,

        pub const Decls: []const clap.Param(@This()) = &.{
            .{.id = .source_dir_set, .names = .{.long = "source-dir", .short = 'i'}, .takes_value = .many},
            .{.id = .schema_dir_set, .names = .{.long = "schema-dir"}, .takes_value = .one},
            .{.id = .include_filter_set, .names = .{.long = "include-filter"}, .takes_value = .many},
            .{.id = .exclude_filter_set, .names = .{.long = "exclude-filter"}, .takes_value = .many},
            .{.id = .output_dir_path, .names = .{.long = "output-dir", .short = 'o'}, .takes_value = .one},
            .{.id = .watch, .names = .{.long = "watch"}, .takes_value = .none},
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

pub const Builder = struct {
    allocator: std.mem.Allocator,
    source_dir_set: std.ArrayListUnmanaged(?FilePath),
    schema_dir_set: std.ArrayListUnmanaged(?FilePath),
    filter_set: std.ArrayListUnmanaged(PathFilter),
    filter_set_counts: std.enums.EnumArray(FilterKind, usize),
    output_dir_path: ?FilePath = null,
    watch: ?bool = null,

// TODO:
//     defaults: DefaultArgs,

//     pub fn init(allocator: std.mem.Allocator, defaults_file: ?std.fs.File) !Builder {
//         const defaults = defaults: {
//             if (defaults_file != null) {
//                 var file = defaults_file.?;
//                 break:defaults try DefaultArgs.loadFromFile(allocator, &file);
//             }
//             else {
//                 break:defaults try DefaultArgs.init(allocator, DefaultArgs.Map.init(.{}));
//             }
//         };
//     }

    pub fn deinit(self: *Builder) void {
        self.source_dir_set.deinit(self.allocator);
        self.schema_dir_set.deinit(self.allocator);
        self.filter_set.deinit(self.allocator);
    }

    pub fn fromArgs(allocator: std.mem.Allocator, iter: *std.process.Args.Iterator) !Builder {
        var builder: Builder = .{
            .allocator = allocator,
            .source_dir_set = .empty,
            .schema_dir_set = .empty,
            .filter_set = .empty,
            .filter_set_counts = std.enums.EnumArray(FilterKind, usize).initFill(0),
        };

        var diag: clap.Diagnostic = .{};
        var parser = clap.streaming.Clap(GenerateArgId, std.process.Args.Iterator){
            .params = GenerateArgId.Decls,
            .iter = iter,
            .diagnostic = &diag,
        };

        while (true) {
            const next_arg = parser.next() catch |err| {
                try core.log_supports.reportClapError(&diag, err);
                return error.ShowCommandHelp;
            };
            if (next_arg == null) {
                return builder;
            }
            const arg = next_arg.?;

            switch (arg.param.id) {
                .source_dir_set => try builder.source_dir_set.append(allocator, arg.value),
                .schema_dir_set => try builder.schema_dir_set.append(allocator, arg.value),
                .include_filter_set => {
                    if (arg.value) |v| try builder.filter_set.append(allocator, .{.kind = .include , .path = v});
                    builder.filter_set_counts.getPtr(.include).* += 1;
                },
                .exclude_filter_set => {
                    if (arg.value) |v| try builder.filter_set.append(allocator, .{.kind = .exclude , .path = v});
                    builder.filter_set_counts.getPtr(.exclude).* += 1;
                },
                .output_dir_path => builder.output_dir_path = arg.value,
                .watch => builder.watch = true,
            }
        }
    }

    fn applyDefaults(ptr: *anyopaque, defaults: *Defaults) !void {
        var self: *Builder = @ptrCast(@alignCast(ptr));
        var iter = defaults.iterator();

        while (iter.next()) |entry| {
            switch (entry.key) {
                .source_dir_set => if (entry.value.tag() == .values) {
                    if (self.source_dir_set.items.len == 0) {
                        for (entry.value.values) |value| {
                            try self.source_dir_set.append(self.allocator, value);
                        }
                    }
                },
                .schema_dir_set => if (entry.value.tag() == .values) {
                    if (self.schema_dir_set.items.len == 0) {
                        for (entry.value.values) |value| {
                            try self.schema_dir_set.append(self.allocator, value);
                        }
                    }
                },
                .include_filter_set => if (entry.value.tag() == .values) {
                    if (self.filter_set_counts.get(.include) == 0) {
                        for (entry.value.values) |value| {
                            try self.filter_set.append(self.allocator, .{.kind = .include, .path = value});
                        }
                    }
                },
                .exclude_filter_set => if (entry.value.tag() == .values) {
                    if (self.filter_set_counts.get(.exclude) == 0) {
                        for (entry.value.values) |value| {
                            try self.filter_set.append(self.allocator, .{.kind = .exclude, .path = value});
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

    pub fn build(self: *Builder, io: std.Io, options: core.configs.supports.FileResolveOptions) !Self {
        if (try core.configs.supports.resolveFileCandidate(io, self.allocator, options)) |file| {
            defer file.close(io);

            const callback: Defaults.ApplyDefaultHandler = .{ .ptr = self, .handler = Builder.applyDefaults };
            try Defaults.loadFromFile(io, self.allocator, file, callback);
        }

        const base_dir = std.Io.Dir.cwd();
        var has_err = false;
        
        const sources = sources: {
            const slice = try self.allocator.alloc(FilePath, self.source_dir_set.items.len);        
            for (self.source_dir_set.items, 0..) |path, i| {
                if (base_dir.realPathFileAlloc(io, path.?, self.allocator)) |path_abs| {
                    slice[i] = path_abs;
                }
                else |err| {
                    has_err = true;
                    std.log.warn("Cannot access source folder/name: {s}, err: {}", .{path.?, err});
                }
            }
            break:sources slice;
        };
        const schemas = schemas: {
            const slice = try self.allocator.alloc(FilePath, self.schema_dir_set.items.len);
            for (self.schema_dir_set.items, 0..) |path, i| {
                if (base_dir.realPathFileAlloc(io, path.?, self.allocator)) |path_abs| {
                    slice[i] = path_abs;
                }
                else |err| {
                    has_err = true;
                    std.log.warn("Cannot access schema folder/name: {s}, err: {}", .{path.?, err});
                }
            }
            break:schemas slice;
        };

        if ((sources.len == 0) and (schemas.len == 0)) {
            has_err = true;
            std.log.warn("Need to specify SQL source and/or schema folder at least one", .{});
        }

        var include_filters: std.ArrayListUnmanaged(core.types.FilePath) = .empty;
        var exclude_filters: std.ArrayListUnmanaged(core.types.FilePath) = .empty;
        filters: {
            for (self.filter_set.items) |filter| {
                switch (filter.kind) {
                    .include => {
                        try include_filters.append(self.allocator, try self.allocator.dupe(u8, filter.path));
                    },
                    .exclude => {
                        try exclude_filters.append(self.allocator, try self.allocator.dupe(u8, filter.path));
                    }
                }
            }
            break:filters;
        }
        const output_dir_path = path: {
            if (self.output_dir_path == null) {
                has_err = true;
                std.log.warn("Need to specify output folder", .{});
                break:path null;
            }
            else {
                try base_dir.createDirPath(io, self.output_dir_path.?);
                break :path try base_dir.realPathFileAlloc(io, self.output_dir_path.?, self.allocator);
            }
        };

        if (has_err) {
            return error.LoadSettingFailed;
        }

        return .{
            .source_dir_set = sources,
            .schema_dir_set = schemas,
            .include_filter_set = try include_filters.toOwnedSlice(self.allocator),
            .exclude_filter_set = try exclude_filters.toOwnedSlice(self.allocator),
            .output_dir_path = output_dir_path.?,
            .watch = self.watch orelse false,
        };
    }




//     }
};
