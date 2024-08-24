const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const log = core.Logger.SystemDirect(@import("build_options").app_context);
const help = @import("../help.zig");

const FilePath = core.FilePath;
const FilterKind = core.FilterKind;

source_dir_set: []FilePath,
schema_dir_set: []FilePath,
filter_set: []PathFilter,
output_dir_path: FilePath,
watch: bool,

const Self = @This();

pub fn loadArgs(arena: *std.heap.ArenaAllocator, comptime Iterator: type, iter: *Iterator) !Self {
    var diag: clap.Diagnostic = .{};
    var parser = clap.streaming.Clap(ArgId(.{}), std.process.ArgIterator){
        .params = ArgId(.{}).Decls,
        .iter = iter,
        .diagnostic = &diag,
    };

    var builder = Builder.init(arena.child_allocator);
    defer builder.deinit();

    while (true) {
        const arg_ = parser.next() catch |err| {
            try diag.report(std.io.getStdErr().writer(), err);
            return error.ShowCommandHelp;
        };
        if (arg_ == null) {
            return try builder.build(arena.allocator());
        }

        if (arg_) |arg| {
            switch (arg.param.id) {
                .source_dir_path => try builder.source_dir_set.append(arg.value),
                .schema_dir_path => try builder.schema_dir_set.append(arg.value),
                .include_filter => {
                    if (arg.value) |v| try builder.filter_set.append(.{.kind = .include , .path = v});
                },
                .exclude_filter => {
                    if (arg.value) |v| try builder.filter_set.append(.{.kind = .exclude , .path = v});
                },
                .output_dir_path => builder.output_dir_path = arg.value,
                .watch => builder.watch = true,
            }
        }
    }
}

pub fn ArgId(comptime descriptions: core.settings.DescriptionMap) type {
    return enum {
        source_dir_path,
        schema_dir_path,
        include_filter,
        exclude_filter,
        output_dir_path,
        watch,

        pub const Decls: []const clap.Param(@This()) = &.{
            .{.id = .source_dir_path, .names = .{.long = "source-dir", .short = 'i'}, .takes_value = .many},
            .{.id = .schema_dir_path, .names = .{.long = "schema-dir"}, .takes_value = .one},
            .{.id = .include_filter, .names = .{.long = "include-filter"}, .takes_value = .many},
            .{.id = .exclude_filter, .names = .{.long = "exclude-filter"}, .takes_value = .many},
            .{.id = .output_dir_path, .names = .{.long = "output-dir", .short = 'o'}, .takes_value = .one},
            .{.id = .watch, .names = .{.long = "watch"}, .takes_value = .none},
            // .{.id = ., .names = .{}, .takes_value = },
        };
        pub usingnamespace core.settings.ArgHelp(@This(), descriptions);
        pub const options: core.settings.ArgHelpOption = .{.category_name = "generate"};
    };
}

const PathFilter = struct {
    kind: FilterKind,
    path: FilePath,
};

const Builder = struct {
    source_dir_set: std.ArrayList(?FilePath),
    schema_dir_set: std.ArrayList(?FilePath),
    filter_set: std.ArrayList(PathFilter),
    output_dir_path: ?FilePath = null,
    watch: bool = false,

    pub fn init(allocator: std.mem.Allocator) Builder {
        return .{
            .source_dir_set = std.ArrayList(?FilePath).init(allocator),
            .schema_dir_set = std.ArrayList(?FilePath).init(allocator),
            .filter_set = std.ArrayList(PathFilter).init(allocator),
       };
    }

    pub fn deinit(self: *Builder) void {
        self.source_dir_set.deinit();
        self.schema_dir_set.deinit();
        self.filter_set.deinit();
    }

    pub fn build(self: Builder, allocator: std.mem.Allocator) !Self {
        var base_dir = std.fs.cwd();

        var sources = std.ArrayList(FilePath).init(allocator);
        defer sources.deinit();
        for (self.source_dir_set.items) |path_| {
            if (path_) |path| {
                _ = base_dir.statFile(path) catch {
                    log.warn("Cannot access source folder: {s}", .{path});
                    return error.SettingLoadFailed;
                };

                try sources.append(try base_dir.realpathAlloc(allocator, path));
            }
        }

        var schemas = try std.ArrayList(FilePath).initCapacity(allocator, self.schema_dir_set.items.len);
        defer schemas.deinit();
        for (self.schema_dir_set.items) |path_| {
            if (path_) |path| {
                _ = base_dir.statFile(path) catch {
                    log.warn("Cannot access source folder: {s}", .{path});
                    return error.SettingLoadFailed;
                };

                try schemas.append(try base_dir.realpathAlloc(allocator, path));
            }
        }

        if ((sources.items.len == 0) and (schemas.items.len == 0)) {
            log.warn("Need to specify SQL source and/or schema folder at least one", .{});
            return error.SettingLoadFailed;
        }

        var filters = try std.ArrayList(PathFilter).initCapacity(allocator, self.filter_set.items.len);
        defer filters.deinit();
        for (self.filter_set.items) |filter| {
            try filters.append(.{.kind = filter.kind, .path = try allocator.dupe(u8, filter.path)});
        }

        const output_dir_path = path: {
            if (self.output_dir_path == null) {
                log.warn("Need to specify output folder", .{});
                return error.SettingLoadFailed;
            }
            else {
                try base_dir.makePath(self.output_dir_path.?);
                break :path try base_dir.realpathAlloc(allocator, self.output_dir_path.?);
            }
        };

        return .{
            .source_dir_set = try sources.toOwnedSlice(),
            .schema_dir_set = try schemas.toOwnedSlice(),
            .filter_set = try filters.toOwnedSlice(),
            .output_dir_path = output_dir_path,
            .watch = self.watch,
        };
    }
};
