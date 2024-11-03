const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const DescriptionItem = core.settings.DescriptionItem;
const log = core.Logger.SystemDirect(@import("build_options").app_context);
const PathMatcher = @import("./PathMatcher.zig").PathMatcher(u21);

const Setting = @This();

arena: *std.heap.ArenaAllocator,
endpoints: core.Endpoints,
log_level: core.LogLevel,
sources: []const SourceDir,
filter: PathMatcher,
watch: bool,
standalone: bool,

pub const SourceDir = struct {
    category: core.TopicCategory,
    dir_path: core.FilePath, 
};

const FilterKind = core.FilterKind;
pub const FilterDir = struct {
    kind: FilterKind,
    dir_path: core.FilePath, 
};

pub fn loadFromArgs(allocator: std.mem.Allocator) !Setting {
    var arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);
    const managed_allocator = arena.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();
    
    var builder = try loadInternal(managed_allocator, &args);
    defer builder.deinit();

    return builder.build(arena);
}

pub fn deinit(self: *Setting) void {
    self.arena.deinit();
    self.arena.child_allocator.destroy(self.arena);
    self.* = undefined;
}

pub fn help(writer: anytype) !void {
    try writer.print("usage: {s} [{?s}]\n\n", .{
        @import("build_options").exe_name, ArgId.options.category_name
    });
    try core.settings.showHelp(writer, ArgId);
}

const ArgDescriptions = core.settings.DescriptionMap.initComptime(.{
    .{@tagName(.request_channel), DescriptionItem{.desc = "Comminicate Req/Rep endpoint for zmq", .value = "CHANNEL"}},
    .{@tagName(.subscribe_channel), DescriptionItem{.desc = "Comminicate Pub/Sub endpoint for zmq", .value = "CHANNEL"}},
    .{@tagName(.log_level), DescriptionItem{.desc = "Pass through log level (err / warn / info / debug / trace). default: info", .value = "LEVEL",}},
    .{@tagName(.source_dir), DescriptionItem{.desc = "Source SQL directores or files", .value = "PATH"}},
    .{@tagName(.schema_dir), DescriptionItem{.desc = "Schema SQL directores or files", .value = "PATH"}},
    .{@tagName(.include_filter), DescriptionItem{.desc = "Filter passing source/schema SQL directores or files satisfied", .value = "PATH"}},
    .{@tagName(.exclude_filter), DescriptionItem{.desc = "Filter rejecting source/schema SQL directores or files satisfied", .value = "PATH"}},
    .{@tagName(.watch), DescriptionItem{.desc = "Enter to watch-mode", .value = ""}},
});

const ArgId = enum {
    request_channel,
    subscribe_channel,
    log_level,
    source_dir,
    schema_dir,
    include_filter,
    exclude_filter,
    watch,
    standalone,

    pub const Decls: []const clap.Param(ArgId) = &.{
        .{.id = .request_channel, .names = .{.long = "request-channel"}, .takes_value = .one},
        .{.id = .subscribe_channel, .names = .{.long = "subscribe-channel"}, .takes_value = .one},
        .{.id = .log_level, .names = .{.long = "log-level"}, .takes_value = .one},
        .{.id = .source_dir, .names = .{.long = "source-dir"}, .takes_value = .many},
        .{.id = .schema_dir, .names = .{.long = "schema-dir"}, .takes_value = .many},
        .{.id = .include_filter, .names = .{.long = "include-filter"}, .takes_value = .many},
        .{.id = .exclude_filter, .names = .{.long = "exclude-filter"}, .takes_value = .many},
        .{.id = .watch, .names = .{.long = "watch"}, .takes_value = .none},
        .{.id = .standalone, .names = .{.long = "standalone"}, .takes_value = .none},
        // .{.id = ., .names = , .takes_value = },
    };
    pub usingnamespace core.settings.ArgHelp(@This(), ArgDescriptions);
    pub const options: core.settings.ArgHelpOption = .{.category_name = "Stage options"};
};

fn loadInternal(allocator: std.mem.Allocator, args_iter: *std.process.ArgIterator) !Builder {
    _ = args_iter.next();

    var diag = clap.Diagnostic{};
    var parser = clap.streaming.Clap(ArgId, std.process.ArgIterator){
        .params = ArgId.Decls,
        .iter = args_iter,
        .diagnostic = &diag,
    };

    var builder = Builder.init(allocator);

    while (true) {
        const arg_ = parser.next() catch |err| {
            try diag.report(std.io.getStdErr().writer(), err);
            return err;
        };
        const arg = arg_ orelse break;

        switch (arg.param.id) {
            .request_channel => builder.request_channel = arg.value,
            .subscribe_channel => builder.subscribe_channel = arg.value,
            .log_level => builder.log_level = arg.value,
            .source_dir => {
                if (arg.value) |v| try builder.addSourceDir(.source, v);
            },
            .schema_dir => {
                if (arg.value) |v| try builder.addSourceDir(.schema, v);
            },
            .include_filter => {
                if (arg.value) |v| try builder.addFilterDir(.include, v);
            },
            .exclude_filter => {
                if (arg.value) |v| try builder.addFilterDir(.exclude, v);
            },
            .watch => builder.watch = true,
            .standalone => builder.standalone = true,
        }
    }

    return builder;
}

const Builder = struct {
    request_channel: ?core.Symbol,
    subscribe_channel: ?core.Symbol,
    log_level: ?core.Symbol,
    sources: SourceList,
    filters: FilterList,
    watch: bool,
    standalone: bool,

    const SourceList = std.ArrayList(struct {category: core.TopicCategory, path: core.FilePath});    const FilterKind = enum {include, exclude};
    const FilterList = std.ArrayList(struct {kind: PathMatcher.FilterKind, path: core.FilePath});
    
    pub fn init(allocator: std.mem.Allocator) Builder {
        return .{
            .request_channel = null,
            .subscribe_channel = null,
            .log_level = null,
            .sources = SourceList.init(allocator),
            .filters = FilterList.init(allocator),
            .watch = false,
            .standalone = false,
        };
    }

    pub fn deinit(self: *Builder) void {
        self.sources.deinit();
        self.filters.deinit();
    }

    pub fn addSourceDir(self: *Builder, category: core.TopicCategory, path: core.FilePath) !void {
        return self.sources.append(.{.category = category, .path = path});
    }

    pub fn addFilterDir(self: *Builder, kind: PathMatcher.FilterKind, path: core.FilePath) !void {
        return self.filters.append(.{.kind = kind, .path = path});
    }

    pub fn build (self: Builder, arena: *std.heap.ArenaAllocator) !Setting {
        const allocator = arena.allocator();

        if (self.request_channel == null) {
            log.warn("Need to specify a `request-channel` arg.\n\n", .{});
            return error.SettingLoadFailed;
        }
        if (self.subscribe_channel == null) {
            log.warn("Need to specify a `subscribe-channel` arg.\n\n", .{});
            return error.SettingLoadFailed;
        }
        if (self.sources.items.len == 0) {
            log.warn("Need to specify at least one `source-dir` arg(s).\n\n", .{});
            return error.SettingLoadFailed;
        }

        const log_level = core.settings.resolveLogLevel(self.log_level) catch |err| {
            log.warn("Unresolved log level: {?s}", .{self.log_level});
            return err;
        };

        var sources = std.ArrayList(SourceDir).init(allocator);
        defer sources.deinit();

        for (self.sources.items) |item| {
            const path_abs = std.fs.cwd().realpathAlloc(allocator, item.path) catch {
                log.warn("can not resolve `{s}-dir` arg ({s}).\n\n", .{@tagName(item.category), item.path});
                return error.SettingLoadFailed;
            };

            try sources.append(.{
                .category = item.category,
                .dir_path = path_abs,
            });
        }

        var filter_builder = PathMatcher.Builder.init(allocator);
        defer filter_builder.deinit();

        for (self.filters.items) |filter| {
            const filter_u = try toUnicodeString(allocator, filter.path);
            defer allocator.free(filter_u);

            try filter_builder.addFilterDir(filter.kind, filter_u);
        }

        return .{
            .arena = arena,
            .endpoints = .{
                .req_rep = try allocator.dupe(u8, self.request_channel.?),
                .pub_sub = try allocator.dupe(u8, self.subscribe_channel.?),
            },
            .log_level = log_level,
            .sources = try sources.toOwnedSlice(),
            .filter = try filter_builder.build(),
            .watch = self.watch,
            .standalone = self.standalone,
        };
    }
};

pub const toUnicodeString = @import("./PathMatcher.zig").toUnicodeString;