const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const Symbol = core.types.Symbol;
const FilePath = core.types.FilePath;

const ArgScanner = core.settings.types.ArgScanner;
const ArgParserPair = core.settings.types.ArgParserPair;

const GuestBaseConfigArgId = core.configs.guests.GuestBaseConfig.ArgId(.{});
const GuestWatchArgId = core.configs.guests.GuestWatch.ArgId(.{});

const toUnicodeString = @import("./PathMatcher.zig").toUnicodeString;

const ArgHelp = @import("./help/ArgHelp.zig");
const PathMatcher = @import("./PathMatcher.zig").PathMatcher(u21);
const Setting = @This();

log_level: core.events.LogLevel,
log_style: core.Logger.LogStyle,
no_color: bool,
endpoints: core.types.Endpoints,
sources: []const SourceDir,
filter: PathMatcher,
default_dialect: core.types.Symbol,
watch: bool,

pub const SourceDir = struct {
    category: core.events.TopicCategory,
    dir_path: core.types.FilePath, 
};

const FilterKind = core.types.FilterKind;
pub const FilterDir = struct {
    kind: FilterKind,
    dir_path: core.types.FilePath, 
};

pub fn loadFromArgs(allocator: std.mem.Allocator, args: std.process.Args) core.settings.types.LoadResult(Setting, *const ArgHelp.Config) {
    var args_iter = try args.iterateAllocator(allocator);
    defer args_iter.deinit();

    _ = args_iter.next();
    
    var scanner: ArgScanner(std.process.Args.Iterator) = .init(&args_iter);
    var builder = Builder(std.process.Args.Iterator).fromArgs(allocator, &scanner, .stderr) catch return .{ .help = &ArgHelp.toplevel };
    defer builder.deinit(allocator);
    const setting = builder.build(allocator) catch return .{ .help = &ArgHelp.toplevel };

    return .{ .success = setting };
}

pub fn deinit(self: *Setting, allocator: std.mem.Allocator) void {
    allocator.free(self.endpoints.req_rep);
    allocator.free(self.endpoints.pub_sub);
    allocator.free(self.endpoints.push_pull);
    allocator.free(self.default_dialect);

    self.filter.deinit();

    for (self.sources) |item| {
        allocator.free(item.dir_path);
    }
    allocator.free(self.sources);
}

fn Builder(comptime ArgIterator: type) type {
    return struct {
        builder_log_style: core.Logger.LogStyle,
        reqrep_channel: ?Symbol,
        pubsub_channel: ?Symbol,
        pushpull_channel: ?Symbol,
        log_level: ?Symbol,
        log_style: ?Symbol,
        no_color: bool,
        sources: SourceList,
        filters: FilterList,
        default_dialect: ?Symbol,
        watch: bool,

        const SourceList = std.ArrayListUnmanaged(struct {category: core.events.TopicCategory, path: FilePath});
        const FilterKind = enum {include, exclude};
        const FilterList = std.ArrayListUnmanaged(struct {kind: PathMatcher.FilterKind, path: FilePath});
        
        pub fn fromArgs(allocator: std.mem.Allocator, scanner: *ArgScanner(ArgIterator), log_style: core.Logger.LogStyle) !Builder(ArgIterator) {
            var diag: clap.Diagnostic = .{};
            var parsers = ArgParserPair(GuestBaseConfigArgId, GuestWatchArgId, ArgIterator).init(scanner, &diag);

            var builder: Builder(ArgIterator) = .{
                .builder_log_style = log_style,
                .reqrep_channel = null,
                .pubsub_channel = null,
                .pushpull_channel = null,
                .log_level = null,
                .log_style = null,
                .no_color = false,
                .sources = .empty,
                .filters = .empty,
                .default_dialect = null,
                .watch = false,
            };

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
                        builder.handleBaseArg(arg);
                    },
                    .extra => |arg| {
                        try builder.handleExtraArg(allocator, arg);
                    }
                }
            }

            return builder;
        }

        pub fn deinit(self: *Builder(ArgIterator), allocator: std.mem.Allocator) void {
            self.sources.deinit(allocator);
            self.filters.deinit(allocator);
        }

        fn handleBaseArg(self: *Builder(ArgIterator), arg: clap.streaming.Arg(GuestBaseConfigArgId)) void {
            switch (arg.param.id) {
                .req_rep => self.reqrep_channel = arg.value,
                .pub_sub => self.pubsub_channel = arg.value,
                .push_pull => self.pushpull_channel = arg.value,
                .log_level => self.log_level = arg.value,
                .log_style => self.log_style = arg.value,
                .no_color => self.no_color = true,
            }
        }

        fn handleExtraArg(self: *Builder(ArgIterator), allocator: std.mem.Allocator, arg: clap.streaming.Arg(GuestWatchArgId)) !void {
            switch (arg.param.id) {
                .source_dir_set => {
                    if (arg.value) |v| try self.addSourceDir(allocator, .source, v);
                },
                .schema_dir_set => {
                    if (arg.value) |v| try self.addSourceDir(allocator, .schema, v);
                },
                .include_filter_set => {
                    if (arg.value) |v| try self.addFilterDir(allocator, .include, v);
                },
                .exclude_filter_set => {
                    if (arg.value) |v| try self.addFilterDir(allocator, .exclude, v);
                },
                .watch => self.watch = true,
            }
        }

        pub fn addSourceDir(self: *Builder(ArgIterator), allocator: std.mem.Allocator, category: core.events.TopicCategory, path: FilePath) !void {
            return self.sources.append(allocator, .{.category = category, .path = path});
        }

        pub fn addFilterDir(self: *Builder(ArgIterator), allocator: std.mem.Allocator, kind: PathMatcher.FilterKind, path: FilePath) !void {
            return self.filters.append(allocator, .{.kind = kind, .path = path});
        }

        pub fn build (self: Builder(ArgIterator), allocator: std.mem.Allocator) !Setting {
            var has_err = false;

            if (self.reqrep_channel == null) {
                has_err = true;
                if (self.builder_log_style == .stderr) {
                    std.log.warn("Need to specify a `request-channel` arg.", .{});
                }
            }
            if (self.pubsub_channel == null) {
                has_err = true;
                if (self.builder_log_style == .stderr) {
                    std.log.warn("Need to specify a `subscribe-channel` arg.", .{});
                }
            }
            if (self.pushpull_channel == null) {
                has_err = true;
                if (self.builder_log_style == .stderr) {
                    std.log.warn("Need to specify a `push-channel` arg.", .{});
                }
            }

            const log_level = core.events.LogLevel.resolveLogLevel(self.log_level) orelse log_level: {
                has_err = true;
                if (self.builder_log_style == .stderr) {
                    std.log.warn("Unresolved log level: {?s}", .{self.log_level});
                }
                break:log_level null;
            };

            const log_style = core.settings.supports.resolveGuestLogStyle(self.log_style) orelse log_style: {
                has_err = true;
                if (self.builder_log_style == .stderr) {
                    std.log.warn("Unresolved log style: {?s}", .{self.log_style});
                }                
                break:log_style null;
            };

            const default_dialect = try allocator.dupe(u8, self.default_dialect orelse "duckdb");

            var sources: std.ArrayListUnmanaged(SourceDir) = .empty;
            defer sources.deinit(allocator);

            if (self.sources.items.len == 0) {
                has_err = true;
                std.log.warn("Need to specify at least one `source-dir` arg(s).", .{});
            }
            else {
                for (self.sources.items) |item| {
                try sources.append(allocator, .{
                    .category = item.category,
                    .dir_path = try allocator.dupe(u8, item.path),
                });
                }
            }

            var filter_builder: PathMatcher.Builder = .init;
            defer filter_builder.deinit(allocator);

            for (self.filters.items) |filter| {
                const filter_u = try toUnicodeString(allocator, filter.path);
                defer allocator.free(filter_u);

                try filter_builder.addFilterDir(allocator, filter.kind, filter_u);
            }

            if (has_err) {
                return error.SettingLoadFailed;
            }

            return .{
                .endpoints = .{
                    .req_rep = try allocator.dupe(u8, self.reqrep_channel.?),
                    .pub_sub = try allocator.dupe(u8, self.pubsub_channel.?),
                    .push_pull = try allocator.dupe(u8, self.pushpull_channel.?),
                },
                .log_level = log_level.?,
                .log_style = log_style.?,
                .no_color = self.no_color,
                .sources = try sources.toOwnedSlice(allocator),
                .filter = try filter_builder.build(allocator),
                .default_dialect = default_dialect,
                .watch = self.watch,
            };
        }
    };
}
