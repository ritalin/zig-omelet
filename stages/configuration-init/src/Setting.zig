const std = @import("std");
const clap = @import("clap");
const core = @import("core");
const log = core.Logger.SystemDirect(@import("build_options").app_context);

const DescriptionItem = core.settings.DescriptionItem;

const Setting = @This();

arena: *std.heap.ArenaAllocator,
endpoints: core.Endpoints,
log_level: core.LogLevel,
source_dir_path: core.FilePath,
output_dir_path: core.FilePath,
category: core.ConfigCategory,
command: core.SubcommandArgId,
scope_set: []const core.Symbol,
from_scope: ?core.Symbol,
standalone: bool,

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
}

pub fn help(writer: anytype) !void {
    try writer.print("usage: {s} [{?s}]\n\n", .{
        @import("build_options").exe_name, 
        ArgId.options.category_name
    });
    try core.settings.showHelp(writer, ArgId);
}

const ArgDescriptions = core.settings.DescriptionMap.initComptime(.{
    .{@tagName(.request_channel), DescriptionItem{.desc = "Comminicate Req/Rep endpoint for zmq", .value = "CHANNEL"}},
    .{@tagName(.subscribe_channel), DescriptionItem{.desc = "Comminicate Pub/Sub endpoint for zmq", .value = "CHANNEL"}},
    .{@tagName(.log_level), DescriptionItem{.desc = "Pass through log level (err / warn / info / debug / trace). default: info", .value = "LEVEL",}},
});

const ArgId = enum {
    request_channel,
    subscribe_channel,
    log_level,
    source_dir,
    output_dir,
    category,
    command,
    scope,
    from_scope,
    standalone,

    pub const Decls: []const clap.Param(ArgId) = &.{
        .{.id = .request_channel, .names = .{.long = "request-channel"}, .takes_value = .one},
        .{.id = .subscribe_channel, .names = .{.long = "subscribe-channel"}, .takes_value = .one},
        .{.id = .log_level, .names = .{.long = "log-level"}, .takes_value = .one},
        .{.id = .source_dir, .names = .{.long = "source-dir", .short = 'i'}, .takes_value = .one},
        .{.id = .output_dir, .names = .{.long = "output-dir", .short = 'o'}, .takes_value = .one},
        .{.id = .category, .names = .{.long = "category"}, .takes_value = .one},
        .{.id = .command, .names = .{.long = "command"}, .takes_value = .one},
        .{.id = .scope, .names = .{.long = "scope"}, .takes_value = .many},
        .{.id = .from_scope, .names = .{.long = "from-scope"}, .takes_value = .one},
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
            .source_dir => builder.source_dir_path = arg.value,
            .output_dir => builder.output_dir_path = arg.value,
            .category => builder.category = arg.value,
            .command => builder.command = arg.value,
            .scope => {
                if (arg.value) |v| try builder.scope_set.append(v);
            },
            .from_scope => builder.from_scope = arg.value,
            .standalone => builder.standalone = true,
        }
    }

    return builder;
}

const Builder = struct {
    request_channel: ?core.Symbol = null,
    subscribe_channel: ?core.Symbol = null,
    log_level: ?core.Symbol = null,
    source_dir_path: ?core.FilePath = null,
    output_dir_path: ?core.FilePath = null,
    standalone: bool = false,
    category: ?core.Symbol = null,
    command: ?core.Symbol = null,
    scope_set: std.ArrayList(core.Symbol),
    from_scope: ?core.Symbol = null,

    pub fn init(allocator: std.mem.Allocator) Builder {
        return .{
            .scope_set = std.ArrayList(core.Symbol).init(allocator),
        };
    }

    pub fn deinit(self: *Builder) void {
        self.scope_set.deinit();
    }

    pub fn build(self: *Builder, arena: *std.heap.ArenaAllocator) !Setting {
        const allocator = arena.allocator();

        if (self.request_channel == null) {
            log.warn("Need to specify a `request-channel` arg.\n\n", .{});
            return error.SettingLoadFailed;
        }
        if (self.subscribe_channel == null) {
            log.warn("Need to specify a `subscribe-channel` arg.\n\n", .{});
            return error.SettingLoadFailed;
        }
        if (self.source_dir_path == null) {
            log.warn("Need to specify a `source-dir` arg.\n\n", .{});
            return error.SettingLoadFailed;
        }
        if (self.output_dir_path == null) {
            log.warn("Need to specify a `output-dir` arg.\n\n", .{});
            return error.SettingLoadFailed;
        }
        if (self.category == null) {
            log.warn("Need to specify a `category` arg.\n\n", .{});
            return error.SettingLoadFailed;
        }
        if (self.command == null) {
            log.warn("Need to specify a `subcommand` arg.\n\n", .{});
            return error.SettingLoadFailed;
        }
        if (self.scope_set.items.len == 0) {
            log.warn("Need to specify a `scope` arg.\n\n", .{});
            return error.SettingLoadFailed;
        }

        const log_level = core.settings.resolveLogLevel(self.log_level) catch |err| {
            log.warn("Unresolved log level: {?s}", .{self.log_level});
            return err;
        };
        const category = std.meta.stringToEnum(core.ConfigCategory, self.category.?) orelse {
            log.warn("Unresolved `category`: `{?s}`.\n\n", .{self.category});
            return error.SettingLoadFailed;
        };
        const subcommand = std.meta.stringToEnum(core.SubcommandArgId, self.command.?) orelse {
            log.warn("Unresolved `subcommand`: `{?s}`.\n\n", .{self.command});
            return error.SettingLoadFailed;
        };

        var scopes = std.ArrayList(core.Symbol).init(allocator);
        defer scopes.deinit();
        
        for (self.scope_set.items) |scope| {
            try scopes.append(try allocator.dupe(u8, scope));
        }

        const from_scope = scope: {
            if (self.from_scope) |scope| {
                break:scope try allocator.dupe(u8, scope);
            }
            else {
                break:scope null;
            }
        };

        return .{
            .arena = arena,
            .endpoints = .{
                .req_rep = try allocator.dupe(u8, self.request_channel.?),
                .pub_sub = try allocator.dupe(u8, self.subscribe_channel.?),
            },
            .log_level = log_level,
            .source_dir_path = try allocator.dupe(u8, self.source_dir_path.?),
            .output_dir_path = try allocator.dupe(u8, self.output_dir_path.?),
            .category = category,
            .command = subcommand,
            .scope_set = try scopes.toOwnedSlice(),
            .from_scope = from_scope,
            .standalone = self.standalone,
        };
    }
};