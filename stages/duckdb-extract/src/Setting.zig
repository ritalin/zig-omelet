const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const log = core.Logger.SystemDirect(@import("build_options").app_context);

const Setting = @This();

arena: *std.heap.ArenaAllocator,
endpoints: core.Endpoints,
log_level: core.LogLevel,
standalone: bool,
schema_dir_set: []const core.FilePath,

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
        @import("build_options").exe_name, ArgId.options.category_name
    });
    try core.settings.showHelp(writer, ArgId);
}

const ArgDescriptions = core.settings.DescriptionMap.initComptime(.{
    .{@tagName(.request_channel), .{.desc = "Comminicate Req/Rep endpoint for zmq", .value = "CHANNEL", .required = true,}},
    .{@tagName(.subscribe_channel), .{.desc = "Comminicate Pub/Sub endpoint for zmq", .value = "CHANNEL", .required = true,}},
    .{@tagName(.log_level), .{.desc = "Pass through log level (err / warn / info / debug / trace). default: info", .value = "LEVEL",}},
    .{@tagName(.schema_dir), .{.desc = "Schema definition directory", .value = "PATH",}},
});

const ArgId = enum {
    request_channel,
    subscribe_channel,
    log_level,
    standalone,
    schema_dir,

    pub const Decls: []const clap.Param(ArgId) = &.{
        .{.id = .request_channel, .names = .{.long = "request-channel"}, .takes_value = .one},
        .{.id = .subscribe_channel, .names = .{.long = "subscribe-channel"}, .takes_value = .one},
        .{.id = .log_level, .names = .{.long = "log-level"}, .takes_value = .one},
        .{.id = .standalone, .names = .{.long = "standalone"}, .takes_value = .none},
        .{.id = .schema_dir, .names = .{.long = "schema-dir"}, .takes_value = .one},
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
            .standalone => builder.standalone = true,
            .schema_dir => try builder.schema_dir_set.append(arg.value),
        }
    }

    return builder;
}

const Builder = struct {
    request_channel: ?core.Symbol,
    subscribe_channel: ?core.Symbol,
    log_level: ?core.Symbol,
    standalone: bool,
    schema_dir_set: std.ArrayList(?core.FilePath),

    pub fn init(allocator: std.mem.Allocator) Builder {
        return .{
            .request_channel = null,
            .subscribe_channel = null,
            .log_level = null,
            .standalone = false,
            .schema_dir_set = std.ArrayList(?core.FilePath).init(allocator),
        };
    }

    pub fn deinit(self: *Builder) void {
        self.schema_dir_set.deinit();
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

        const log_level = core.settings.resolveLogLevel(self.log_level) catch |err| {
            log.warn("Unresolved log level: {?s}", .{self.log_level});
            return err;
        };
        
        var schemas = std.ArrayList(core.FilePath).init(allocator);
        defer schemas.deinit();
        for (self.schema_dir_set.items) |path_| {
            if (path_) |path| {
                try schemas.append(try allocator.dupe(u8, path));
            }
        }

        return .{
            .arena = arena,
            .endpoints = .{
                .req_rep = try allocator.dupe(u8, self.request_channel.?),
                .pub_sub = try allocator.dupe(u8, self.subscribe_channel.?),
            },
            .log_level = log_level,
            .standalone = self.standalone,
            .schema_dir_set = try schemas.toOwnedSlice(),
        };
    }
};