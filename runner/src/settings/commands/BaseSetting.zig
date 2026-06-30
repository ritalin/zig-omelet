const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const Endpoint = core.configs.Endpoint;
const ArgScanner = core.settings.types.ArgScanner;
const ArgParserPair = core.settings.types.ArgParserPair;

const default_init_scope = @import("build_options").default_init_scope;
const Defaults = @import("../default_args.zig").Defaults(std.meta.FieldEnum(BaseArgId));
const BaseSetting = @This();

log_level: core.events.LogLevel,
log_quiet: bool,
no_color: bool,
interactive: bool,
endpoints: core.types.Endpoints,
ipc_config: Endpoint.Config,
scope: core.types.Symbol,
config_scope: core.types.Symbol,

pub fn deinit(self: *BaseSetting, io: std.Io, allocator: std.mem.Allocator) void {
    Endpoint.releaseIpcStorage(io, &self.ipc_config);
    Endpoint.releaseIpcConfig(allocator, &self.ipc_config);

    allocator.free(self.endpoints.req_rep);
    allocator.free(self.endpoints.pub_sub);
    allocator.free(self.endpoints.push_pull);
    if (self.endpoints.worker) |worker| allocator.free(worker);
    allocator.free(self.scope);
}

pub fn ArgId(comptime descriptions: core.settings.types.DescriptionMap) type {
    return enum {
        req_rep_channel,
        pub_sub_channel,
        push_pull_channel,
        log_level,
        log_quiet,
        no_color,
        interactive,
        use_scope,
        help,

        pub const Decls: []const clap.Param(@This()) = &.{
            .{.id = .req_rep_channel, .names = .{.long = "reqrep-channel"}, .takes_value = .one},
            .{.id = .pub_sub_channel, .names = .{.long = "pubsub-channel"}, .takes_value = .one},
            .{.id = .push_pull_channel, .names = .{.long = "pushpull-channel"}, .takes_value = .one},
            .{.id = .log_level, .names = .{.long = "log-level"}, .takes_value = .one},
            .{.id = .log_quiet, .names = .{.long = "quiet", .short = 'q'}, .takes_value = .none},
            .{.id = .no_color, .names = .{.long = "no-color"}, .takes_value = .none},
            .{.id = .use_scope, .names = .{.long = "use-scope"}, .takes_value = .one},
            .{.id = .interactive, .names = .{.long = "watch"}, .takes_value = .none},
            .{.id = .help, .names = .{.long = "help", .short = 'h'}, .takes_value = .none},
        };
 
        const arg_view = core.settings.types.ArgHelp(@This(), descriptions);
        pub const description = arg_view.description;
        pub const value = arg_view.value;
    };
}

const BaseArgId = ArgId(.{});

const PositionalArg = struct {
    pub const Id = enum { 
        p0,

        pub const Decls: []const clap.Param(PositionalArg.Id) = &.{
            .{ .id = .p0, .takes_value = .one },
        };
    };
};

fn resolveSubcommand(arg: clap.streaming.Arg(PositionalArg.Id)) !SubcommandArgId {
    if (arg.value) |value| {
        return (clap.parsers.enumeration(SubcommandArgId))(value) catch error.InvalidCommand;
    }

    return error.InvalidCommand;
}

const Subcommand = @import("./Subcommand.zig");
const SubcommandArgId = Subcommand.ArgId(.{});

pub fn Builder(comptime ArgIterator: type) type {
    return struct {
        build_log_style: core.Logger.LogStyle,
        log_level: ?core.events.LogLevel = null,
        log_quiet: ?bool = null,
        no_color: ?bool = null,
        interactive: ?bool = null,
        req_rep_channel: ?core.types.Symbol = null,
        pub_sub_channel: ?core.types.Symbol = null,
        push_pull_channel: ?core.types.Symbol = null,
        scope: ?core.types.Symbol = null,

        pub fn fromArgs(allocator: std.mem.Allocator, scanner: *ArgScanner(ArgIterator), log_style: core.Logger.LogStyle) !struct{ builder: Builder(ArgIterator), command: SubcommandArgId } {
            var diag: clap.Diagnostic = .{}; 
            var parsers = ArgParserPair(BaseArgId, PositionalArg.Id, ArgIterator).init(scanner, &diag);

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
                        const command = try resolveSubcommand(arg);
                        return .{ .builder = builder, .command = command };
                    }
                }
            }

            if (log_style == .stderr) {
                std.log.warn("Missing command", .{});
            }

            return error.MissngCommand;
        }

        pub fn handleArg(self: *Builder(ArgIterator), allocator: std.mem.Allocator, arg: clap.streaming.Arg(BaseArgId)) !void {
            _ = allocator;

            switch (arg.param.id) {
                .help => return error.ShowHelp,
                .req_rep_channel => self.req_rep_channel = arg.value,
                .pub_sub_channel => self.pub_sub_channel = arg.value,
                .push_pull_channel => self.push_pull_channel = arg.value,
                .log_level => {
                    self.log_level = core.Logger.resolveLogLevel(arg.value) catch null;
                },
                .log_quiet => self.log_quiet = true,
                .no_color => self.no_color = true,
                .interactive => self.interactive = true,
                .use_scope => {
                    if (arg.value) |v| self.scope = v;
                },
            }
        }

        fn applyDefaults(ptr: *anyopaque, allocator: std.mem.Allocator, defaults: *Defaults) !void {
            var self: *Builder(ArgIterator) = @ptrCast(@alignCast(ptr));
            var iter = defaults.iterator();

            while (iter.next()) |entry| {
                switch (entry.key) {
                    .req_rep_channel => if (entry.value.tag() == .values) {
                        self.req_rep_channel = try allocator.dupe(u8, entry.value.values[0]);
                    },
                    .pub_sub_channel => if (entry.value.tag() == .values) {
                        self.pub_sub_channel = try allocator.dupe(u8, entry.value.values[0]);
                    },
                    .push_pull_channel => if (entry.value.tag() == .values) {
                        self.push_pull_channel = try allocator.dupe(u8, entry.value.values[0]);
                    },
                    .log_level => if ((entry.value.tag() == .values)) {
                        self.log_level = std.meta.stringToEnum(core.events.LogLevel, entry.value.values[0]);
                    },
                    .log_quiet => if ((entry.value.tag() == .enabled)) {
                        self.log_quiet = entry.value.enabled;
                    },
                    .no_color => if ((entry.value.tag() == .enabled)) {
                        self.no_color = entry.value.enabled;
                    },
                    .interactive => if ((entry.value.tag() == .enabled)) {
                        self.interactive = entry.value.enabled;
                    },
                    .use_scope => if ((entry.value.tag() == .values)) {
                        self.scope = try allocator.dupe(u8, entry.value.values[0]);
                    },
                    .help => {},
                }
            }
        }

        pub fn build (self: *Builder(ArgIterator), io: std.Io, allocator: std.mem.Allocator, default_scope: core.types.Symbol, options: core.configs.supports.FileResolveOptions) !BaseSetting {
            var arena = std.heap.ArenaAllocator.init(allocator);
            defer arena.deinit();
            
            var builder_default: Builder(ArgIterator) = .{ .build_log_style = self.build_log_style };

            if (try core.configs.supports.resolveFileCandidate(io, arena.allocator(), options)) |file| {
                defer file.close(io);

                const callback: Defaults.ApplyDefaultHandler = .{ .ptr = &builder_default, .handler = Builder(ArgIterator).applyDefaults };
                try Defaults.loadFromFile(io, arena.allocator(), file, self.build_log_style, callback);
            }

            // default IPC path
            try Endpoint.createIpcStorage(io, &Endpoint.Config.default);

            const ipc_config = try Endpoint.renewIpcConfig(io, allocator, &Endpoint.Config.default);

            if ((self.req_rep_channel == null) or (self.pub_sub_channel == null) or (self.push_pull_channel == null)) {
                try Endpoint.createIpcStorage(io, &ipc_config);
            }

            var ipc_endpoints = try Endpoint.runtimeIpc(allocator, ipc_config);
            defer Endpoint.releaseRuntimeIpc(allocator, &ipc_endpoints);
            
            const req_rep_channel = try allocator.dupe(u8, self.req_rep_channel orelse builder_default.req_rep_channel orelse ipc_endpoints.req_rep);
            const pub_sub_channel = try allocator.dupe(u8, self.pub_sub_channel orelse builder_default.pub_sub_channel orelse ipc_endpoints.pub_sub);
            const push_pull_channel = try allocator.dupe(u8, self.push_pull_channel orelse builder_default.push_pull_channel orelse ipc_endpoints.push_pull);
            const scope = try allocator.dupe(u8, self.scope orelse builder_default.scope orelse default_scope);

            return .{
                .log_level = self.log_level orelse builder_default.log_level orelse core.Logger.default,
                .log_quiet = self.log_quiet orelse builder_default.log_quiet orelse false,
                .no_color = self.no_color orelse builder_default.no_color orelse false,
                .interactive = self.interactive orelse builder_default.interactive orelse false,
                .endpoints = .{
                    .req_rep = req_rep_channel,
                    .pub_sub = pub_sub_channel,
                    .push_pull = push_pull_channel,
                },
                .ipc_config = ipc_config,
                .scope = scope,
                .config_scope = default_init_scope, // TODO: wants to pass from CLI arg
            };
        }
    };
}

test "Base setting test" {
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

        var tmp_dir = std.testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        var args: std.ArrayListUnmanaged(core.types.Symbol) = .empty;
        try args.appendSlice(allocator, &.{
            "--reqrep-channel", "inproc://req-rep",
            "--pubsub-channel", "inproc://pub-sub",
            "--pushpull-channel", "inproc://push-pull",
            "--log-level", "trace",
            "--quiet",
            "--no-color", 
            "--watch",
            "--use-scope", "test",
            "generate"
        });        

        const defaults_source = ".{}";

        const file_candidates: ConfigFileCandidates = .{ .current_dir = try tmp_dir.dir.realPathFileAlloc(io, ".", allocator), };
        const options: FileResolveOptions = .{
            .command = "generate", .scope = "default", .category = .defaults, .root = file_candidates
        };

        try writeAssetFile(&tmp_dir, options, defaults_source);

        var iter: TetsArgsIterator = .{.args = args.items};
        var scanner = ArgScanner(TetsArgsIterator).init(&iter);

        var res = try Builder(TetsArgsIterator).fromArgs(allocator, &scanner, .discard);
        const setting: BaseSetting = try res.builder.build(io, allocator, "default", options);

        try std.testing.expectEqualStrings("inproc://req-rep", setting.endpoints.req_rep);
        try std.testing.expectEqualStrings("inproc://pub-sub", setting.endpoints.pub_sub);
        try std.testing.expectEqualStrings("inproc://push-pull", setting.endpoints.push_pull);
        try std.testing.expectEqual(.trace, setting.log_level);
        try std.testing.expectEqual(true, setting.log_quiet);
        try std.testing.expectEqual(true, setting.no_color);
        try std.testing.expectEqual(true, setting.interactive);
        try std.testing.expectEqualStrings("test", setting.scope);
    }

    test "All default args" {
        const io = std.testing.io;
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        var tmp_dir = std.testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        const defaults_source = 
            \\.{
            \\    .req_rep_channel = .{ .values = .{ "ipc:///path/to/req-rep" } },
            \\    .pub_sub_channel = .{ .values = .{ "ipc:///path/to/pub-sub" } },
            \\    .push_pull_channel = .{ .values = .{ "ipc:///path/to/push-pull" } },
            \\    .log_level = .{ .values = .{"debug"} },
            \\    .log_quiet = .{ .enabled = true },
            \\    .no_color = .{ .enabled = true },
            \\    .interactive = .{ .enabled = true },
            \\    .use_scope = .{ .values = .{ "demo" } },
            \\}
        ;

        const file_candidates: ConfigFileCandidates = .{ .current_dir = try tmp_dir.dir.realPathFileAlloc(io, ".", allocator), };
        const options: FileResolveOptions = .{
            .command = "generate", .scope = "default", .category = .defaults, .root = file_candidates
        };

        try writeAssetFile(&tmp_dir, options, defaults_source);

        var iter: TetsArgsIterator = .{.args = &.{ "generate" }};
        var scanner = ArgScanner(TetsArgsIterator).init(&iter);

        var res = try Builder(TetsArgsIterator).fromArgs(allocator, &scanner, .discard);
        const setting: BaseSetting = try res.builder.build(io, allocator, "default", options);

        try std.testing.expectEqualStrings("ipc:///path/to/req-rep", setting.endpoints.req_rep);
        try std.testing.expectEqualStrings("ipc:///path/to/pub-sub", setting.endpoints.pub_sub);
        try std.testing.expectEqualStrings("ipc:///path/to/push-pull", setting.endpoints.push_pull);
        try std.testing.expectEqual(.debug, setting.log_level);
        try std.testing.expectEqual(true, setting.log_quiet);
        try std.testing.expectEqual(true, setting.no_color);
        try std.testing.expectEqual(true, setting.interactive);
        try std.testing.expectEqualStrings("demo", setting.scope);
    }

    test "Explict + default args" {
        const io = std.testing.io;
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        var tmp_dir = std.testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        var args: std.ArrayListUnmanaged(core.types.Symbol) = .empty;
        try args.appendSlice(allocator, &.{
            "--pushpull-channel", "inproc://explicit-push-pull",
            "--log-level", "trace",
            "--use-scope", "test",
            "generate"
        });        

        const defaults_source = 
            \\.{
            \\    .req_rep_channel = .{ .values = .{ "inproc://default-req-rep" } },
            \\    .pub_sub_channel = .{ .values = .{ "inproc://default-pub-sub" } },
            \\    .log_level = .{ .values = .{"debug"} },
            \\    .log_quiet = .{ .enabled = true },
            \\    .no_color = .{ .enabled = true },
            \\    .interactive = .{ .enabled = true },
            \\    .use_scope = .{ .values = .{ "demo" } },
            \\}
        ;

        const file_candidates: ConfigFileCandidates = .{ .current_dir = try tmp_dir.dir.realPathFileAlloc(io, ".", allocator), };
        const options: FileResolveOptions = .{
            .command = "generate", .scope = "default", .category = .defaults, .root = file_candidates
        };

        try writeAssetFile(&tmp_dir, options, defaults_source);

        var iter: TetsArgsIterator = .{.args = args.items};
        var scanner = ArgScanner(TetsArgsIterator).init(&iter);

        var res = try Builder(TetsArgsIterator).fromArgs(allocator, &scanner, .discard);
        const setting: BaseSetting = try res.builder.build(io, allocator, "default", options);

        try std.testing.expectEqualStrings("inproc://default-req-rep", setting.endpoints.req_rep);
        try std.testing.expectEqualStrings("inproc://default-pub-sub", setting.endpoints.pub_sub);
        try std.testing.expectEqualStrings("inproc://explicit-push-pull", setting.endpoints.push_pull);
        try std.testing.expectEqual(.trace, setting.log_level);
        try std.testing.expectEqual(true, setting.log_quiet);
        try std.testing.expectEqual(true, setting.no_color);
        try std.testing.expectEqual(true, setting.interactive);
        try std.testing.expectEqualStrings("test", setting.scope);
    }
};