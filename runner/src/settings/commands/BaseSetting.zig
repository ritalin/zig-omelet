const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const Endpoint = core.configs.Endpoint;

const Defaults = @import("../default_args.zig").Defaults(std.meta.FieldEnum(BaseArgId));
const Self = @This();

log_level: core.events.LogLevel,
log_quiet: bool,
no_color: bool,
endpoints: core.types.Endpoints,
ipc_config: Endpoint.Config,
scope: core.types.Symbol,

pub fn deinit(self: *Self, io: std.Io, allocator: std.mem.Allocator) void {
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
            .{.id = .help, .names = .{.long = "help", .short = 'h'}, .takes_value = .none},
        };
 
        const arg_view = core.settings.types.ArgHelp(@This(), descriptions);
        pub const description = arg_view.description;
        pub const value = arg_view.value;
    };
}

const BaseArgId = ArgId(.{});

const Subcommand = @import("./Subcommand.zig");
const SubcommandArgid = Subcommand.Setting.ArgId(.{});

pub const Builder = struct {
    allocator: std.mem.Allocator, 
    log_level: ?core.events.LogLevel,
    log_quiet: bool,
    no_color: bool,
    req_rep_channel: ?core.types.Symbol,
    pub_sub_channel: ?core.types.Symbol,
    push_pull_channel: ?core.types.Symbol,
    scope: core.types.Symbol,

    pub fn fromArgs(allocator: std.mem.Allocator, iter: *std.process.Args.Iterator) !struct{ builder: Builder, command: SubcommandArgid } {
        const params = BaseArgId.Decls;
        var diag: clap.Diagnostic = .{}; 

        var parser = clap.streaming.Clap(BaseArgId, std.process.Args.Iterator){
            .params = params,
            .iter = iter,
            .diagnostic = &diag,        
        };

        var builder: Builder = .{
            .allocator = allocator,
            .log_level = null,
            .log_quiet = false,
            .no_color = false,
            .req_rep_channel = null,
            .pub_sub_channel = null,
            .push_pull_channel = null,
            .scope = "default",
        };
        
        while (true) {
            const next_arg = parser.next() catch |err| switch (err) {
                error.InvalidArgument => {
                    const command = SubcommandArgid.fromString(diag.arg) orelse return error.ShowHelp;
                    return .{ .builder = builder, .command = command };
                },
                else => {
                    try core.log_supports.reportClapError(&diag, err);
                    return error.SettingLoadFailed;
                }
            };
            if (next_arg == null) {
                return error.SettingLoadFailed;
            }
            const arg = next_arg.?;

            switch (arg.param.id) {
                .help => return error.ShowHelp,
                .req_rep_channel => builder.req_rep_channel = arg.value,
                .pub_sub_channel => builder.pub_sub_channel = arg.value,
                .push_pull_channel => builder.push_pull_channel = arg.value,
                .log_level => {
                    builder.log_level = core.Logger.resolveLogLevel(arg.value) catch null;
                },
                .log_quiet => builder.log_quiet = true,
                .no_color => builder.no_color = true,
                .use_scope => {
                    if (arg.value) |v| builder.scope = v;
                },
            }
        }
    }

    fn applyDefaults(ptr: *anyopaque, defaults: *Defaults) !void {
        var self: *Builder = @ptrCast(@alignCast(ptr));
        var iter = defaults.iterator();

        while (iter.next()) |entry| {
            switch (entry.key) {
                .req_rep_channel => if (entry.value.tag() == .values) {
                    self.req_rep_channel = entry.value.values[0];
                },
                .pub_sub_channel => if (entry.value.tag() == .values) {
                    self.pub_sub_channel = entry.value.values[0];
                },
                .push_pull_channel => if (entry.value.tag() == .values) {
                    self.push_pull_channel = entry.value.values[0];
                },
                .log_level => if (entry.value.tag() == .values) {
                    self.log_level = std.meta.stringToEnum(core.events.LogLevel, entry.value.values[0]);
                },
                .log_quiet => if (entry.value.tag() == .enabled) {
                    self.log_quiet = entry.value.enabled;
                },
                .no_color => if (entry.value.tag() == .enabled) {
                    self.no_color = entry.value.enabled;
                },
                .use_scope => if (entry.value.tag() == .values) {
                    self.scope = entry.value.values[0];
                },
                .help => {},
            }
        }
    }

    pub fn build (self: *Builder, io: std.Io, allocator: std.mem.Allocator, options: core.configs.supports.FileResolveOptions) !Self {
        if (try core.configs.supports.resolveFileCandidate(io, self.allocator, options)) |file| {
            defer file.close(io);

            const callback: Defaults.ApplyDefaultHandler = .{ .ptr = self, .handler = Builder.applyDefaults };
            try Defaults.loadFromFile(io, self.allocator, file, callback);
        }

        // default IPC path
        try Endpoint.createIpcStorage(io, &Endpoint.Config.default);

        const ipc_config = try Endpoint.renewIpcConfig(io, allocator, &Endpoint.Config.default);

        if ((self.req_rep_channel == null) or (self.pub_sub_channel == null) or (self.push_pull_channel == null)) {
            try Endpoint.createIpcStorage(io, &ipc_config);
        }

        var ipc_endpoints = try Endpoint.runtimeIpc(allocator, ipc_config);
        defer Endpoint.releaseRuntimeIpc(allocator, &ipc_endpoints);
        
        const req_rep_channel = try allocator.dupe(u8, self.req_rep_channel orelse ipc_endpoints.req_rep);
        const pub_sub_channel = try allocator.dupe(u8, self.pub_sub_channel orelse ipc_endpoints.pub_sub);
        const push_pull_channel = try allocator.dupe(u8, self.push_pull_channel orelse ipc_endpoints.push_pull);

        return .{
            .log_level = self.log_level orelse core.Logger.default,
            .log_quiet = self.log_quiet,
            .no_color = self.no_color,
            .endpoints = .{
                .req_rep = req_rep_channel,
                .pub_sub = pub_sub_channel,
                .push_pull = push_pull_channel,
            },
            .ipc_config = ipc_config,
            .scope = try allocator.dupe(u8, self.scope),
        };
    }
};