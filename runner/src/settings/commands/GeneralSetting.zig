const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const log = core.Logger.SystemDirect(@import("build_options").APP_CONTEXT);

const Self = @This();

log_level: core.LogLevel,
runner_endpoints: core.Endpoints,
stage_endpoints: core.Endpoints,

pub fn ArgId(comptime descriptions: core.settings.DescriptionMap) type {
    return enum {
        req_rep_channel,
        pub_sub_channel,
        log_level,
        help,

        pub const Decls: []const clap.Param(@This()) = &.{
            .{.id = .req_rep_channel, .names = .{.long = "reqrep-channel"}, .takes_value = .one},
            .{.id = .pub_sub_channel, .names = .{.long = "pubsub-channel"}, .takes_value = .one},
            .{.id = .log_level, .names = .{.long = "log-level"}, .takes_value = .one},
            .{.id = .help, .names = .{.long = "help", .short = 'h'}, .takes_value = .none},
        };
        pub usingnamespace core.settings.ArgHelp(@This(), descriptions);
        pub const options: core.settings.ArgHelpOption = .{.category_name = "General options"};
    };
}

pub fn StageArgId(comptime descriptions: core.settings.DescriptionMap) type {
    return enum {
        request_channel,
        subscribe_channel,
        log_level,

        pub const Decls: []const clap.Param(@This()) = &.{
            .{.id = .request_channel, .names = .{.long = "request-channel"}, .takes_value = .one},
            .{.id = .subscribe_channel, .names = .{.long = "subscribe-channel"}, .takes_value = .one},
            .{.id = .log_level, .names = .{.long = "log-level"}, .takes_value = .one},
        };
        pub usingnamespace core.settings.ArgHelp(@This(), descriptions);
        pub const options: core.settings.ArgHelpOption = .{.category_name = "Stage general options"};
    };
}

pub const Command = struct {
    pub fn ArgId(comptime description: core.settings.DescriptionMap) type {
        return enum {
            help,

            pub const Decls: []const clap.Param(@This()) = &.{
                .{.id = .help, .names = .{.long = "help", .short = 'h'}, .takes_value = .none},
            };
            pub usingnamespace core.settings.ArgHelp(@This(), description);
            pub const options: core.settings.ArgHelpOption = .{.category_name = "General options"};
        };
    }
};

pub const Builder = struct {
    log_level: ?core.LogLevel,
    request_channel: ?core.Symbol,
    subscribe_channel: ?core.Symbol,
    
    pub fn init() Builder {
        return .{
            .log_level = null,
            .request_channel = null,
            .subscribe_channel = null,
        };
    }

    pub fn deinit(_: Builder) void {

    }

    pub fn loadArgs(comptime Parser: type, parser: *Parser) anyerror!Builder {
        var required_set = std.enums.EnumSet(ArgId(.{})).init(.{});
        var builder = Builder.init();
        
        while (true) {
            const arg_ = parser.next() catch {
                if (required_set.count() > 0) {
                    return error.SettingLoadFailed;
                }
                else {
                    return builder;
                }
            };
            if (arg_ == null) {
                return error.SettingLoadFailed;
            }

            if (arg_) |arg| switch (arg.param.id) {
                .help => return error.ShowHelp,
                .log_level => {
                    builder.log_level = core.settings.resolveLogLevel(arg.value) catch |err| {
                        log.warn("Unresolved log level: {?s}", .{arg.value});
                        return err;
                    };
                },
                .req_rep_channel => builder.request_channel = arg.value,
                .pub_sub_channel => builder.subscribe_channel = arg.value,
            };
        }
    }

    fn makeIPCChannel(allocator: std.mem.Allocator) ![]const u8 {
        var seed = std.rand.Random.DefaultPrng.init(@intCast(std.time.microTimestamp()));
        var rand = seed.random();

        var buf: [24]u8 = undefined;
        rand.bytes(&buf);

        const sub_path = try core.bytesToHexAlloc(allocator, &buf);
        defer allocator.free(sub_path);

        return std.fmt.allocPrint(allocator, "{s}/{s}", .{core.CHANNEL_ROOT, sub_path});
    }

    pub fn build (self: Builder, allocator: std.mem.Allocator) !Self {
        const channel_folder = try makeIPCChannel(allocator);
        defer allocator.free(channel_folder);
        const default_channel_root = try std.fmt.allocPrint(allocator, "ipc://{s}", .{channel_folder});
        defer allocator.free(default_channel_root);
        
        const req_rep_channels = channel: {
            if (self.request_channel) |channel| {
                break :channel .{
                    try core.resolveConnectPort(allocator, channel), // REQ
                    try core.resolveBindPort(allocator, channel), // REP
                };
            }
            else {
                break :channel .{
                    try core.resolveIPCConnectPort(allocator, default_channel_root, core.REQ_PORT), // REQ
                    try core.resolveIPCBindPort(allocator, default_channel_root, core.REQ_PORT), // REP
                };
            }
        };
        const pub_sub_channels = channel: {
            if (self.subscribe_channel) |channel| {
                break :channel .{
                    try core.resolveBindPort(allocator, channel), // PUB
                    try core.resolveConnectPort(allocator, channel), // SUB
                };
            }
            else {
                break :channel .{
                    try core.resolveIPCBindPort(allocator, default_channel_root, core.PUBSUB_PORT), // PUB
                    try core.resolveIPCConnectPort(allocator, default_channel_root, core.PUBSUB_PORT), // SUB
                };
            }
        };

        return .{
            .log_level = self.log_level orelse .info,
            .runner_endpoints = .{
                .req_rep = req_rep_channels[1],
                .pub_sub = pub_sub_channels[0],
            },
            .stage_endpoints = .{
                .req_rep = req_rep_channels[0],
                .pub_sub = pub_sub_channels[1],
            },
        };
    }
};