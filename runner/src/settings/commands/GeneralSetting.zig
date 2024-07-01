const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const Self = @This();

ipc_root_dir_path: ?core.FilePath,
runner_endpoints: core.Endpoints,
stage_endpoints: core.Endpoints,

pub fn ArgId(comptime descriptions: core.settings.DescriptionMap) type {
    return enum {
        req_rep_channel,
        pub_sub_channel,
        help,

        pub const Decls: []const clap.Param(@This()) = &.{
            .{.id = .req_rep_channel, .names = .{.long = "reqrep-channel"}, .takes_value = .one},
            .{.id = .pub_sub_channel, .names = .{.long = "pubsub-channel"}, .takes_value = .one},
            .{.id = .help, .names = .{.long = "help", .short = 'h'}, .takes_value = .none},
        };
        pub usingnamespace core.settings.ArgHelp(@This(), descriptions);
        pub const options: core.settings.ArgHelpOption = .{.category_name = "General Options"};
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
            pub const options: core.settings.ArgHelpOption = .{.category_name = "General Options"};
        };
    }
};

pub const Builder = struct {
    request_channel: ?core.Symbol,
    subscribe_channel: ?core.Symbol,
    
    pub fn init() Builder {
        return .{
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
                .req_rep_channel => builder.request_channel = arg.value,
                .pub_sub_channel => builder.subscribe_channel = arg.value,
            };
        }
    }

    pub fn build (self: Builder, allocator: std.mem.Allocator) !Self {
        const default_channel_folder = core.CHANNEL_ROOT;
        const default_channel_root = std.fmt.comptimePrint("ipc://{s}", .{default_channel_folder});
        
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
            .ipc_root_dir_path = try allocator.dupe(u8, default_channel_folder),
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