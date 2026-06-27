const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const config_types = @import("../../configs/types.zig");

const Generate = @import("./Generate.zig");
const Initialize = @import("./Initialize.zig");

const ArgHelp = @import("../../help/ArgHelp.zig");
const SubcommandSetting = @This().Setting;

const SubcommandArgd = ArgId(.{});

pub const Setting = union(SubcommandArgd) {
    generate: Generate,
    @"init-default": Initialize,
    @"init-config": Initialize,

    pub const deinit = deinitSetting;
    pub const fromArgs = buildFromArgs;
    pub const tag = activeTag;
};

fn deinitSetting(self: *Setting, allocator: std.mem.Allocator) void {
    switch (self.*) {
        .generate => |*setting| setting.deinit(allocator),
        .@"init-default" => |*setting| setting.deinit(allocator),
        .@"init-config" => |*setting| setting.deinit(allocator),
    }
}

fn activeTag(self: *const Setting) SubcommandArgd {
    return std.meta.activeTag(self.*);
}

pub fn ArgId(comptime descriptions: core.settings.types.DescriptionMap) type {
    return enum {
        generate,
        @"init-default",
        @"init-config",

        pub const Decls: []const clap.Param(ArgId(descriptions)) = &.{
            .{.id = .generate, .takes_value = .none },
            .{.id = .@"init-default", .takes_value = .none},
            .{.id = .@"init-config", .takes_value = .none},
        };

        const arg_view = core.settings.types.ArgHelp(@This(), descriptions);
        pub const description = arg_view.description;
        pub const value = arg_view.value;
        pub const fromString = enumFromString;
    };
}

fn enumFromString(s: ?core.types.Symbol) ?SubcommandArgd {
    if (s == null) return null;
    return std.meta.stringToEnum(SubcommandArgd, s.?);
}

pub fn buildFromArgs(
    io: std.Io,
    allocator: std.mem.Allocator,
    iter: *std.process.Args.Iterator, 
    command: SubcommandArgd,
    scope: core.types.Symbol) core.settings.types.LoadResult(SubcommandSetting, *const ArgHelp.Config)
{
    const command_setting: SubcommandSetting = command: {
        switch (command) {
            .generate => {
                var builder = Generate.Builder(std.process.Args.Iterator).fromArgs(allocator, iter, .stderr) catch return .{.help = &ArgHelp.generate};
                defer builder.deinit();

                const options: core.configs.supports.FileResolveOptions = .{ .command = @tagName(command), .scope = scope, .category = .defaults, .root = config_types.path_candidates };
                const setting = builder.build(io, options) catch return .{.help = &ArgHelp.generate};
                break:command .{ .generate = setting};
            },
            .@"init-default" => {
                unreachable;
            },
            .@"init-config" => {
                unreachable;
            },  
        }
    };

    return .{ .success = command_setting };
}
