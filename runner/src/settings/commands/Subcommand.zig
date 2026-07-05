const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const default_init_scope = @import("build_options").default_init_scope;

const ArgScanner = core.settings.types.ArgScanner;

const config_types = @import("../../configs/types.zig");

const BaseSetting = @import("./BaseSetting.zig");
const Generate = @import("./Generate.zig");
const Initialize = @import("./Initialize.zig");

const ArgHelp = @import("../../help/ArgHelp.zig");
const SubcommandSetting = @This().Setting;

const SubcommandArgId = ArgId(.{});

pub const Setting = union(SubcommandArgId) {
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

fn activeTag(self: *const Setting) SubcommandArgId {
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
        pub const configTagName = enumIntoConfigTagName;
    };
}

fn enumFromString(s: ?core.types.Symbol) ?SubcommandArgId {
    if (s == null) return null;
    return std.meta.stringToEnum(SubcommandArgId, s.?);
}

fn enumIntoConfigTagName(self: SubcommandArgId) core.types.Symbol {
    const tag: core.types.Symbol = switch (self) {
        .generate => @tagName(self),
        .@"init-default", .@"init-config" => "initialize",
    };
    return tag;
}

pub fn buildFromArgs(
    io: std.Io,
    allocator: std.mem.Allocator, 
    env: *const std.process.Environ.Map,
    scanner: *ArgScanner(std.process.Args.Iterator), 
    base_builder: *BaseSetting.Builder(std.process.Args.Iterator),
    command: SubcommandArgId) core.settings.types.LoadResult(struct{base: BaseSetting, command: SubcommandSetting}, *const ArgHelp.Config)
{
    const command_setting: SubcommandSetting = command: {
        switch (command) {
            .generate => {
                var builder = 
                    Generate.Builder(std.process.Args.Iterator).fromArgs(
                        allocator, 
                        scanner, 
                        base_builder, 
                        .stderr) 
                    catch return .{.help = &ArgHelp.generate}
                ;
                defer builder.deinit(allocator);

                const setting = build: {
                    const options: core.configs.supports.FileResolveOptions = .{ 
                        .command = @tagName(command), 
                        .scope = base_builder.scope orelse default_init_scope, 
                        .category = .defaults, 
                        .root = config_types.path_candidates,
                        .default_scope = default_init_scope,
                    };
                    break:build builder.build(io, allocator, env, options) 
                    catch |err| switch (err) {
                        error.ShowCommandHelp => return .{.help = &ArgHelp.generate},
                        else => return .{.help = &ArgHelp.toplevel},
                    };
                };

                break:command .{ .generate = setting};
            },
            .@"init-default" => {
                var builder = Initialize.Builder(std.process.Args.Iterator).fromArgs(allocator, scanner, base_builder, .defaults, .stderr) catch return .{.help = &ArgHelp.init_default};

                const setting = build: {    
                    const options: core.configs.supports.FileResolveOptions = .{ 
                        .command = "initalize", 
                        .scope = base_builder.scope orelse default_init_scope, 
                        .category = .defaults, 
                        .root = config_types.path_candidates, 
                        .default_scope = default_init_scope 
                    };
                    break:build builder.build(io, allocator, env, options) 
                    catch |err| switch (err) {
                        error.ShowCommandHelp => return .{.help = &ArgHelp.init_default},
                        else => return .{.help = &ArgHelp.toplevel},
                    };
                };

                break:command .{ .@"init-default" = setting};
            },
            .@"init-config" => {
                var builder = Initialize.Builder(std.process.Args.Iterator).fromArgs(allocator, scanner, base_builder, .configs, .stderr) catch return .{.help = &ArgHelp.init_config};

                const setting = build: {    
                    const options: core.configs.supports.FileResolveOptions = .{ 
                        .command = "initalize", 
                        .scope = base_builder.scope orelse default_init_scope, 
                        .category = .defaults, 
                        .root = config_types.path_candidates, 
                        .default_scope = default_init_scope 
                    };
                    break:build builder.build(io, allocator, env, options) 
                    catch |err| switch (err) {
                        error.ShowCommandHelp => return .{.help = &ArgHelp.init_config},
                        else => return .{.help = &ArgHelp.toplevel},
                    };
                };

                break:command .{ .@"init-config" = setting};
            },
        }
    };

    const base_setting = build: {
        const scope = base_builder.scope orelse default_init_scope;
        const options: core.configs.supports.FileResolveOptions = .{ .command = "base", .scope = scope, .category = .defaults, .root = config_types.path_candidates, .default_scope = default_init_scope };
        break:build base_builder.build(io, allocator, env, scope, options) catch return .{ .help = &ArgHelp.toplevel };
    };

    return .{ .success = .{.base = base_setting, .command = command_setting} };
}
