const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const ArgDescriptions = core.settings.DescriptionMap.initComptime(.{
    // General
    .{@tagName(.req_rep_channel), .{.desc = "Comminicate Req/Rep endpoint for zmq", .value = "CHANNEL",}},
    .{@tagName(.pub_sub_channel), .{.desc = "Comminicate Pub/Sub endpoint for zmq", .value = "CHANNEL",}},
    .{@tagName(.help), .{.desc = "Print command-specific usage", .value = "",}},
    // Commands
    .{@tagName(.generate), .{.desc = "Generate query parameters", .value = "",}},
    // Command/Generate
    .{@tagName(.source_dir), .{.desc = "Source SQL folder(s) or file(s)", .value = "PATH", .required = true}},
    .{@tagName(.output_dir), .{.desc = "Output filder", .value = "PATH", .required = true}},
});

const GeneralSetting = @import("./commands/GeneralSetting.zig");
pub const GeneralSettingArgId = GeneralSetting.ArgId(ArgDescriptions);
const CommandGeneralArgId = GeneralSetting.Command.ArgId(ArgDescriptions);

const GenerateSetting = @import("./commands/Generate.zig");
const GenerateCommandArgId = GenerateSetting.ArgId(ArgDescriptions);

pub const CommandArgId = enum {
    generate,

    pub usingnamespace core.settings.ArgHelp(@This(), ArgDescriptions);
    pub const options: core.settings.ArgHelpOption = .{.category_name = "Subcommands"};
};

pub const ArgHelpSetting = struct {
    pub const Tag = enum {
        general,
        subcommand,
        cmd_general,
        cmd_generate,
    };

    tags: []const Tag,

    pub fn help(self: ArgHelpSetting, writer: anytype) !void {
        for (self.tags) |tag| {
            switch (tag) {
                .general => {
                    try core.settings.showHelp(writer, GeneralSettingArgId);
                },
                .cmd_general => {
                    try core.settings.showHelp(writer, CommandGeneralArgId);
                },
                .subcommand => {
                    try core.settings.showSubcommandAll(writer, CommandArgId);
                },
                .cmd_generate => {
                    try core.settings.showHelp(writer, GenerateCommandArgId);
                },
            }
        }
    }
};

pub const GeneralHelpSetting: ArgHelpSetting = .{ .tags = &.{.subcommand, .general} };
