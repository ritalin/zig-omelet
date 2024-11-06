const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const DescriptionItem = core.settings.DescriptionItem;

const ArgDescriptions = core.settings.DescriptionMap.initComptime(.{
    // General
    .{@tagName(.req_rep_channel), DescriptionItem{.desc = "Comminicate Req/Rep endpoint for zmq", .value = "CHANNEL",}},
    .{@tagName(.pub_sub_channel), DescriptionItem{.desc = "Comminicate Pub/Sub endpoint for zmq", .value = "CHANNEL",}},
    .{@tagName(.log_level), DescriptionItem{.desc = "Pass through log level (err / warn / info / debug / trace). default: info", .value = "LEVEL",}},
    .{@tagName(.help), DescriptionItem{.desc = "Print command-specific usage", .value = "",}},
    // Commands
    .{@tagName(.generate), DescriptionItem{.desc = "Generate query parameters/result-sets", .value = "",}},
    .{@tagName(.@"init-default"), DescriptionItem{.desc = "Initialize subcommand default value config", .value = "",}},
    // Command/Generate
    .{@tagName(.source_dir), DescriptionItem{.desc = "Source SQL folder(s) or file(s)", .value = "PATH", .required = true}},
    .{@tagName(.output_dir), DescriptionItem{.desc = "Output folder", .value = "PATH", .required = true}},  
    .{@tagName(.schema_dir), DescriptionItem{.desc = "Schema SQL folder", .value = "PATH", .required = true}},
    .{@tagName(.include_filter), DescriptionItem{.desc = "Filter passing source/schema SQL directores or files satisfied", .value = "PATH"}},
    .{@tagName(.exclude_filter), DescriptionItem{.desc = "Filter rejecting source/schema SQL directores or files satisfied", .value = "PATH"}},
    // Command/init-default
    .{@tagName(.subcommand), DescriptionItem{.desc = "Subcommand name", .value = "COMMAND", .required = true}},
    .{@tagName(.global), DescriptionItem{.desc = "Enable globally setting/config", .value = "", .required = false}},
});

const GeneralSetting = @import("./commands/GeneralSetting.zig");
pub const GeneralSettingArgId = GeneralSetting.ArgId(ArgDescriptions);
const CommandGeneralArgId = GeneralSetting.Command.ArgId(ArgDescriptions);

const GenerateSetting = @import("./commands/Generate.zig");
const GenerateCommandArgId = GenerateSetting.ArgId(ArgDescriptions);

const InitializeSetting = @import("./commands/Initialize.zig");
const InitializeCommandArgId = InitializeSetting.ArgId(ArgDescriptions);

const SubcommandHelp = struct {
    pub usingnamespace core.settings.ArgHelp(core.SubcommandArgId, ArgDescriptions);
    pub const options: core.settings.ArgHelpOption = .{.category_name = "Subcommands"};
};

pub const ArgHelpSetting = struct {
    pub const Tag = enum {
        general,
        subcommand,
        cmd_general,
        cmd_generate,
        cmd_init_default,
    };

    tags: []const Tag,
    command: ?core.SubcommandArgId,

    pub fn help(self: ArgHelpSetting, writer: anytype) !void {
        try writer.print("usage: {s} [General options] {s} [Subcommand options]\n\n", .{
            @import("build_options").exe_name, 
            if (self.command) |c| @tagName(c) 
            else SubcommandHelp.options.category_name.?
        });

        for (self.tags) |tag| {
            switch (tag) {
                .general => {
                    try core.settings.showHelp(writer, GeneralSettingArgId);
                },
                .cmd_general => {
                    try core.settings.showHelp(writer, CommandGeneralArgId);
                },
                .subcommand => {
                    try core.settings.showSubcommandAll(writer, core.SubcommandArgId, SubcommandHelp);
                },
                .cmd_generate => {
                    try core.settings.showHelp(writer, GenerateCommandArgId);
                },
                .cmd_init_default => {
                    try core.settings.showHelp(writer, InitializeCommandArgId);
                }
            }
        }
    }
};

pub const GeneralHelpSetting: ArgHelpSetting = .{ .tags = &.{.subcommand, .general}, .command = null };
