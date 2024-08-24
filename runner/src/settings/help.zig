const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const ArgDescriptions = core.settings.DescriptionMap.initComptime(.{
    // General
    .{@tagName(.req_rep_channel), .{.desc = "Comminicate Req/Rep endpoint for zmq", .value = "CHANNEL",}},
    .{@tagName(.pub_sub_channel), .{.desc = "Comminicate Pub/Sub endpoint for zmq", .value = "CHANNEL",}},
    .{@tagName(.log_level), .{.desc = "Pass through log level (err / warn / info / debug / trace). default: info", .value = "LEVEL",}},
    .{@tagName(.help), .{.desc = "Print command-specific usage", .value = "",}},
    // Commands
    .{@tagName(.generate), .{.desc = "Generate query parameters", .value = "",}},
    // Command/Generate
    .{@tagName(.source_dir_path), .{.desc = "Source SQL folder(s) or file(s)", .value = "PATH", .required = true}},
    .{@tagName(.output_dir_path), .{.desc = "Output folder", .value = "PATH", .required = true}},  
    .{@tagName(.schema_dir_path), .{.desc = "Schema SQL folder", .value = "PATH", .required = true}},
    .{@tagName(.include_filter), .{.desc = "Filter passing source/schema SQL directores or files satisfied", .value = "PATH"}},
    .{@tagName(.exclude_filter), .{.desc = "Filter rejecting source/schema SQL directores or files satisfied", .value = "PATH"}},
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
    command: ?CommandArgId,

    pub fn help(self: ArgHelpSetting, writer: anytype) !void {
        try writer.print("usage: {s} [General options] {s} [Subcommand options]\n\n", .{
            @import("build_options").exe_name, 
            if (self.command) |c| std.fmt.comptimePrint("{s}", .{@tagName(c)}) 
            else CommandArgId.options.category_name.?
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
                    try core.settings.showSubcommandAll(writer, CommandArgId);
                },
                .cmd_generate => {
                    try core.settings.showHelp(writer, GenerateCommandArgId);
                },
            }
        }
    }
};

pub const GeneralHelpSetting: ArgHelpSetting = .{ .tags = &.{.subcommand, .general}, .command = null };
