const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const DescriptionItem = core.settings.types.DescriptionItem;

// TODO:
const ArgDescriptions = core.settings.types.DescriptionMap.initComptime(.{
//     // General
//     // Commands
//     .{@tagName(.generate), DescriptionItem{.desc = "Generate query parameters/result-sets", .value = "",}},
//     .{@tagName(.@"init-default"), DescriptionItem{.desc = "Initialize subcommand default value environment", .value = "",}},
//     .{@tagName(.@"init-config"), DescriptionItem{.desc = "Initialize subcommand configuration environment", .value = "",}},
//     .{log_style: "Set log output style (integrated / stderr / discard). Default: stderr."},
//     // Command/Generate
//     .{@tagName(.source_dir), DescriptionItem{.desc = "Source SQL folder(s) or file(s)", .value = "PATH", .required = true}},
//     .{@tagName(.output_dir), DescriptionItem{.desc = "Output folder", .value = "PATH", .required = true}},  
//     .{@tagName(.schema_dir), DescriptionItem{.desc = "Schema SQL folder", .value = "PATH", .required = true}},
//     .{@tagName(.include_filter), DescriptionItem{.desc = "Filter passing source/schema SQL directores or files satisfied (optional)", .value = "PATH"}},
//     .{@tagName(.exclude_filter), DescriptionItem{.desc = "Filter rejecting source/schema SQL directores or files satisfied (optional)", .value = "PATH"}},
//     // Command/init-default
//     .{@tagName(.subcommand), DescriptionItem{.desc = "Subcommand name", .value = "COMMAND", .required = true}},
//     .{@tagName(.new_scope), DescriptionItem{.desc = "init environment scope (default: default)", .value = "VALUE", .required = false}},
//     .{@tagName(.from_scope), DescriptionItem{.desc = "source environment scope (optional)", .value = "VALUE", .required = false}},
//     .{@tagName(.global), DescriptionItem{.desc = "Enable globally setting/config", .value = "", .required = false}},
});

const GeneralSetting = @import("../settings/commands/GeneralSetting.zig");
const GeneralSettingDescMap: core.settings.types.DescriptionMap = .initComptime(.{
    .{@tagName(.req_rep_channel), DescriptionItem{.desc = "Comminicate Req/Rep endpoint for nng", .value = "CHANNEL",}},
    .{@tagName(.pub_sub_channel), DescriptionItem{.desc = "Comminicate Pub/Sub endpoint for nng", .value = "CHANNEL",}},
    .{@tagName(.push_pull_channel), DescriptionItem{.desc = "Comminicate Push/Pull endpont for nng", .value = "CHANNEL"}},
    .{@tagName(.log_level), DescriptionItem{.desc = "Pass through log level (err / warn / info / debug / trace). default: info", .value = "LEVEL",}},
    .{@tagName(.log_quiet), DescriptionItem{.desc = "Disable all host and guest log output", .value = ""}},
    .{@tagName(.no_color), DescriptionItem{.desc = "Disable colored log", .value = ""}},
    .{@tagName(.use_scope), DescriptionItem{.desc = "Use environment scope. default: default", .value = "VALUE",}},
    .{@tagName(.help), DescriptionItem{.desc = "Print command-specific usage", .value = "",}},  
});
pub const GeneralSettingArgId = GeneralSetting.ArgId(GeneralSettingDescMap);

const GenerateSetting = @import("../settings/commands/Generate.zig");
const GenerateCommandDescmap: core.settings.types.DescriptionMap = .initComptime(.{
    .{@tagName(.source_dir), DescriptionItem{.desc = "Source SQL folder(s) or file(s)", .value = "PATH", .required = true}},
    .{@tagName(.output_dir), DescriptionItem{.desc = "Output folder", .value = "PATH", .required = true}},  
    .{@tagName(.schema_dir), DescriptionItem{.desc = "Schema SQL folder", .value = "PATH", .required = true}},
    .{@tagName(.include_filter), DescriptionItem{.desc = "Filter passing source/schema SQL directores or files satisfied (optional)", .value = "PATH"}},
    .{@tagName(.exclude_filter), DescriptionItem{.desc = "Filter rejecting source/schema SQL directores or files satisfied (optional)", .value = "PATH"}},
    .{@tagName(.watch), DescriptionItem{.desc = "Launch as interactive mode", .value = ""}},
});
pub const GenerateCommandArgId = GenerateSetting.ArgId(GenerateCommandDescmap);

// const InitializeSetting = @import("./commands/Initialize.zig");
// const InitializeCommandArgId = InitializeSetting.InitArgId(ArgDescriptions);

const SubcommandSetting = @import("../settings/commands/Subcommand.zig");
pub const SubcommandArgId = SubcommandSetting.ArgId(.{
    .{@tagName(.generate), DescriptionItem{.desc = "Generate query parameters/result-sets", .value = "",}},
    .{@tagName(.@"init-default"), DescriptionItem{.desc = "Initialize subcommand default value environment", .value = "",}},
    .{@tagName(.@"init-config"), DescriptionItem{.desc = "Initialize subcommand configuration environment", .value = "",}}, 
});
// const SubcommandHelp = struct {
//     pub usingnamespace core.settings.ArgHelp(core.SubcommandArgId, ArgDescriptions);
//     pub const options: core.settings.ArgHelpOption = .{.category_name = "Subcommands"};
// };

const ArgHelp = @This();

pub const Config = struct {
    tag: ArgHelp.Config.Tag,
    sections: []const ArgHelp.Config,

    const Self = @This();

    pub const Tag = enum {
        toplevel,
        args,
        subcommands,
        generate,
        init_config,
        init_default,
    };
};

pub const Descriptor = struct {
    name: []const u8,
    description: ?[]const u8 = null,
};

pub const DescriptorMap = std.enums.EnumFieldStruct(ArgHelp.Config.Tag, ?ArgHelp.Descriptor, null);

pub const senction_descriptors: DescriptorMap = .{
    .toplevel = .{ 
        .name = "omelet:", 
        .description = "Language binding declaration generator from SQL." 
    },
    .subcommands = .{ 
        .name = "Subcommands:" 
    },
    .generate = .{ 
        .name = "generate", 
        .description = "Generate language data binding." 
    },
    .init_config = .{ 
        .name = "init-config", 
        .description = "Initialize new guest component configuration." 
    },
    .init_default = .{
        .name = "init-default",
        .description = "Initialize new default settings."
    },
    .args = null,
};

pub const general_arg_desc: ArgHelp.Descriptor = .{
    .name = "General Args:",
};
pub const command_arg_desc: ArgHelp.Descriptor = .{
    .name = "Command Args:",
};

pub fn resolveDescriptor(tag: ArgHelp.Config.Tag) ?ArgHelp.Descriptor {
    inline for (std.meta.fields(ArgHelp.Config.Tag)) |f| {
        if (f.value == @intFromEnum(tag)) {
            return @field(senction_descriptors, f.name);
        }
    }
    unreachable;
}


//     pub fn help(self: ArgHelpSetting, writer: anytype) !void {
//         try writer.print("usage: {s} [General options] {s} [Subcommand options]\n\n", .{
//             @import("build_options").exe_name, 
//             if (self.command) |c| @tagName(c) 
//             else SubcommandHelp.options.category_name.?
//         });

//         for (self.tags) |tag| {
//             switch (tag) {
//                 .general => {
//                     try core.settings.showHelp(writer, GeneralSettingArgId);
//                 },
//                 .cmd_general => {
//                     try core.settings.showHelp(writer, CommandGeneralArgId);
//                 },
//                 .subcommand => {
//                     try core.settings.showSubcommandAll(writer, core.SubcommandArgId, SubcommandHelp);
//                 },
//                 .cmd_generate => {
//                     try core.settings.showHelp(writer, GenerateCommandArgId);
//                 },
//                 .cmd_init_default, .cmd_init_config => {
//                     try core.settings.showHelp(writer, InitializeCommandArgId);
//                 },
//             }
//         }
//     }
pub const toplevel: ArgHelp.Config = .{ .tag = .toplevel, .sections = &.{ subcommands } };
pub const args: ArgHelp.Config = .{ .tag = .args, .sections = &.{} };
pub const subcommands: ArgHelp.Config = .{ .tag = .subcommands, .sections = &.{generate, init_config, } };
pub const generate: ArgHelp.Config = .{ .tag = .generate, .sections = &.{args} };
pub const init_config: ArgHelp.Config = .{ .tag = .init_config, .sections = &.{args} };

