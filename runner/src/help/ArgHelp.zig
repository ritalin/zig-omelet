const std = @import("std");
const core = @import("core");

const DescriptionItem = core.settings.types.DescriptionItem;

const BaseSetting = @import("../settings/commands/BaseSetting.zig");
const BaseSettingDescMap: core.settings.types.DescriptionMap = .initComptime(.{
    .{@tagName(.req_rep_channel), DescriptionItem{.desc = "Comminicate Req/Rep endpoint for nng", .value = "CHANNEL",}},
    .{@tagName(.pub_sub_channel), DescriptionItem{.desc = "Comminicate Pub/Sub endpoint for nng", .value = "CHANNEL",}},
    .{@tagName(.push_pull_channel), DescriptionItem{.desc = "Comminicate Push/Pull endpont for nng", .value = "CHANNEL"}},
    .{@tagName(.log_level), DescriptionItem{.desc = "Pass through log level (err / warn / info / debug / trace). default: info", .value = "LEVEL",}},
    .{@tagName(.log_quiet), DescriptionItem{.desc = "Disable all host and guest log output", .value = ""}},
    .{@tagName(.no_color), DescriptionItem{.desc = "Disable colored log", .value = ""}},
    .{@tagName(.interactive), DescriptionItem{.desc = "Launch as interactive mode", .value = ""}},
    .{@tagName(.use_scope), DescriptionItem{.desc = "Use environment scope. default: default", .value = "VALUE",}},
    .{@tagName(.help), DescriptionItem{.desc = "Print command-specific usage", .value = "",}},  
});
pub const BaseSettingArgId = BaseSetting.ArgId(BaseSettingDescMap);

const GenerateSetting = @import("../settings/commands/Generate.zig");
const GenerateCommandDescMap: core.settings.types.DescriptionMap = .initComptime(.{
    .{@tagName(.source_dir), DescriptionItem{.desc = "Source SQL folder(s) or file(s)", .value = "PATH", .required = true}},
    .{@tagName(.output_dir), DescriptionItem{.desc = "Output folder", .value = "PATH", .required = true}},  
    .{@tagName(.schema_dir), DescriptionItem{.desc = "Schema SQL folder", .value = "PATH", .required = true}},
    .{@tagName(.include_filter), DescriptionItem{.desc = "Filter passing source/schema SQL directores or files satisfied (optional)", .value = "PATH"}},
    .{@tagName(.exclude_filter), DescriptionItem{.desc = "Filter rejecting source/schema SQL directores or files satisfied (optional)", .value = "PATH"}},
});
pub const GenerateCommandArgId = GenerateSetting.ArgId(GenerateCommandDescMap);

const InitializeSetting = @import("../settings/commands/Initialize.zig");
const InitializeCommandDescMap: core.settings.types.DescriptionMap = .initComptime(.{
    .{@tagName(.target_scope), DescriptionItem{.desc = "init environment scope", .value = "VALUE", .required = true}},
    .{@tagName(.from_scope), DescriptionItem{.desc = "source environment scope (optional).", .value = "VALUE", .required = false}},
    .{@tagName(.global), DescriptionItem{.desc = "Enable globally setting/config", .value = "", .required = false}},
});
pub const InitializeCommandArgId = InitializeSetting.ArgId(InitializeCommandDescMap);

const PalletHelpDescMap: core.settings.types.DescriptionMap = .initComptime(.{
    .{@tagName(.help), DescriptionItem{.desc = "Show help text.", .value = "",}},  
    .{@tagName(.quit), DescriptionItem{.desc = "Exit this program.", .value = "",}},      
    .{@tagName(.run), DescriptionItem{.desc = "Run invoked subcommand again.", .value = "",}},  
});
pub const PalletCommandArgId = @import("../tasks/CommandPalletTask.zig").CommandArgIid(PalletHelpDescMap);

const SubcommandSetting = @import("../settings/commands/Subcommand.zig");
const SubcommandDescMap: core.settings.types.DescriptionMap = .initComptime(.{
    .{@tagName(.generate), DescriptionItem{.desc = generate_cmd_desc.description.?, .value = "",}},
    .{init_config_cmd_desc.name, DescriptionItem{.desc = init_config_cmd_desc.description.?, .value = "",}},
    .{init_default_cmd_desc.name, DescriptionItem{.desc = init_default_cmd_desc.description.?, .value = "",}}, 
});
pub const SubcommandArgId = SubcommandSetting.ArgId(SubcommandDescMap);

const ArgHelp = @This();

pub const ConfigTag = enum {
    toplevel,
    title,
    base_args,
    extra_args,
    subcommands,
    generate,
    init_config,
    init_default,
    pallet_help,
    pallet_commands,
};

pub const Config = core.help.types.HelpConfig(ConfigTag);
pub const Descriptor = core.help.types.Descriptor;

pub const toplevel_title_desc: Descriptor = .{ 
    .name = @import("build_options").exe_name, 
    .description = "Language binding declaration generator from SQL." 
};
pub const base_args_desc: Descriptor = .{
    .name = "Base Args",
};
pub const command_args_desc: Descriptor = .{
    .name = "Command Args",
};
pub const command_list_desc: Descriptor = .{ 
    .name = "Subcommands"
};

//
// Command descriptors
//

pub const generate_cmd_desc: Descriptor = .{ 
    .name = @tagName(.generate), 
    .description = "Generate query parameters/result-sets."
};
pub const init_config_cmd_desc: Descriptor = .{ 
    .name = @tagName(.@"init-config"), 
    .description = "Initialize new guest component configuration." 
};
pub const init_default_cmd_desc: Descriptor = .{
    .name = @tagName(.@"init-default"),
    .description = "Initialize new default settings."
};

pub const title: ArgHelp.Config = .{ .tag = .title, .sections = &.{} };
pub const base_args: ArgHelp.Config = .{ .tag = .base_args, .sections = &.{} };
pub const extra_args: ArgHelp.Config = .{ .tag = .extra_args, .sections = &.{} };
pub const subcommands: ArgHelp.Config = .{ .tag = .subcommands, .sections = &.{} };
pub const pallet_commands: ArgHelp.Config = .{ .tag = .pallet_commands, .sections = &.{} };

pub const toplevel: ArgHelp.Config = .{ .tag = .toplevel, .sections = &.{ title, base_args, subcommands } };
pub const generate: ArgHelp.Config = .{ .tag = .generate, .sections = &.{ title, base_args, extra_args, } };
pub const init_config: ArgHelp.Config = .{ .tag = .init_config, .sections = &.{ title, base_args, extra_args} };
pub const init_default: ArgHelp.Config = .{ .tag = .init_default, .sections = &.{ title, base_args, extra_args} };

pub const pallet_help: ArgHelp.Config = .{ .tag = .pallet_help, .sections = &.{ pallet_commands } };
