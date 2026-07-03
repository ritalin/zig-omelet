const std = @import("std");
const core = @import("core");

const GuestBaseConfig = core.configs.guests.GuestBaseConfig;
pub const GuestBaseArgId = GuestBaseConfig.ArgId(GuestBaseConfig.DescMap);

const GuestExtract = core.configs.guests.GuestGenerate;
pub const GuestExtractArgId = GuestExtract.ArgId(GuestExtract.DescMap);

pub const ArgHelp = @This();

pub const Config = core.help.types.HelpConfig(core.help.types.GuestHelpTag);
pub const Descriptor = core.help.types.Descriptor;

pub const toplevel_title_desc: Descriptor = .{ 
    .name = @import("build_options").exe_name,
    .description = "Extract type declarations from SQL",
};
pub const base_args_desc: Descriptor = .{
    .name = "Base Args",
    .description = null,
};
pub const extra_args_desc: Descriptor = .{
    .name = "Guest specific Args",
    .description = null,
};

pub const app_title: ArgHelp.Config = .{ .tag = .title, .sections = &.{} };
pub const base_args: ArgHelp.Config = .{ .tag = .base_args, .sections = &.{} };
pub const extra_args: ArgHelp.Config = .{ .tag = .extra_args, .sections = &.{} };

pub const toplevel: ArgHelp.Config = .{ .tag = .toplevel, .sections = &.{ app_title, base_args, extra_args } };
