const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const ArgHelp = @import("./ArgHelp.zig");
const HelpRenderer = core.help.ArgHelpRenderer(ArgHelp.Config.Tag);

pub fn renderToStderr(help: *const ArgHelp.Config) !void {
    const renderer = HelpRenderer.init(onRenderHelp);
    try renderer.renderToStderr(help);
}

fn onRenderHelp(writer: *std.Io.Writer, parent_settng: *const ArgHelp.Config, session_setting: *const ArgHelp.Config, depth: usize) !void {
    switch (session_setting.tag) {
        .title => {
            try renderTitle(writer, parent_settng);
        },
        .base_args => {
            try renderArgs(writer, &ArgHelp.toplevel, depth);
        },
        .extra_args => {
            try renderArgs(writer, parent_settng, depth);
        },
        .subcommands => {
            try renderSubcommandList(writer, ArgHelp.SubcommandArgId, depth);
        },
        else => unreachable,
    }
}

fn renderTitle(writer: *std.Io.Writer, parent_settng: *const ArgHelp.Config) !void {
    switch (parent_settng.tag) {
        .toplevel => {
            try HelpRenderer.Support.renderTitle(writer, ArgHelp.toplevel_title_desc);
        },
        .generate => {
            try HelpRenderer.Support.renderTitle(writer, ArgHelp.generate_cmd_desc);
        },
        else => unreachable,
    }
}
fn renderArgs(writer: *std.Io.Writer, parent_settng: *const ArgHelp.Config, depth: usize) !void {
    switch (parent_settng.tag) {
        .toplevel => {
            try HelpRenderer.Support.renderDescriptor(writer, ArgHelp.base_args_desc, .{ .name_after_colon = true });
            try HelpRenderer.Support.renderArgs(writer, ArgHelp.BaseSettingArgId, depth);
        },
        .generate => {
            try HelpRenderer.Support.renderDescriptor(writer, ArgHelp.command_args_desc, .{ .name_after_colon = true });
            try HelpRenderer.Support.renderArgs(writer, ArgHelp.GenerateCommandArgId, depth);
        },
        else => unreachable,
    }
}



fn renderSubcommandList(writer: *std.Io.Writer, comptime ArgId: type, depth: usize) !void {
    // render capton
    try HelpRenderer.Support.renderDescriptor(writer, ArgHelp.command_list_desc, .{ .name_after_colon = true });

    const width = try measureNameWidth(ArgId);

    // render subcommands
    inline for (ArgId.Decls) |decl| {
        const desc: ArgHelp.Descriptor = .{
            .name = @tagName(decl.id),
            .description = decl.id.description(),
        };
        try HelpRenderer.Support.renderDescriptor(writer, desc, .{.description_indent = 4, .name_width = width, .indent = 4 * depth});
    }
}

fn measureNameWidth(comptime ArgId: type) !usize {
    var width: usize = 0;

    for (std.meta.fieldNames(ArgId)) |name| {
        var discarding: std.Io.Writer.Discarding = .init(&.{});
        var cc: clap.ccw.CodepointCountingWriter = .init(&discarding.writer);
        try cc.interface.writeAll(name);
        width = @max(width, cc.codepoints_written);
    }
    return width;
}
