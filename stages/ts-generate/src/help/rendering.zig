const std = @import("std");
const core = @import("core");

const ArgHelp = @import("./ArgHelp.zig");
const HelpRenderer = core.help.ArgHelpRenderer(ArgHelp.Config.Tag);

pub fn render(config: *const ArgHelp.Config) !void {
    const renderer = HelpRenderer.init(onRenderHelp);
    try renderer.renderToStderr(config);
}

fn onRenderHelp(writer: *std.Io.Writer, parent_config: *const ArgHelp.Config, session_config: *const ArgHelp.Config, depth: usize) !void {
    _ = parent_config;
    
    switch (session_config.tag) {
        .title => {
            try HelpRenderer.Support.renderTitle(writer, ArgHelp.toplevel_title_desc);
        },
        .base_args => {
            try HelpRenderer.Support.renderDescriptor(writer, ArgHelp.base_args_desc, .{ .name_after_colon = true });
            try HelpRenderer.Support.renderArgs(writer, ArgHelp.GuestBaseArgId, depth);
        },
        .extra_args => {
            try HelpRenderer.Support.renderDescriptor(writer, ArgHelp.extra_args_desc, .{ .name_after_colon = true });
            try HelpRenderer.Support.renderArgs(writer, ArgHelp.GuestGenerateArgId, depth);
        },
        else => {
            unreachable;
        }
    }
}

