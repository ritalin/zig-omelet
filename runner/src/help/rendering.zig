const std = @import("std");
const clap = @import("clap");

const ArgHelp = @import("./ArgHelp.zig");

pub fn renderToStderr(help: *const ArgHelp.Config) !void {
    var buffer: [1024]u8 = undefined;
    const t = std.debug.lockStderr(&buffer).terminal();
    defer std.debug.unlockStderr();
    
    try renderHelp(t.writer, help);
}

fn renderHelp(writer: *std.Io.Writer, help: *const ArgHelp.Config) !void {
    // render app title
    try renderDescriptor(writer, ArgHelp.resolveDescriptor(help.tag), .{});
    // render general args
    try renderSectionInternal(writer, ArgHelp.general_arg_desc, ArgHelp.GeneralSettingArgId);

    // render sections
    for (help.sections) |section| {
        try writer.writeByte('\n');
        try renderSection(writer, &section);
    }
}

fn renderSection(writer: *std.Io.Writer, config: *const ArgHelp.Config) !void {
    switch (config.tag) {
        .subcommands => {
            try renderSubcommands(writer, ArgHelp.senction_descriptors.subcommands, config);
        },
        .generate => {
            try renderSectionInternal(writer,ArgHelp.command_arg_desc, ArgHelp.GenerateCommandArgId);
        },
        else => {
            unreachable;
        }
    }
}

fn renderSectionInternal(writer: *std.Io.Writer, desc: ArgHelp.Descriptor, comptime ArgId: type) !void {
    try renderDescriptor(writer, desc, .{ .indent = 0 });
    try renderArgs(writer, ArgId);
}

fn renderDescriptor(writer: *std.Io.Writer, desc: ?ArgHelp.Descriptor, options: HelpOptions) !void {
    if (desc == null) return;
    if (options.indent) |indent| {
        try writer.splatByteAll(' ', indent);
    }
    if (options.name_width) |w| {
        try writer.print("{s:<[w]}", .{.@"0" = desc.?.name, .w = w});
    }
    else {
        try writer.writeAll(desc.?.name);
    }
    try writer.splatByteAll(' ', options.description_indent);
    if (desc.?.description) |d| {
        try writer.writeAll(d);
    }
    try writer.splatByteAll('\n', options.spacing_between_parameters + 1);
}

fn renderArgs(writer: *std.Io.Writer, comptime ArgId: type) !void {
    try clap.help(writer, ArgId, ArgId.Decls, .{.description_on_new_line = false, .spacing_between_parameters = 0, .description_indent = 0, .indent = 4});
}

fn renderSubcommands(writer: *std.Io.Writer, desc: ?ArgHelp.Descriptor, config: *const ArgHelp.Config) !void {
    // render capton
    try renderDescriptor(writer, desc, .{ .indent = 0 });

    const width = try measureNameWidth(config.sections);

    // render subcommands
    for (config.sections) |section| {
        try renderDescriptor(writer, ArgHelp.resolveDescriptor(section.tag), .{.description_indent = 4, .name_width = width, .indent = 4});
    }
}

fn measureNameWidth(sections: []const ArgHelp.Config) !usize {
    var width: usize = 0;

    for (sections) |section| {
        const desc = ArgHelp.resolveDescriptor(section.tag);
        if (desc) |d| {
            var discarding: std.Io.Writer.Discarding = .init(&.{});
            var cc: clap.ccw.CodepointCountingWriter = .init(&discarding.writer);
            try cc.interface.writeAll(d.name);
            width = @max(width, cc.codepoints_written);
        }
    }
    return width;
}

const HelpOptions = struct {
    spacing_between_parameters: usize = 0,
    description_indent: usize = 1,
    name_width: ?usize = null,
    indent: ?usize = null,
};