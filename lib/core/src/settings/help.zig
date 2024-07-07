const std = @import("std");
const clap = @import("clap");

const types = @import("./types.zig");

pub fn showHelp(writer: anytype, comptime Id: type) !void {
    try showCategory(writer, Id);
    try clap.help(writer, Id, Id.Decls, .{});
}

pub fn showSubcommandAll(writer: anytype, comptime Id: type) !void {
    const clap_opt: clap.HelpOptions = .{};
    const max_width = try measureLineWidth(Id);

    try showCategory(writer, Id);

    inline for (std.meta.fields(Id)) |id| {
        // write command
        try writer.writeByteNTimes(' ', clap_opt.indent);
        try writer.writeAll(id.name);
        try writer.writeByteNTimes(' ', clap_opt.indent + max_width - id.name.len);
        // write description
        try writer.writeAll(@as(Id, @enumFromInt(id.value)).description());
        try writer.writeByte('\n');
    }
    
    try writer.writeByte('\n');
}

fn showCategory(writer: anytype, comptime Id: type) !void {
    if (Id.options.category_name) |category| {
        try writer.print("{s}:\n", .{category});
        try writer.writeByte('\n');
    }
}

fn measureLineWidth(comptime Id: type) !usize {
    var max_width: usize = 0;

    inline for (std.meta.fields(Id)) |id| {
        var cw = std.io.countingWriter(std.io.null_writer);
        var writer = cw.writer();

        try writer.writeAll(id.name);
        max_width = @max(max_width, cw.bytes_written);
    }

    return max_width;
}
