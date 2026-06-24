const std = @import("std");
const clap = @import("clap");
const root = @import("../root.zig");

const StageName = root.types.StageName;
const Logger = root.Logger;
const LogLevel = root.events.LogLevel;

pub fn putConsoleLog(level: LogLevel, stage_name: StageName, comptime fmt: []const u8, args: anytype) !void {
    var buffer: [1024]u8 = undefined;
    var g = std.debug.lockStderr(&buffer);
    defer std.debug.unlockStderr();

    const terminal: std.Io.Terminal = .{
        .writer = &g.file_writer.interface,
        .mode = g.terminal_mode,
    };
    try Logger.putAppLog(terminal, level, stage_name, fmt, args);
}

pub fn reportClapError(diag: *const clap.Diagnostic, err: anyerror) !void {
    var buffer: [1024]u8 = undefined;
    const t = std.debug.lockStderr(&buffer).terminal();
    defer std.debug.unlockStderr();
    try diag.report(t.writer, err);
}