const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const help = @import("../help.zig");

pub const Generate = @import("./Generate.zig");

pub const CommandSetting = union(help.CommandArgId) {
    generate: Generate,

    pub fn loadArgs(arena: *std.heap.ArenaAllocator, comptime Parser: type, parser: *Parser) !core.settings.LoadResult(CommandSetting, help.ArgHelpSetting) {
        const id = findTag(parser.diagnostic) catch  |err| switch (err) {
            error.ShowGeneralHelp => return .{ .help = help.GeneralHelpSetting },
            else => return err,
        };

        const Iterator = @typeInfo(@TypeOf(parser.iter)).Pointer.child;
        
        switch (id) {
            .generate => {
                const setting = Generate.loadArgs(arena, Iterator, parser.iter) 
                catch {
                    return .{
                        .help = .{.tags = &.{ .cmd_generate, .cmd_general }, .command = .generate }
                    };
                };
                return .{
                    .success = .{ .generate = setting }
                };       
            },
        }
    }

    pub fn watchModeEnabled(self: CommandSetting) bool {
        return switch (self) {
            .generate => |c| c.watch,
        };
    }
};

pub fn findTag(diag: ?*clap.Diagnostic) !help.CommandArgId {
    if (diag == null) return error.ShowGeneralHelp;
    return std.meta.stringToEnum(help.CommandArgId, diag.?.arg) orelse return error.ShowGeneralHelp;
}