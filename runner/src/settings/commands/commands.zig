const std = @import("std");
const clap = @import("clap");
const known_folders = @import("known_folders");

const core = @import("core");

const help = @import("../help.zig");

pub const Generate = @import("./Generate.zig");

const path_candidates: core.configs.ConfigFileCandidates = .{
    .current_dir = ".omelet/defaults",
    .home_dir = ".omelet/defaults",
    .executable_dir = "defaults",
};

pub const CommandSetting = union(help.CommandArgId) {
    generate: Generate,

    pub fn loadArgs(arena: *std.heap.ArenaAllocator, comptime Parser: type, parser: *Parser) !core.settings.LoadResult(CommandSetting, help.ArgHelpSetting) {
        const id = findTag(parser.diagnostic) catch  |err| switch (err) {
            error.ShowGeneralHelp => return .{ .help = help.GeneralHelpSetting },
            else => return err,
        };

        var defaults_file = try core.configs.resolveFileCandidate(arena.allocator(), help.CommandArgId, id, path_candidates);
        defer if (defaults_file) |*file| file.close();

        const Iterator = @typeInfo(@TypeOf(parser.iter)).Pointer.child;
        
        switch (id) {
            .generate => {
                var builder = try Generate.Builder.init(arena.allocator(), defaults_file);
                const setting = builder.loadArgs(Iterator, parser.iter) 
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

    pub fn tag(self: CommandSetting) help.CommandArgId {
        return std.meta.activeTag(self);
    }

    pub fn strategy(self: CommandSetting) core.configs.StageStrategy {
        return switch (self) {
            .generate => Generate.strategy,
        };
    }
};

pub fn findTag(diag: ?*clap.Diagnostic) !help.CommandArgId {
    if (diag == null) return error.ShowGeneralHelp;
    return std.meta.stringToEnum(help.CommandArgId, diag.?.arg) orelse return error.ShowGeneralHelp;
}