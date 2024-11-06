const std = @import("std");
const clap = @import("clap");
const known_folders = @import("known_folders");

const core = @import("core");

const help = @import("../help.zig");

pub const Generate = @import("./Generate.zig");
pub const Initialize = @import("./Initialize.zig");

const path_candidates: core.configs.ConfigFileCandidates = .{
    .current_dir = ".omelet",
    .home_dir = ".omelet",
    .executable_dir = "",
};

pub const CommandSetting = union(core.SubcommandArgId) {
    generate: Generate,
    @"init-default": Initialize,
    @"init-config": Initialize,

    pub fn loadArgs(arena: *std.heap.ArenaAllocator, comptime Parser: type, parser: *Parser, scope: core.Symbol) !core.settings.LoadResult(CommandSetting, help.ArgHelpSetting) {
        const id = findTag(parser.diagnostic) catch  |err| switch (err) {
            error.ShowGeneralHelp => return .{ .help = help.GeneralHelpSetting },
            else => return err,
        };

        var defaults_file = try core.configs.resolveFileCandidate(arena.allocator(), @tagName(id), path_candidates, scope, .defaults);
        defer if (defaults_file) |*file| file.close();

        const Iterator = @typeInfo(@TypeOf(parser.iter)).pointer.child;
        
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
            .@"init-default" => {
                var builder = Initialize.Builder.init(arena.allocator(), .defaults, false, .{.@"init-default" = true, .@"init-config" = true});
                const setting = builder.loadArgs(Iterator, parser.iter) 
                catch {
                    return .{
                        .help = .{.tags = &.{ .cmd_init_default, .cmd_general }, .command = .@"init-default" }
                    };
                };
                return .{
                    .success = .{ .@"init-default" = setting }
                };       
            },
            .@"init-config" => {
                var builder = Initialize.Builder.init(arena.allocator(), .configs, false, .{});
                const setting = builder.loadArgs(Iterator, parser.iter) 
                catch {
                    return .{
                        .help = .{.tags = &.{ .cmd_init_config, .cmd_general }, .command = .@"init-config" }
                    };
                };
                return .{
                    .success = .{ .@"init-default" = setting }
                };       
            }
        }
    }

    pub fn watchModeEnabled(self: CommandSetting) bool {
        return switch (self) {
            .generate => |c| c.watch,
            else => false,
        };
    }

    pub fn tag(self: CommandSetting) core.SubcommandArgId {
        return std.meta.activeTag(self);
    }

    pub fn strategy(self: CommandSetting) core.configs.StageStrategy {
        return switch (self) {
            .generate => Generate.strategy,
            .@"init-default", .@"init-config" => Initialize.strategy,
        };
    }

    pub fn watching(self: CommandSetting) bool {
        return switch (self) {
            .generate => |setting| return setting.watch,
            else => false,
        };
    }
};

pub fn findTag(diag: ?*clap.Diagnostic) !core.SubcommandArgId {
    if (diag == null) return error.ShowGeneralHelp;

    const subcommand_text = @constCast(diag.?.arg);

    return std.meta.stringToEnum(core.SubcommandArgId, subcommand_text) orelse return error.ShowGeneralHelp;
}