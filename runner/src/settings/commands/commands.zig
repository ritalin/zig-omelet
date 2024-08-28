const std = @import("std");
const clap = @import("clap");
const known_folders = @import("known_folders");

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

        var defaults_file = try resolveDefaultArgCandidate(arena.allocator(), id);
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

    const DefaultPathCandidate = std.enums.EnumArray(enum {current_dir, home_dir, executable_dir}, core.FilePath).init(.{
        .current_dir = ".omelet/defaults",
        .home_dir = ".omelet/defaults",
        .executable_dir = "defaults",
    });

    fn resolveDefaultArgCandidate(allocator: std.mem.Allocator, id: help.CommandArgId) !?std.fs.File {
        const file_name = try std.fmt.allocPrint(allocator, "{s}.zon", .{@tagName(id)});
        defer allocator.free(file_name);
        
        defaults_faule: {
            const path = try std.fs.path.join(allocator, &.{DefaultPathCandidate.get(.current_dir), file_name});
            defer allocator.free(path);

            return std.fs.cwd().openFile(path, .{}) catch |err| switch (err) {
                error.FileNotFound => break:defaults_faule,
                else => return err,
            };
        }
        defaults_faule: {
            var dir_ = try known_folders.open(allocator, .home, .{});
            if (dir_) |*dir| {
                defer dir.close();

                const path = try std.fs.path.join(allocator, &.{DefaultPathCandidate.get(.home_dir), file_name});
                defer allocator.free(path);

                return dir.openFile(path, .{}) catch |err| switch (err) {
                    error.FileNotFound => break:defaults_faule,
                    else => return err,
                };
            }
        }
        defaults_faule: {
            var dir_ = try known_folders.open(allocator, .executable_dir, .{});
            if (dir_) |*dir| {
                defer dir.close();

                const path = try std.fs.path.join(allocator, &.{DefaultPathCandidate.get(.executable_dir), file_name});
                defer allocator.free(path);

                return dir.openFile(path, .{}) catch |err| switch (err) {
                    error.FileNotFound => break:defaults_faule,
                    else => return err,
                };
            }
        }

        return null;
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