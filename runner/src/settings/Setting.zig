const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const log = core.Logger.SystemDirect(@import("build_options").AppContext);

const help = @import("./help.zig");

const GeneralSetting = @import("./commands/GeneralSetting.zig");
const CommandSetting = @import("./commands/commands.zig").CommandSetting;

const Setting = @This();

arena: *std.heap.ArenaAllocator,
general: GeneralSetting,
command: CommandSetting,


pub fn deinit(self: *Setting) void {
    self.arena.deinit();
    self.arena.child_allocator.destroy(self.arena);
}

pub fn loadFromArgs(allocator: std.mem.Allocator) !core.settings.LoadResult(Setting, help.ArgHelpSetting) {
    var args_iter = try std.process.argsWithAllocator(allocator);
    defer args_iter.deinit();

    _ = args_iter.next();

    var diag: clap.Diagnostic = .{};
    var parser = clap.streaming.Clap(help.GeneralSettingArgId, std.process.ArgIterator){
        .params = help.GeneralSettingArgId.Decls,
        .iter = &args_iter,
        .diagnostic = &diag,
    };

    const arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);
    errdefer allocator.destroy(arena);
    errdefer arena.deinit();


    const general_setting = 
        switch (try loadGeneralArgs(arena, @TypeOf(parser), &parser)) {
            .success => |setting| setting,
            .help => |setting| return .{ .help = setting },
        }
    ;
    const command_setting = 
        switch(try CommandSetting.loadArgs(arena, @TypeOf(parser), &parser)) {
            .success => |setting| setting,
            .help => |setting| return .{ .help = setting },
        }
    ;
    
    const setting = .{
        .arena = arena,
        .general = general_setting,
        .command = command_setting,
    };

    return .{ .success = setting };
}

fn loadGeneralArgs(arena: *std.heap.ArenaAllocator, comptime Parser: type, parser: *Parser) !core.settings.LoadResult(GeneralSetting, help.ArgHelpSetting) {
    const builder = 
        GeneralSetting.Builder.loadArgs(Parser, parser)
        catch |err| switch (err) {
            error.SettingLoadFailed, error.ShowHelp => return .{ .help = help.GeneralHelpSetting },
            else => return err,
        }
    ;

    return .{
        .success = try builder.build(arena.allocator())
    };
}
