const std = @import("std");
// const clap = @import("clap");
const core = @import("core");

const config_types = @import("../configs/types.zig");

const ArgHelp = @import("../help/ArgHelp.zig");
const Setting = @This();

// arena: *std.heap.ArenaAllocator,
general: BaseSetting,
command: SubcommandSetting,

pub const BaseSetting = @import("./commands/BaseSetting.zig");
pub const SubcommandSetting = @import("./commands/Subcommand.zig").Setting;

pub fn loadFromArgs(io: std.Io, allocator: std.mem.Allocator, args: std.process.Args) !core.settings.types.LoadResult(Setting, *const ArgHelp.Config) {
    var args_iter = try args.iterateAllocator(allocator);
    defer args_iter.deinit();

    _ = args_iter.next();

    var res = BaseSetting.Builder.fromArgs(allocator, &args_iter)
    catch {
        return .{.help = &ArgHelp.toplevel};
    };

    const options: core.configs.supports.FileResolveOptions = .{ .command = "base", .scope = res.builder.scope, .category = .defaults, .root = config_types.path_candidates };
    const general_setting = try res.builder.build(io, allocator, options);

    const sub_res = SubcommandSetting.fromArgs(io, allocator, &args_iter, res.command, general_setting.scope);
    const command_setting = switch (sub_res) {
        .help => |help| return .{.help = help},
        .success => |setting| setting,
    };

    return .{
        .success = .{
            .general = general_setting,
            .command = command_setting,
        }
    };
}

pub fn deinit(self: *Setting, io: std.Io, allocator: std.mem.Allocator) void {
    self.general.deinit(io, allocator);
    self.command.deinit(allocator);
}
