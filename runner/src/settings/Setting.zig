const std = @import("std");
// const clap = @import("clap");
const core = @import("core");

const ArgScanner = core.settings.types.ArgScanner;

const default_init_scope = @import("build_options").default_init_scope;

const config_types = @import("../configs/types.zig");
const ArgHelp = @import("../help/ArgHelp.zig");
const Setting = @This();

base: BaseSetting,
command: SubcommandSetting,

pub const BaseSetting = @import("./commands/BaseSetting.zig");
pub const SubcommandSetting = @import("./commands/Subcommand.zig").Setting;

pub fn loadFromArgs(io: std.Io, allocator: std.mem.Allocator, env: *const std.process.Environ.Map, args: std.process.Args) !core.settings.types.LoadResult(Setting, *const ArgHelp.Config) {
    var args_iter = try args.iterateAllocator(allocator);
    defer args_iter.deinit();

    _ = args_iter.next();

    var scanner = ArgScanner(std.process.Args.Iterator).init(&args_iter);
    var res = BaseSetting.Builder(std.process.Args.Iterator).fromArgs(allocator, &scanner, .stderr)
    catch {
        return .{.help = &ArgHelp.toplevel};
    };

    const sub_res = SubcommandSetting.fromArgs(io, allocator, env, &scanner, &res.builder, res.command);

    const setting_pair = switch (sub_res) {
        .help => |help| return .{.help = help},
        .success => |settings| settings,
    };

    return .{
        .success = .{
            .base = setting_pair.base,
            .command = setting_pair.command,
        }
    };
}

pub fn deinit(self: *Setting, io: std.Io, allocator: std.mem.Allocator) void {
    self.base.deinit(io, allocator);
    self.command.deinit(allocator);
}
