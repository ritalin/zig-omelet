const std = @import("std");
const core = @import("core");

const ArgHelp = @import("../help/ArgHelp.zig");
const loader = @import("./config_loader.zig");
const Setting = @import("../settings/Setting.zig");

const GenerateSetting = @import("../settings/commands/Generate.zig");
const InitializeSetting = @import("../settings/commands/Initialize.zig");

host: Config.Host,
guests: std.MultiArrayList(Config.Guest),

const Config = @This();

pub const Host = @import("./types.zig").Host;
pub const Guest = @import("./types.zig").Guest;

pub fn load(io: std.Io, allocator: std.mem.Allocator, env: *const std.process.Environ.Map, setting: *const Setting) !core.settings.types.LoadResult(Config, *const ArgHelp.Config) {
    const config = 
        loadInternal(io, allocator, env, setting)
        catch {
            return .{
                .help = &ArgHelp.toplevel,
            };
        }
    ;

    return .{
        .success = config,
    };
}

fn loadInternal(io: std.Io, allocator: std.mem.Allocator, env: *const std.process.Environ.Map, setting: *const Setting) !Config {
    const host = loader.loadHost(io, allocator, env, setting.base.scope)
    catch |err| {
        handleError(err);
        return err;
    }; 
    const guests = loader.loadGuest(io, allocator, env, setting.command.tag(), setting.base.config_scope)
    catch |err| {
        handleError(err);
        return err;
    };

    return .{.host = host, .guests = guests};
}

fn handleError(err: anyerror) void {
    switch (err) {
        error.CofigLoadFailed => {
            std.log.err("Faild to load configuration file./err: {}", .{err});
        },
        error.InvalidConfig => {
            std.log.err("Invalid configuration file./err: {}", .{err});
        },
        error.InvalidStageCount => {
            std.log.err("Invalid guest stage count./err: {}", .{err});
        },
        else => {
            std.log.err("Unexpected error on loading configuration/err: {}", .{err});
        }
    }
}

pub fn deinit(self: *Config, allocator: std.mem.Allocator) void {
    self.guests.deinit(allocator);
    self.* = undefined;
}
