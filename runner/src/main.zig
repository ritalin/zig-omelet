const std = @import("std");
const core = @import("core");
const Runner = @import("./Runner.zig");
const Setting = @import("./settings/Setting.zig");
const Config = @import("./configs/Config.zig");

const log = core.Logger.TraceDirect(@import("build_options").app_context);
const exe_prefix = @import("build_options").exe_prefix;

const default_log_level = .debug;
const std_options = .{
    .scope_levels = &.{
        .{.scope = .default, .level = default_log_level}, 
        .{.scope = .trace, .level = default_log_level},
    },
};

pub fn main() !void {
    var gpa = (std.heap.GeneralPurposeAllocator(.{}){});
    defer {
        log.debug("Leak? {}", .{gpa.deinit()});
    }
    const allocator = gpa.allocator();

    var setting = switch (try Setting.loadFromArgs(allocator)) {
        .help => |setting| {
            try setting.help(std.io.getStdErr().writer());
            std.process.exit(2);
        },
        .success => |setting| setting,
    };
    defer setting.deinit();

    core.Logger.filterWith(setting.general.log_level);

    try core.makeIpcChannelRoot(setting.general.stage_endpoints);
    defer core.cleanupIpcChannelRoot(setting.general.stage_endpoints);

    var runner = try Runner.init(allocator, setting);
    errdefer runner.deinit();

    var stages = switch (try Config.spawnStages(allocator, setting)) {
        .help => |help_setting| {
            try help_setting.help(std.io.getStdErr().writer());
            std.process.exit(2);
        },
        .success => |stages| stages,
    };
    defer stages.deinit();

    try runner.run(stages.stage_count, setting);
    runner.deinit();
    
    try stages.wait();
}

test "main" {
    core.Logger.disable();
    std.testing.refAllDecls(@This());
}