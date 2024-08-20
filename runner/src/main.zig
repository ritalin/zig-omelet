const std = @import("std");
const core = @import("core");
const Runner = @import("./Runner.zig");
const Setting = @import("./settings/Setting.zig");
const Config = @import("./Config.zig");

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
    // core.Logger.filterWith(.trace);

    try core.makeIpcChannelRoot(setting.general.stage_endpoints);
    defer core.cleanupIpcChannelRoot(setting.general.stage_endpoints);

    // TODO provide as configuration files
    const config: Config = .{
        .stage_watch = .{
            .path = exe_prefix ++ "-" ++ "watch-files",
            .extra_args = &.{
                @tagName(.source_dir_set), 
                @tagName(.filter_set), 
                @tagName(.watch)
            },
            .managed = true,
        },
        .stage_extract = &.{
            .{
                .path = exe_prefix ++ "-" ++ "duckdb-extract",
                .extra_args = &.{@tagName(.schema_dir_set)},
                .managed = true,
            },
        },
        .stage_generate = &.{
            .{
                .path = exe_prefix ++ "-" ++ "ts-generate",
                .extra_args = &.{@tagName(.output_dir_path)},
                .managed = true,
            },
        },
    };

    var runner = try Runner.init(allocator, setting);

    var stages = switch (try config.spawnStages(allocator, setting.general, setting.command.generate) ) {
        .help => |help_setting| {
            try help_setting.help(std.io.getStdErr().writer());
            std.process.exit(3);
        },
        .success => |stages| stages,
    };
    defer stages.deinit();

    try runner.run(config.stageCount(), setting);
    runner.deinit();
    
    try stages.wait();
}

test "main" {
    std.testing.refAllDecls(@This());
}