const std = @import("std");
const core = @import("core");
const Runner = @import("./Runner.zig");
const Setting = @import("./settings/Setting.zig");
const Config = @import("./Config.zig");

// const traceLog = std.log;
const traceLog = core.Logger.TraceDirect(Runner.APP_CONTEXT);

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
        traceLog.debug("Leak? {}", .{gpa.deinit()});
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

    try core.makeIpcChannelRoot(setting.general.ipc_root_dir_path);
    defer core.cleanupIpcChannelRoot(setting.general.ipc_root_dir_path);

    const config: Config = .{
        .stage_watch = .{
            .path = "stage-watch-files",
            .extra_args = &.{@tagName(.source_dir_path), @tagName(.watch)},
            .managed = true,
        },
        .stage_extract = &.{
            .{
                .path = "stage-extract-ph",
                .extra_args = &.{},
                .managed = true,
            },
        },
        .stage_generate = &.{
            .{
                .path = "stage-generate-ts",
                .extra_args = &.{@tagName(.output_dir_path)},
                .managed = true,
            },
        },
    };

    var runner = try Runner.init(allocator, setting);

    var stages = try config.spawnStages(allocator, setting.general, setting.command.generate);
    defer stages.deinit();

    try runner.run(config.stageCount(), setting);
    runner.deinit();
    
    try stages.wait();
}

test "main" {
    std.testing.refAllDecls(@This());
}