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

    try core.makeIpcChannelRoot(setting.general.stage_endpoints);
    defer core.cleanupIpcChannelRoot(setting.general.stage_endpoints);

    // TODO provide as configuration files
    const GenerateSetting = @import("./settings/commands/Generate.zig");
    const GenerateConfig = @import("./configs/GenerateConfig.zig");
    const MockConfig = Config.StageSet(GenerateSetting, GenerateConfig);

    // var arena = std.heap.ArenaAllocator.init(allocator);
    // defer arena.deinit();

    // var config: MockConfig = .{
    //     .arena = &arena,
    //     .stages = &.{
    //     // .stage_watch = 
    //     .{
    //         .category = .stage_watch,
    //         .location = exe_prefix ++ "-" ++ "watch-files",
    //         .extra_args = MockConfig.ExtraArgSet.init(.{
    //             .source_dir = .default,
    //             .schema_dir = .default,
    //             .include_filter = .default,
    //             .exclude_filter = .default,
    //             .watch = .default,
    //         }),
    //         //     @tagName(.source_dir_set), 
    //         //     @tagName(.schema_dir_set),
    //         //     @tagName(.filter_set), 
    //         //     @tagName(.watch)
    //         // },
    //         .managed = true,
    //     },
    //     // .stage_extract = &.{
    //         .{
    //             .category = .stage_extract,
    //             .location = exe_prefix ++ "-" ++ "duckdb-extract",
    //             .extra_args = MockConfig.ExtraArgSet.init(.{
    //                 .schema_dir = .default,
    //             }),
    //             // .extra_args = &.{@tagName(.schema_dir_set)},
    //             .managed = false,
    //         },
    //     // },
    //     // .stage_generate = &.{
    //         .{
    //             .category = .stage_generate,
    //             .location = exe_prefix ++ "-" ++ "ts-generate",
    //             .extra_args = MockConfig.ExtraArgSet.init(.{
    //                 .output_dir = .default,
    //             }),
    //             // .extra_args = &.{@tagName(.output_dir_path)},
    //             .managed = true,
    //         },
    //     },
    // };

    var config = config: {
        var file = try std.fs.cwd().openFile("/Users/tamurakazuhiko/work/test/ziglang/duckdb/sql-parser/runner/assets/configs/generate.zon", .{});
        defer file.close();
        break:config try MockConfig.createConfigFromFile(allocator, &file, setting.command.strategy());
    };
    defer config.deinit();

    var runner = try Runner.init(allocator, setting);
    errdefer runner.deinit();
    
    var stages = switch (try config.spawnAll(allocator, setting.general, setting.command.generate) ) {
        .help => |help_setting| {
            try help_setting.help(std.io.getStdErr().writer());
            std.process.exit(3);
        },
        .success => |stages| stages,
    };
    defer stages.deinit();

    try runner.run(stages.stage_count, setting);
    runner.deinit();
    
    try stages.wait();
}

test "main" {
    std.testing.refAllDecls(@This());
}