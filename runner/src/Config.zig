const std = @import("std");
const core = @import("core");
const clap = @import("clap");
const known_folders = @import("known_folders");

const log = core.Logger.TraceDirect(@import("build_options").app_context);

const help = @import("./settings/help.zig");

const loader = @import("./configs/config_loader.zig");
const mappings = @import("./configs/bind_mappings.zig");
const GeneralConfig = @import("./configs/GeneralConfig.zig");
const GenerateConfig = @import("./configs/GenerateConfig.zig");
const Stage = loader.Stage;

const Setting = @import("./settings/Setting.zig");

const GenerateSetting = @import("./settings/commands/Generate.zig");




const findDecl = mappings.findDecl;
const ConfigBindMap = mappings.BindMap;

pub const StageCount = std.enums.EnumFieldStruct(core.configs.StageCategory, usize, 0);

pub fn spawnStages(allocator: std.mem.Allocator, setting: Setting) !core.settings.LoadResult(StageProcess, help.ArgHelpSetting) {
    const result_ = spawn: {
        switch (setting.command.tag()) {
            .generate => {
                var stages = 
                    StageSet(GenerateSetting, GenerateConfig).createConfig(allocator, setting.command) 
                    catch |err| break:spawn err
                ;
                defer stages.deinit();
                break:spawn stages.spawnAll(setting.general, setting.command.generate);
            },
        }
    };

    if (result_) |result| {
        return result;
    }
    else |err| {
        _ = err;
        return .{.help = .{.tags = &.{.cmd_general}, .subcommand = .general}};
    }
}

pub fn StageSet(comptime SubcommandSetting: type, comptime SubcommandConfig: type) type {
    return struct {
        const Self = @This();
        const ArgId = SubcommandSetting.ArgId(.{});

        arena: *std.heap.ArenaAllocator,
        stages: []const Stage(ArgId),

        pub const ExtraArgSet = Stage(ArgId).ExtraArgSet;

        pub fn deinit(self: *Self) void {
            self.arena.deinit();
            self.arena.child_allocator.destroy(self.arena);
        }
        
        pub fn createConfig(allocator: std.mem.Allocator, subcommand: Setting.CommandSetting) !Self {
            var file = try core.configs.resolveFileCandidate(allocator, @tagName(subcommand.tag()), loader.ConfigPathCandidate) orelse {
                log.err("Configuration file not found.", .{});
                return error.CofigLoadFailed;
            };
            defer file.close();

            return createConfigFromFile(allocator, &file, subcommand.strategy());
        }

        pub fn createConfigFromFile(allocator: std.mem.Allocator, file: *std.fs.File, strategy_map: core.configs.StageStrategy) !Self {
            const arena = try allocator.create(std.heap.ArenaAllocator);
            arena.* = std.heap.ArenaAllocator.init(allocator);
            errdefer {
                arena.deinit();
                allocator.destroy(arena);
            }

            const stages = loader.StageLoader(ArgId).loadFromFile(arena.allocator(), file, strategy_map) catch {
                log.err("Configuration file load failed.", .{});
                return error.CofigLoadFailed;
            };

            return .{
                .arena = arena,
                .stages = stages,
            };
        }

        pub fn spawnAll(self: *Self, allocator: std.mem.Allocator, general_setting: Setting.GeneralSetting, subcommand_setting: SubcommandSetting) !core.settings.LoadResult(StageProcess, help.ArgHelpSetting) {
            var arena = try allocator.create(std.heap.ArenaAllocator);
            arena.* = std.heap.ArenaAllocator.init(allocator);
            errdefer {
                arena.deinit();
                allocator.destroy(arena);
            }

            var entries = std.ArrayList(StageProcess.Entry).init(arena.allocator());
            defer entries.deinit();

            const app_dir_path = try std.fs.selfExeDirPathAlloc(allocator);
            defer allocator.free(app_dir_path);
            log.debug("Runner/dir: {s}", .{app_dir_path});

            var app_dir = try std.fs.cwd().openDir(app_dir_path, .{});
            defer app_dir.close();

            const managed_allocator = arena.allocator();
            var count: StageCount = .{};

            for (self.stages) |*stage| {
                defer switch (stage.category) {
                    .stage_watch => count.stage_watch += 1,
                    .stage_extract => count.stage_extract += 1,
                    .stage_generate => count.stage_generate += 1,
                };

            // WatchStage: {
                // const stage = self.stage_watch;
                _ = try initStageProcess(
                    managed_allocator, app_dir, @constCast(stage), 
                    general_setting, subcommand_setting,
                    &entries
                );
                    
                // switch (result) {
                //     .help => |help_setting| return .{ .help = help_setting },
                //     .success => {},
                // }

                // break :WatchStage;
            // }
            }

            // ExtractStage: {
            //     for (self.stage_extract) |stage| {
            //         const result = initStageProcess(
            //             managed_allocator, app_dir, stage, 
            //             general_setting, generate_setting,
            //             &entries
            //         );
                    
            //         switch (try result) {
            //             .help => |help_setting| return .{ .help = help_setting },
            //             .success => {},
            //         }
            //     }
            //     break :ExtractStage;
            // }
            // GenerateStage: {
            //     for (self.stage_generate) |stage| {
            //         const result = initStageProcess(
            //             managed_allocator, app_dir, stage, 
            //             general_setting, generate_setting,
            //             &entries
            //         );
                    
            //         switch (try result) {
            //             .help => |help_setting| return .{ .help = help_setting },
            //             .success => {},
            //         }
            //     }
            //     break :GenerateStage;
            // }

            for (entries.items) |*entry| {
                _ = try entry.process.spawn();
            }

            return .{
                .success = .{
                    .arena = arena,
                    .entries = try entries.toOwnedSlice(),
                    .stage_count = count,
                }
            };
        }

        fn initStageProcess(
            allocator: std.mem.Allocator, base_dir: std.fs.Dir, stage: *Stage(ArgId), 
            general_setting: Setting.GeneralSetting, subcommand_setting: SubcommandSetting, entries: *std.ArrayList(StageProcess.Entry)) !core.settings.LoadResult(void, help.ArgHelpSetting) 
        {   
            if (!stage.managed) return .success;

            var args = std.ArrayList(core.Symbol).init(allocator);
            defer args.deinit();

            try args.append(try base_dir.realpathAlloc(allocator, stage.location));

            general: {
                try GeneralConfig.apply(general_setting, &args);
                break:general;
            }
            subcommand: {
                var iter = stage.extra_args.iterator();
                while (iter.next()) |extra| {
                    const name = SubcommandConfig.argName(extra.key);
                    switch(extra.value.*) {
                        .default => {
                            _ = try SubcommandConfig.applyValue(subcommand_setting, extra.key, &args);
                        },
                        .values => |values| {
                            try applyFixedValues(name, values, &args);
                        },
                        .enabled => |value| {
                            try applyFixedEnabled(name, value, &args);
                        },
                    }
                }
                

                // // var iter = stage.extra_args.iterator();
                // // while (iter.next()) |extra| {
                // // const result = apply(setting.generate, extra, &args),
                // // };

                // // for (stage.extra_args) |extra| {
                // //     const binder = GenerateConfigMap.get(extra);
                // //     std.debug.assert(binder != null);
                // //     switch (try binder.?(setting.command.generate, &args)) {
                // //         .help => |help_setting| return .{ .help = help_setting },
                // //         .success => {},
                // //     }
                // // }
                break :subcommand;
            }

            const cli_args = try std.mem.join(allocator, " ", args.items);
            defer allocator.free(cli_args);
            log.debug("stage args: {s}", .{cli_args});

            var process = std.process.Child.init(try args.toOwnedSlice(), allocator);
            process.stderr_behavior = .Ignore;
            process.stdout_behavior = .Ignore;

            try entries.append(.{.category = stage.category, .process = process});

            return .success;
        }

        fn applyFixedValues(name: core.Symbol, values: []const core.Symbol, args: *std.ArrayList(core.Symbol)) !void {
            for (values) |value| {
                try args.append(name);
                try args.append(value);
            }
        }

        fn applyFixedEnabled(name: core.Symbol, enabled: bool, args: *std.ArrayList(core.Symbol)) !void {
            if (enabled) {
                try args.append(name);
            }
        }
    };
}

pub const StageProcess = struct {
    pub const Entry = struct {
        category: core.configs.StageCategory,
        process: std.process.Child,
    };

    arena: *std.heap.ArenaAllocator, 
    entries: []Entry,
    stage_count: StageCount,

    pub fn deinit(self: *StageProcess) void {
        self.arena.deinit();
        self.arena.child_allocator.destroy(self.arena);
    }

    pub fn wait(self: *StageProcess) !void {
        log.debug("Waiting stage terminate...", .{});
        defer log.debug("Stage terminate done", .{});

        for (self.entries) |*entry| {
            _ = try entry.process.wait();
        }
    }
};

