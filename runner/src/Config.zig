const std = @import("std");
const core = @import("core");
const clap = @import("clap");

const log = core.Logger.TraceDirect(@import("build_options").app_context);
const help = @import("./settings/help.zig");

const GeneralSetting = @import("./settings/commands/GeneralSetting.zig");
const GenerateSetting = @import("./settings/commands/Generate.zig");

const Config = @This();

stage_watch: Config.Stage,
stage_extract: []const Config.Stage,
stage_generate: []const Config.Stage,

pub const Stage = struct {
    path: core.FilePath,
    extra_args: []const core.Symbol,
    managed: bool,
};

pub const StageCount = std.enums.EnumFieldStruct(std.meta.FieldEnum(Config), usize, 0);

pub fn stageCount(self: Config) StageCount {
    return .{
        .stage_watch = 1,
        .stage_extract = self.stage_extract.len,
        .stage_generate = self.stage_generate.len,
    };
}

pub const StageProcess = struct {
    pub const Entry = std.process.Child;

    arena: *std.heap.ArenaAllocator, 
    entries: []Entry,

    pub fn deinit(self: *StageProcess) void {
        self.arena.deinit();
        self.arena.child_allocator.destroy(self.arena);
    }

    pub fn wait(self: *StageProcess) !void {
        log.debug("Waiting stage terminate...", .{});
        defer log.debug("Stage terminate done", .{});

        for (self.entries) |*entry| {
            _ = try entry.wait();
        }
    }
};

pub fn spawnStages(self: Config, allocator: std.mem.Allocator, general_setting: GeneralSetting, generate_setting: GenerateSetting) !core.settings.LoadResult(StageProcess, help.ArgHelpSetting) {
    var arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();

    var entries = std.ArrayList(StageProcess.Entry).init(arena.allocator());
    defer entries.deinit();

    const app_dir_path = try std.fs.selfExeDirPathAlloc(allocator);
    defer allocator.free(app_dir_path);
    log.debug("Runner/dir: {s}", .{app_dir_path});

    var app_dir = try std.fs.cwd().openDir(app_dir_path, .{});
    defer app_dir.close();

    const managed_allocator = arena.allocator();

    WatchStage: {
        const stage = self.stage_watch;
        _ = try initStageProcess(
            managed_allocator, app_dir, stage, 
            general_setting, generate_setting,
            &entries
        );
        break :WatchStage;
    }
    ExtractStage: {
        for (self.stage_extract) |stage| {
            const result = initStageProcess(
                managed_allocator, app_dir, stage, 
                general_setting, generate_setting,
                &entries
            );
            
            switch (try result) {
                .help => |help_setting| return .{ .help = help_setting },
                .success => {},
            }
        }
        break :ExtractStage;
    }
    GenerateStage: {
        for (self.stage_generate) |stage| {
            const result = initStageProcess(
                managed_allocator, app_dir, stage, 
                general_setting, generate_setting,
                &entries
            );
            
            switch (try result) {
                .help => |help_setting| return .{ .help = help_setting },
                .success => {},
            }
        }
        break :GenerateStage;
    }

    for (entries.items) |*entry| {
        _ = try entry.spawn();
    }

    return .{
        .success = .{
            .arena = arena,
            .entries = try entries.toOwnedSlice(),
        }
    };
}

fn initStageProcess(
    allocator: std.mem.Allocator, base_dir: std.fs.Dir, stage: Stage, 
    general_setting: GeneralSetting, generate_setting: GenerateSetting, entries: *std.ArrayList(StageProcess.Entry)) !core.settings.LoadResult(void, help.ArgHelpSetting) 
{   
    if (!stage.managed) return .success;

    var args = std.ArrayList(core.Symbol).init(allocator);
    defer args.deinit();

    try args.append(try base_dir.realpathAlloc(allocator, stage.path));

    request_channel: {
        const binder = EndpointConfigMap.get(@tagName(.req_rep));
        std.debug.assert(binder != null);
        _ = try binder.?(general_setting.stage_endpoints, &args);
        break :request_channel;
    }
    pub_sub_channel: {
        const binder = EndpointConfigMap.get(@tagName(.pub_sub));
        std.debug.assert(binder != null);
        _ = try binder.?(general_setting.stage_endpoints, &args);
        break :pub_sub_channel;
    }
    log_level: {
        const binder = GeneralConfigMap.get(@tagName(.log_level));
        std.debug.assert(binder != null);
        _ = try binder.?(general_setting, &args);
        break :log_level;
    }
    generate: {
        for (stage.extra_args) |extra| {
            const binder = GenerateConfigMap.get(extra);
            std.debug.assert(binder != null);
            switch (try binder.?(generate_setting, &args)) {
                .help => |help_setting| return .{ .help = help_setting },
                .success => {},
            }

        }
        break :generate;
    }

    var process = std.process.Child.init(try args.toOwnedSlice(), allocator);
    process.stderr_behavior = .Ignore;
    process.stdout_behavior = .Ignore;

    try entries.append(process);

    return .success;
}

fn ConfigBindMap(comptime SettingType: type) type {
    return struct {
        pub const Fn = *const fn (setting: SettingType, args: *std.ArrayList(core.Symbol)) anyerror!core.settings.LoadResult(void, help.ArgHelpSetting) ;
        pub const KV = struct {std.meta.FieldEnum(SettingType), Fn};
        
        pub fn init(comptime kvs: []const KV) std.StaticStringMap(Fn) {
            comptime var map_kvs: [kvs.len](struct {core.Symbol, Fn}) = undefined;
            for (kvs, 0..) |kv, i| {
                map_kvs[i] = .{ @tagName(kv[0]), kv[1] };
            }

            return std.StaticStringMap(Fn).initComptime(&map_kvs);
        }
    };
}

fn findDecl(comptime Id: type, comptime decls: []const clap.Param(Id), comptime id: Id) clap.Param(Id) {
    inline for (decls) |decl| {
        if (decl.id == id) {
            std.debug.assert(decl.names.long != null);
            return decl;
        }
    }

    @compileError(std.fmt.comptimePrint("Not contained CLI arg setting: {}",.{id}));
}

const EndpointConfigMap = 
    ConfigBindMap(core.Endpoints).init(&.{
        .{.req_rep, Binder.General.bindRequestChannel},
        .{.pub_sub, Binder.General.bindSubscribeChannel},
    })
;
const GeneralConfigMap = 
    ConfigBindMap(GeneralSetting).init(&.{
        .{.log_level, Binder.General.bindLogLevel},
    })
;
const GenerateConfigMap = 
    ConfigBindMap(GenerateSetting).init(&.{
        .{.source_dir_path, Binder.Generate.bindSourceDir},
        .{.schema_dir_path, Binder.Generate.bindSchemaDir},
        .{.output_dir_path, Binder.Generate.bindOutputDir},
        .{.watch, Binder.Generate.bindWatchMode},
    })
;

const Binder = struct {
    const General = struct {
        const ArgId = GeneralSetting.StageArgId(.{});
        const decls = ArgId.Decls;
        fn bindRequestChannel(eps: core.Endpoints, args: *std.ArrayList(core.Symbol)) !core.settings.LoadResult(void, help.ArgHelpSetting)  {
            const decl = comptime findDecl(ArgId, decls, .request_channel);
            try args.append("--" ++ decl.names.long.?);
            try args.append(eps.req_rep);

            return .success;
        }
        fn bindSubscribeChannel(eps: core.Endpoints, args: *std.ArrayList(core.Symbol)) !core.settings.LoadResult(void, help.ArgHelpSetting)  {
            const decl = comptime findDecl(ArgId, decls, .subscribe_channel);
            try args.append("--" ++ decl.names.long.?);
            try args.append(eps.pub_sub);

            return .success;
        }
        fn bindLogLevel(setting: GeneralSetting, args: *std.ArrayList(core.Symbol)) !core.settings.LoadResult(void, help.ArgHelpSetting)  {
            const decl = comptime findDecl(ArgId, decls, .log_level);
            try args.append("--" ++ decl.names.long.?);
            try args.append(@tagName(setting.log_level));

            return .success;
        }
    };
    const Generate = struct {
        const ArgId = GenerateSetting.ArgId(.{});
        const decls = ArgId.Decls;
        fn bindSourceDir(setting: GenerateSetting, args: *std.ArrayList(core.Symbol)) !core.settings.LoadResult(void, help.ArgHelpSetting)  {
            const decl = comptime findDecl(ArgId, decls, .source_dir_path);
            try args.append("--" ++ decl.names.long.?);

            for (setting.source_dir_path) |path| {
                try args.append(path);
            }
    
            return .success;
    }
        fn bindSchemaDir(setting: GenerateSetting, args: *std.ArrayList(core.Symbol)) !core.settings.LoadResult(void, help.ArgHelpSetting)  {
            if (setting.schema_dir_path == null) {
                log.warn("Need to specify `--schema-dir` arg", .{});
                return .{
                    .help = .{.tags = &.{ .cmd_generate, .cmd_general }, .command = .generate },
                };
            }

            const decl = comptime findDecl(ArgId, decls, .schema_dir_path);
            try args.append("--" ++ decl.names.long.?);
            try args.append(setting.schema_dir_path.?);

            return .success;
        }
        fn bindOutputDir(setting: GenerateSetting, args: *std.ArrayList(core.Symbol)) !core.settings.LoadResult(void, help.ArgHelpSetting)  {
            const decl = comptime findDecl(ArgId, decls, .output_dir_path);
            try args.append("--" ++ decl.names.long.?);

            try args.append(setting.output_dir_path);

            return .success;
        }
        fn bindWatchMode(setting: GenerateSetting, args: *std.ArrayList(core.Symbol)) !core.settings.LoadResult(void, help.ArgHelpSetting)  {
            if (setting.watch) {
                const decl = comptime findDecl(ArgId, decls, .watch);
                try args.append("--" ++ decl.names.long.?);
            }

            return .success;
        }
    };
};

