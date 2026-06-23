const std = @import("std");
const builtin = @import("builtin");
const core = @import("core");

const Symbol = core.types.Symbol;
const FilePath = core.types.FilePath;

const config_types = @import("./types.zig");

const ArgHelp = @import("../help/ArgHelp.zig");
const default_args = @import("../settings/default_args.zig");
const Defaults = default_args.Defaults;
const DufaultArg = default_args.DufaultArg;

const HostGenerateArg = @import("../settings/commands/Generate.zig");

pub fn loadGuest(io: std.Io, allocator: std.mem.Allocator) !std.MultiArrayList(config_types.Guest) {
    return loadGuestInternal(io, allocator, HostGenerateArg.strategies);
}

fn loadGuestInternal(io: std.Io, allocator: std.mem.Allocator, strategies: core.configs.types.StageStrategy) !std.MultiArrayList(config_types.Guest) {
    const options: core.configs.supports.FileResolveOptions = .{ .command = "generate", .scope = "default", .category = .configs, .root = config_types.path_candidates };
    const file = try core.configs.supports.resolveFileCandidate(io, allocator, options) orelse return error.CofigLoadFailed;
    defer file.close(io);

    return loadGuestConfigFile(io, allocator, file, strategies);
}

fn loadGuestConfigFile(io: std.Io, allocator: std.mem.Allocator, file: std.Io.File, strategies: core.configs.types.StageStrategy) !std.MultiArrayList(config_types.Guest) {
    var buffer: [1024]u8 = undefined;
    var reader = file.reader(io, &buffer);
    const source = try reader.interface.allocRemainingAlignedSentinel(allocator, .unlimited, .@"1", 0);
    defer allocator.free(source);

    return loadGuestConfigSource(allocator, source, strategies);
}

fn loadGuestConfigSource(allocator: std.mem.Allocator, source: core.types.SymbolZ, strategies: core.configs.types.StageStrategy) !std.MultiArrayList(config_types.Guest) {
    var ast = try std.zig.Ast.parse(allocator, source, .zon);
    defer ast.deinit(allocator);
    if (ast.errors.len > 0) {
        // TODO:
        // var buffer: [1024]u8 = undefined;
        // const t = std.debug.lockStderr(&buffer).terminal();
        // defer std.debug.unlockStderr();

        // for (ast.errors) |err| {
        //     try ast.renderError(err, t.writer);
        // }
        return error.InvalidConfigFile;
    }

    var ir = try std.zig.ZonGen.generate(allocator, ast, .{});
    defer ir.deinit(allocator);

    var guests: std.MultiArrayList(config_types.Guest) = .empty;

    const index: std.zig.Zoir.Node.Index = .root;
    const node = index.get(ir);
    if (std.meta.activeTag(node) != .struct_literal) return error.InvalidConfigFile;

    for (node.struct_literal.names, 0..) |name, i| {
        const field_index: std.zig.Zoir.Node.Index = node.struct_literal.vals.at(@intCast(i));
        const ident_name = name.get(ir);

        switch (try core.configs.supports.resolveStageKind(ident_name)) {
            .watch => {
                try loadGuestStages(allocator, &ast, &ir, field_index, .watch, strategies, core.configs.guests.GuestWatch.ArgId(.{}), &guests);
            },
            .extract => {
                try loadGuestStages(allocator, &ast, &ir, field_index, .extract, strategies, core.configs.guests.GuestExtract.ArgId(.{}), &guests);
            },
            .generate => {
                try loadGuestStages(allocator, &ast, &ir, field_index, .generate, strategies, core.configs.guests.GuestGenerate.ArgId(.{}), &guests);
            },
            else => return error.UnsupportedGuestStage,
        }
    }
    return guests;
}

fn loadGuestStages(
    allocator: std.mem.Allocator, 
    ast: *const std.zig.Ast, 
    ir: *const std.zig.Zoir, 
    parent_index: std.zig.Zoir.Node.Index, 
    comptime stage_kind: core.configs.types.StageKind,
    strategies: core.configs.types.StageStrategy,
    comptime GuestArgId: type,
    guests: *std.MultiArrayList(config_types.Guest)) !void
{
    const strategy = strategies.get(stage_kind) orelse return error.UnexpectedGuestStage;
    const parent_node = parent_index.get(ir.*);
    validate_node: {
        switch (parent_node) {
            .struct_literal => break:validate_node,
            .empty_literal => {
                if (core.configs.supports.confirmToStrategy(strategy, 0)) return;
                return error.InvalidStageCount;
            },
            else => return error.CofigLoadFailed,
        }
    }

    if (! core.configs.supports.confirmToStrategy(strategy, parent_node.struct_literal.vals.len)) {
        return error.InvalidStageCount;
    }

    for (parent_node.struct_literal.names, 0..) |ident_name, i| {
        const index = parent_node.struct_literal.vals.at(@intCast(i));
        const stage_name = ident_name.get(ir.*);

        const tmpl: GuestTemplate(GuestArgId) = 
            std.zon.parse.fromZoirNodeAlloc(
                GuestTemplate(GuestArgId), 
                allocator, ast.*, ir.*, index, null, 
                .{}
            )
            catch return error.InvalidConfigFile
        ;

        const name = if ((std.meta.activeTag(tmpl.name) == .values) and (tmpl.name.values.len > 0)) tmpl.name.values[0] else stage_name;
        const enable_managed = if (std.meta.activeTag(tmpl.enable_managed) == .enabled) tmpl.enable_managed.enabled else true;
        const location = if ((std.meta.activeTag(tmpl.location) == .values) and (tmpl.location.values.len > 0)) tmpl.location.values[0] else stage_name;

        const extra_args: config_types.Guest.ExtraArgSet = switch (stage_kind) {
            .watch => .{.watch = tmpl.extra_args},
            .extract => .{.extract = tmpl.extra_args},
            .generate => .{.generate = tmpl.extra_args},
            else => error.UnsupportedGuestStage,
        };

        const guest: config_types.Guest = .{
            .name = try allocator.dupe(u8, name),
            .kind = stage_kind,
            .mode = if (enable_managed) .managed else .daemon,
            .location = try allocator.dupe(u8, location),
            .extra_args = extra_args,
        };
        try guests.append(allocator, guest);
    }
}

fn GuestTemplate(comptime ArgId: type) type {
    return struct {
        name: DufaultArg = .default,
        enable_managed: DufaultArg = .default,
        location: DufaultArg = .default,
        extra_args: config_types.ExtraArg(ArgId) = .{},
    };
}

pub fn loadHost(io: std.Io, allocator: std.mem.Allocator) !config_types.Host {
    const options: core.configs.supports.FileResolveOptions = .{ .command = "runner", .scope = "default", .category = .configs, .root = config_types.path_candidates };
    const file = try core.configs.supports.resolveFileCandidate(io, allocator, options) orelse return hostConfigFromTemplate(.{});
    defer file.close(io);

    var buffer: [1024]u8 = undefined;
    var reader = file.reader(io, &buffer);
    const source = try reader.interface.allocRemainingAlignedSentinel(allocator, .unlimited, .@"1", 0);
    defer allocator.free(source);

    return loadHostConfigSource(allocator, source);
}

fn loadHostConfigSource(allocator: std.mem.Allocator, source: core.types.SymbolZ) !config_types.Host {
    var ast = try std.zig.Ast.parse(allocator, source, .zon);
    defer ast.deinit(allocator);
    if (ast.errors.len > 0) {
        return error.InvalidConfigFile;
    }

    var ir = try std.zig.ZonGen.generate(allocator, ast, .{});
    defer ir.deinit(allocator);

    const tmpl: HostTemplate = 
        std.zon.parse.fromZoirNodeAlloc(
            HostTemplate, 
            allocator, ast, ir, .root, null, 
            .{}
        )
        catch return error.InvalidConfigFile
    ;

    return hostConfigFromTemplate(tmpl);
}

fn hostConfigFromTemplate(tmpl: HostTemplate) config_types.Host {

    const heartbeat_interval = switch(tmpl.heartbeat_interval) {
        .default => std.Io.Duration.fromMilliseconds(DEFAULT_HEARTBEAT_INTERVAL_MS),
        .ns => |ns| std.Io.Duration.fromNanoseconds(ns),
        .us => |us| std.Io.Duration.fromMicroseconds(us),
        .ms => |ms| std.Io.Duration.fromMilliseconds(ms),
    };
    const progress_interval = switch(tmpl.ready_progress_interval) {
        .default => std.Io.Duration.fromMilliseconds(DEFAULT_PROGRESS_INTERVAL_MS),
        .ns => |ns| std.Io.Duration.fromNanoseconds(ns),
        .us => |us| std.Io.Duration.fromMicroseconds(us),
        .ms => |ms| std.Io.Duration.fromMilliseconds(ms),
    };

    return .{
        .heartbeat_interval = heartbeat_interval,
        .heartbeat_limit = if (tmpl.heartbeat_limit) |limit| .{.count = limit} else .unlimited,
        .ready_progress_interval = progress_interval, 
    };
}

pub const Interval = union(enum) {
    default: void,
    // nano-second interval
    ns: i96,
    // micro-second interval
    us: i64,
    // milli-second interval
    ms: i64,
};

const DEFAULT_HEARTBEAT_INTERVAL_MS = 200; // 200 msec
const DEFAULT_PROGRESS_INTERVAL_MS = 10; // 10 msec

const HostTemplate = struct {
    heartbeat_interval: Interval = .default,
    heartbeat_limit: ?u64 = null,
    ready_progress_interval: Interval = .default,
};

// pub fn Stage(comptime _ArgId: type) type {
//     return struct {
//         category: core.configs.GuestKind,
//         location: FilePath,
//         extra_args: ExtraArgSet,
//         managed: bool,

//         pub const ArgId = _ArgId;
//         pub const ExtraArgSet = Defaults(ArgId).Map;
//     };
// }

// pub fn StageLoader(comptime ArgId: type) type {
//     return struct {
//         pub fn load(allocator: std.mem.Allocator, contents: [:0]const u8, strategy_map: core.configs.StageStrategy) ![]const Stage(ArgId) {
//             var ast = try std.zig.Ast.parse(allocator, contents, .zon);
//             defer ast.deinit(allocator);

//             const node_datas = ast.nodes.items(.data);
//             var buf: [2]std.zig.Ast.Node.Index = undefined;

//             if (ast.fullStructInit(&buf, node_datas[0].lhs)) |node| {
//                 return loadFromZon(allocator, ast, node, strategy_map);
//             }
//             else {
//                 return error.InvalidConfig;
//             }
//         }

//         pub fn loadFromFile(allocator: std.mem.Allocator, file: *std.fs.File, strategy_map: core.configs.StageStrategy) ![]const Stage(ArgId) {
//             const meta = try file.metadata();

//             const contents = try file.readToEndAllocOptions(allocator, meta.size(), null, @alignOf(u8), 0);
//             defer allocator.free(contents);

//             return load(allocator, contents, strategy_map);
//         }

//         fn loadFromZon(allocator: std.mem.Allocator, ast: std.zig.Ast, node: std.zig.Ast.full.StructInit, strategy_map: core.configs.StageStrategy) ![]const Stage(ArgId) {
//             var stages = std.ArrayList(Stage(ArgId)).init(allocator);
//             defer stages.deinit();

//             const node_tags = ast.nodes.items(.tag);
//             var buf: [2]std.zig.Ast.Node.Index = undefined;

//             for (node.ast.fields) |cat_index| {
//                 const token_index = ast.firstToken(cat_index) - 2;
//                 const ident_name = ast.tokenSlice(token_index);
//                 const category = std.meta.stringToEnum(core.configs.StageCategory, ident_name) orelse return error.InvalidCategory;
//                 const strategy = strategy_map.get(category).?;

//                 if (ast.fullStructInit(&buf, cat_index)) |cat_node| {
//                     if (isInconsistentCount(strategy, cat_node.ast.fields.len)) return error.InvalidStageCount;

//                     try loadStage(allocator, ast, node_tags, cat_node, category, &stages);
//                 }
//             }

//             return try stages.toOwnedSlice();
//         }

//         fn loadStage(allocator: std.mem.Allocator, ast: std.zig.Ast, tags: []const std.zig.Ast.Node.Tag, node: std.zig.Ast.full.StructInit, category: core.configs.StageCategory, stages: *std.ArrayList(Stage(ArgId))) !void {
//             for (node.ast.fields) |stage_index| {
//                 const stage_name = name: {
//                         const token_index = ast.firstToken(stage_index) - 2;
//                         const name = ast.tokenSlice(token_index);
//                         if (name[0] == '@') {
//                             break:name try std.zig.string_literal.parseAlloc(allocator, name[1..]);
//                         }
//                         else {
//                             break:name try allocator.dupe(u8, name);
//                         }
//                 };

//                 var buf: [2]std.zig.Ast.Node.Index = undefined;

//                 if (ast.fullStructInit(&buf, stage_index)) |stage_node| {
//                     var stage: Stage(ArgId) = .{
//                         .category = category,
//                         .location = undefined,
//                         .extra_args = undefined,
//                         .managed = true,
//                     };
//                     try loadStageInternal(allocator, ast, tags, stage_node, stage_name, &stage);
//                     try stages.append(stage);
//                 }
//             }
//         }

//         fn loadStageInternal(allocator: std.mem.Allocator, ast: std.zig.Ast, tags: []const std.zig.Ast.Node.Tag, node: std.zig.Ast.full.StructInit, stage_name: Symbol, stage: *Stage(ArgId)) !void {
//             var status = std.enums.EnumSet(std.meta.FieldEnum(Stage(ArgId))).initFull();
//             status.remove(.category);

//             for (node.ast.fields) |field_index| {
//                 const token_index = ast.firstToken(field_index) - 2;
//                 const field_name = ast.tokenSlice(token_index);
//                 const field = std.meta.stringToEnum(std.meta.FieldEnum(Stage(ArgId)), field_name) orelse {
//                     if (! builtin.is_test) {
//                         log.err("Unexpected field: {s} in configration file", .{field_name});
//                     }
//                     return error.InvalidConfigFieldKey;
//                 };
//                 if (!status.contains(field)) {}
//                 defer status.remove(field);

//                 switch (field) {
//                     .location => {
//                         stage.location = try resolveStagePath(allocator, ast, tags, field_index, stage_name);
//                     },
//                     .extra_args => {
//                         // var extra_args = std.ArrayList().init(allocator);
//                         // defer extra_args.deinit();
//                         stage.extra_args = try resolveExtraArgs(allocator, ast, field_index);
//                     },
//                     .managed => {
//                         stage.managed = try resolveManaged(allocator, ast, field_index);
//                     },
//                     else => {
//                         return error.InvalidConfigFieldKey;
//                     },
//                 }
//             }

//             status.remove(.managed); // has default value
//             if (status.count() > 0) {
//                 return error.InvalidConfigFieldCount;
//             }
//         }

//         fn resolveStagePath(allocator: std.mem.Allocator, ast: std.zig.Ast, tags: []const std.zig.Ast.Node.Tag, node_index: std.zig.Ast.Node.Index, stage_name: Symbol) !FilePath {
//             const dir_path = path: {
//                 if ((tags[node_index] == .struct_init_dot_two) or (tags[node_index] == .struct_init_dot_two_comma)) {
//                     var buf: [2]std.zig.Ast.Node.Index = undefined;
//                     if (ast.fullStructInit(&buf, node_index)) |path_node| {
//                         std.debug.assert(path_node.ast.fields.len == 1);

//                         const path_node_index = path_node.ast.fields[0];
//                         const path_key_index = ast.firstToken(path_node_index) - 2;
//                         const path_key_token = ast.tokenSlice(path_key_index);
//                         _ = std.meta.stringToEnum(enum {path}, path_key_token) orelse return error.InvalidConfigFieldValue;

//                         const path_value_index = ast.firstToken(path_node_index);
//                         const path_value = ast.tokenSlice(path_value_index);

//                         break:path try std.zig.string_literal.parseAlloc(allocator, path_value);
//                     }
//                     else {
//                         return error.InvalidPathConfig;
//                     }
//                 }
//                 else if (tags[node_index] == .enum_literal) {
//                     const path_key_index = ast.firstToken(node_index) + 1;
//                     const path_key_token = ast.tokenSlice(path_key_index);
//                     _ = std.meta.stringToEnum(enum {default}, path_key_token) orelse return error.InvalidConfigFieldValue;

//                     break:path try std.fs.selfExeDirPathAlloc(allocator);
//                 }
//                 else {
//                     return error.InvalidConfigFieldValue;
//                 }
//             };
//             defer allocator.free(dir_path);
            
//             return std.fs.path.join(allocator, &.{dir_path, stage_name});
//         }

//         fn resolveExtraArgs(allocator: std.mem.Allocator, ast: std.zig.Ast, node_index: std.zig.Ast.Node.Index) !Stage(ArgId).ExtraArgSet {
//             var buf: [2]std.zig.Ast.Node.Index = undefined;
//             if (ast.fullStructInit(&buf, node_index)) |args_node| {
//                 return Defaults(ArgId).loadFromZon(allocator, ast, args_node);
//             }
//             else {
//                 return error.InvalidConfigExtraArgs;
//             }
//         }

//         const StringBollMap = std.StaticStringMap(bool).initComptime(.{
//             .{"false", false},
//             .{"true", true},
//         });

//         fn resolveManaged(allocator: std.mem.Allocator, ast: std.zig.Ast, node_index: std.zig.Ast.Node.Index) !bool {
//             const token_index = ast.firstToken(node_index);
//             const value = try std.ascii.allocLowerString(allocator, ast.tokenSlice(token_index));
//             defer allocator.free(value);

//             return StringBollMap.get(value) orelse return error.InvalidConfigFieldValue;
//         }

//         fn isInconsistentCount(strategy: core.configs.StageStrategy.Value, count: usize) bool {
//             return switch (strategy) {
//                 .one => count != 1,
//                 .many => count == 0,
//                 .optional => count > 1,
//             };
//         }
//     };
// }

// const TestStage = Stage(enum {source_dir_set, filter_set, watch});
// const TestLoader = StageLoader(TestStage.ArgId);

test "load config test" {
    std.testing.refAllDecls(@This());
}

pub const tests_guest = struct {
    const StageStrategy = core.configs.types.StageStrategy;

    test "Toplevel node" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const source: [:0]const u8 = 
            \\[]
        ;
        const strategies = StageStrategy.init(.{});

        try std.testing.expectError(error.InvalidConfigFile, loadGuestConfigSource(allocator, source, strategies));
    }

    test "category node#1 (invalid)" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const source: [:0]const u8 = 
            \\.{
            \\  .stage_unknown = .{},
            \\}
        ;
        const strategies = StageStrategy.init(.{});

        try std.testing.expectError(error.UnexpectGuestStage, loadGuestConfigSource(allocator, source, strategies));
    }

    test "category node#2 (valid)" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const source: [:0]const u8 = 
            \\.{
            \\  .stage_watch = .{},
            \\  .stage_extract = .{},
            \\  .stage_generate = .{},
            \\}
        ;
        const strategies = StageStrategy.init(.{.watch = .optional, .extract = .optional, .generate = .optional});

        var stages = try loadGuestConfigSource(allocator, source, strategies);
        defer stages.deinit(allocator);

        try std.testing.expectEqual(0, stages.len);
    }

    test "stage name token#1" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const source: [:0]const u8 = 
            \\.{
            \\  .stage_generate = .{
            \\      .some_stage = .{
            \\          .name = .default,
            \\          .location = .default,
            \\          .extra_args = .{},
            \\          .enable_managed = .default,
            \\      },
            \\  },
            \\}
        ;
        const strategies = StageStrategy.init(.{.generate = .one});
        const stages = try loadGuestConfigSource(allocator, source, strategies);

        try std.testing.expectEqual(1, stages.len);

        const g = stages.get(0);
        try std.testing.expect(std.mem.endsWith(u8, g.location, "some_stage"));
        try std.testing.expectEqual(.generate, g.kind);
        try std.testing.expectEqual(.managed, g.mode);
        try std.testing.expectEqual(config_types.Guest.ExtraArgSet{.generate = .{.output_dir_path = .default}}, g.extra_args); 
    }

    test "stage name token#2" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const source: [:0]const u8 = 
            \\.{
            \\  .stage_generate = .{
            \\      .@"some-stage" = .{
            \\          .name = .default,
            \\          .location = .default,
            \\          .extra_args = .{},
            \\          .enable_managed = .{.enabled = true},
            \\      }
            \\  }
            \\}
        ;
        const strategies = StageStrategy.init(.{.generate = .one});
        var stages = try loadGuestConfigSource(allocator, source, strategies);
        defer stages.deinit(allocator);

        try std.testing.expectEqual(1, stages.len);

        const g = stages.get(0);
        try std.testing.expect(std.mem.endsWith(u8, g.location, "some-stage"));
        try std.testing.expectEqual(.generate, g.kind);
        try std.testing.expectEqual(config_types.Guest.ExtraArgSet{.generate = .{.output_dir_path = .default}}, g.extra_args); 
        try std.testing.expectEqual(.managed, g.mode);
    }

    test "Missing config field" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const source: [:0]const u8 = 
            \\.{
            \\  .stage_generate = .{
            \\      .@"some-stage" = .{
            \\          .name = .default,
            \\          .extra_args = .{},
            \\      }
            \\  }
            \\}
        ;

        const strategies = StageStrategy.init(.{.generate = .one});
        var stages = try loadGuestConfigSource(allocator, source, strategies);
        defer stages.deinit(allocator);

        try std.testing.expectEqual(1, stages.len);

        const g = stages.get(0);
        try std.testing.expect(std.mem.endsWith(u8, g.location, "some-stage"));
        try std.testing.expectEqual(.generate, g.kind);
        try std.testing.expectEqual(config_types.Guest.ExtraArgSet{.generate = .{.output_dir_path = .default}}, g.extra_args); 
        try std.testing.expectEqual(.managed, g.mode);
    }

    test "Invalid stage strategy#1 (one stage#1)" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const source: [:0]const u8 = 
            \\.{
            \\  .stage_generate = .{
            \\  }
            \\}
        ;
        const strategies = StageStrategy.init(.{.generate = .one});

        try std.testing.expectError(error.InvalidStageCount, loadGuestConfigSource(allocator, source, strategies));
    }

    test "Invalid stage strategy#1 (one stage#2)" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const source: [:0]const u8 = 
            \\.{
            \\  .stage_generate = .{
            \\      .@"some-stage" = .{
            \\          .location = .default,
            \\          .extra_args = .{},
            \\          .enable_managed = .default,
            \\      },
            \\      .@"some-stage-2" = .{
            \\          .location = .default,
            \\          .extra_args = .{},
            \\          .enable_managed = .default,
            \\      },
            \\  }
            \\}
        ;
        const strategies = StageStrategy.init(.{.generate = .one});

        try std.testing.expectError(error.InvalidStageCount, loadGuestConfigSource(allocator, source, strategies));
    }

    test "Invalid stage strategy#2 (many stage)" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const source: [:0]const u8 = 
            \\.{
            \\  .stage_generate = .{
            \\  }
            \\}
        ;
        const strategies = StageStrategy.init(.{.generate = .many});

        try std.testing.expectError(error.InvalidStageCount, loadGuestConfigSource(allocator, source, strategies));
    }

    test "Invalid stage strategy#2 (optional stage)" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const source: [:0]const u8 = 
            \\.{
            \\  .stage_generate = .{
            \\      .@"some-stage" = .{
            \\          .location = .default,
            \\          .extra_args = .{},
            \\          .enable_managed = .default,
            \\      },
            \\      .@"some-stage-2" = .{
            \\          .location = .default,
            \\          .extra_args = .{},
            \\          .enable_managed = .default,
            \\      },
            \\  }
            \\}
        ;
        const strategies = StageStrategy.init(.{.generate = .optional});

        try std.testing.expectError(error.InvalidStageCount, loadGuestConfigSource(allocator, source, strategies));
    }

    test "Invalid location field name" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const source: [:0]const u8 = 
            \\.{
            \\  .stage_generate = .{
            \\      .@"some-stage" = .{
            \\          .location = .default,
            \\          .extra_args = .{},
            \\          .managed = false,
            \\      }
            \\  }
            \\}
        ;
        const strategies = StageStrategy.init(.{.generate = .optional});

        try std.testing.expectError(error.InvalidConfigFile, loadGuestConfigSource(allocator, source, strategies));
    }

    test "Invalid managed field value#1" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const source: [:0]const u8 = 
            \\.{
            \\  .stage_generate = .{
            \\      .@"some-stage" = .{
            \\          .location = .default,
            \\          .extra_args = .{},
            \\          .enable_managed = 0,
            \\      }
            \\  }
            \\}
        ;
        const strategies = StageStrategy.init(.{.generate = .optional});

        try std.testing.expectError(error.InvalidConfigFile, loadGuestConfigSource(allocator, source, strategies));
    }

    test "Invalid location field value#2 (default)" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const source: [:0]const u8 = 
            \\.{
            \\  .stage_generate = .{
            \\      .@"some-stage" = .{
            \\          .location = .{.default = "/path/to"},
            \\          .extra_args = .{},
            \\          .managed = false,
            \\      }
            \\  }
            \\}
        ;
        const strategies = StageStrategy.init(.{.generate = .optional});

        try std.testing.expectError(error.InvalidConfigFile, loadGuestConfigSource(allocator, source, strategies));
    }

    test "Custom extra_args field value" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const source: [:0]const u8 = 
            \\.{
            \\  .stage_watch = .{
            \\      .@"some-stage" = .{
            \\          .extra_args = .{
            \\            .source_dir_set = .{.values = .{"/path/to/source.sql"}}, 
            \\            .include_filter_set = .default, 
            \\            .watch = .{.enabled = false},
            \\          },
            \\      }
            \\  }
            \\}
        ;
        const strategies = StageStrategy.init(.{.watch = .one});

        var stages = try loadGuestConfigSource(allocator, source, strategies);
        defer stages.deinit(allocator);

        const extra_arg_set = stages.get(0).extra_args;
        try std.testing.expectEqual(.watch, std.meta.activeTag(extra_arg_set));

        const extra_args = extra_arg_set.watch;
        try std.testing.expectEqual(.values, std.meta.activeTag(extra_args.source_dir_set));
        try std.testing.expectEqual(1, extra_args.source_dir_set.values.len);
        try std.testing.expectEqualStrings("/path/to/source.sql", extra_args.source_dir_set.values[0]);

        try std.testing.expectEqual(.default, std.meta.activeTag(extra_args.include_filter_set));

        try std.testing.expectEqual(.enabled, std.meta.activeTag(extra_args.watch));
        try std.testing.expectEqual(false, extra_args.watch.enabled);
    }
};

pub const tests_host = struct {
    test "Toplevel node" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const source: [:0]const u8 = 
            \\[]
        ;

        try std.testing.expectError(error.InvalidConfigFile, loadHostConfigSource(allocator, source));
    }

    test "empty config" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const source: [:0]const u8 = 
            \\.{}
        ;

        const config = try loadHostConfigSource(allocator, source);
        try std.testing.expectEqual(std.Io.Duration.fromMilliseconds(DEFAULT_HEARTBEAT_INTERVAL_MS), config.heartbeat_interval);
        try std.testing.expectEqual(.unlimited, config.heartbeat_limit);
        try std.testing.expectEqual(std.Io.Duration.fromMilliseconds(DEFAULT_PROGRESS_INTERVAL_MS), config.ready_progress_interval);
    }

    test "explicit config" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const source: [:0]const u8 = 
            \\.{
            \\    .heartbeat_interval = .{.ms = 100},
            \\    .heartbeat_limit = 16,
            \\    .ready_progress_interval = .{.us = 5000},
            \\}
        ;

        const config = try loadHostConfigSource(allocator, source);
        try std.testing.expectEqual(std.Io.Duration.fromMilliseconds(100), config.heartbeat_interval);
        try std.testing.expectEqual(@FieldType(config_types.Host, "heartbeat_limit"){.count = 16}, config.heartbeat_limit);
        try std.testing.expectEqual(std.Io.Duration.fromMilliseconds(5), config.ready_progress_interval);
    }

    test "Invalid location field name" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const source: [:0]const u8 = 
            \\.{
            \\  .interval = .{ .ms = 20 },
            \\}
        ;

        try std.testing.expectError(error.InvalidConfigFile, loadHostConfigSource(allocator, source));
    }

    test "Invalid location field value" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const source: [:0]const u8 = 
            \\.{
            \\  .heartbeat_interval = 20,
            \\}
        ;

        try std.testing.expectError(error.InvalidConfigFile, loadHostConfigSource(allocator, source));
    }
};