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
const SubcommandArgId = @import("../settings/commands/Subcommand.zig").ArgId(.{});

pub fn loadGuest(io: std.Io, allocator: std.mem.Allocator, command: SubcommandArgId, scope: Symbol) !std.MultiArrayList(config_types.Guest) {
    const options: core.configs.supports.FileResolveOptions = .{ .command = @tagName(command), .scope = scope, .category = .configs, .root = config_types.path_candidates };
    return loadGuestInternal(io, allocator, HostGenerateArg.strategies, options);
}

fn loadGuestInternal(io: std.Io, allocator: std.mem.Allocator, strategies: core.configs.types.StageStrategy, options: core.configs.supports.FileResolveOptions) !std.MultiArrayList(config_types.Guest) {
    const file = try core.configs.supports.resolveFileCandidate(io, allocator, options) orelse return error.CofigLoadFailed;
    defer file.close(io);

    return loadGuestConfigFile(io, allocator, file, strategies);
}

fn loadGuestConfigFile(io: std.Io, allocator: std.mem.Allocator, file: std.Io.File, strategies: core.configs.types.StageStrategy) !std.MultiArrayList(config_types.Guest) {
    var buffer: [1024]u8 = undefined;
    var reader = file.reader(io, &buffer);
    const source = try reader.interface.allocRemainingAlignedSentinel(allocator, .unlimited, .@"1", 0);
    defer allocator.free(source);

    return loadGuestConfigSource(allocator, source, strategies, .stderr);
}

fn loadGuestConfigSource(allocator: std.mem.Allocator, source: core.types.SymbolZ, strategies: core.configs.types.StageStrategy, log_style: core.Logger.LogStyle) !std.MultiArrayList(config_types.Guest) {
    var ast = try std.zig.Ast.parse(allocator, source, .zon);
    defer ast.deinit(allocator);
    if (ast.errors.len > 0) {
        if (log_style == .stderr) {
            var buffer: [1024]u8 = undefined;
            const t = std.debug.lockStderr(&buffer).terminal();
            defer std.debug.unlockStderr();

            for (ast.errors) |err| {
                try ast.renderError(err, t.writer);
            }
        }
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

pub fn loadHost(io: std.Io, allocator: std.mem.Allocator, scope: Symbol) !config_types.Host {
    const options: core.configs.supports.FileResolveOptions = .{ .command = "runner", .scope = scope, .category = .configs, .root = config_types.path_candidates };
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

        try std.testing.expectError(error.InvalidConfigFile, loadGuestConfigSource(allocator, source, strategies, .discard));
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

        try std.testing.expectError(error.UnexpectGuestStage, loadGuestConfigSource(allocator, source, strategies, .discard));
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

        var stages = try loadGuestConfigSource(allocator, source, strategies, .discard);
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
        const stages = try loadGuestConfigSource(allocator, source, strategies, .discard);

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
        var stages = try loadGuestConfigSource(allocator, source, strategies, .discard);
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
        var stages = try loadGuestConfigSource(allocator, source, strategies, .discard);
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

        try std.testing.expectError(error.InvalidStageCount, loadGuestConfigSource(allocator, source, strategies, .discard));
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

        try std.testing.expectError(error.InvalidStageCount, loadGuestConfigSource(allocator, source, strategies, .discard));
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

        try std.testing.expectError(error.InvalidStageCount, loadGuestConfigSource(allocator, source, strategies, .discard));
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

        try std.testing.expectError(error.InvalidStageCount, loadGuestConfigSource(allocator, source, strategies, .discard));
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

        try std.testing.expectError(error.InvalidConfigFile, loadGuestConfigSource(allocator, source, strategies, .discard));
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

        try std.testing.expectError(error.InvalidConfigFile, loadGuestConfigSource(allocator, source, strategies, .discard));
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

        try std.testing.expectError(error.InvalidConfigFile, loadGuestConfigSource(allocator, source, strategies, .discard));
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

        var stages = try loadGuestConfigSource(allocator, source, strategies, .discard);
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