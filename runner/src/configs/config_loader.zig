const std = @import("std");
const core = @import("core");

const Symbol = core.Symbol;
const FilePath = core.FilePath;

const log = core.Logger.SystemDirect(@import("build_options").app_context);

const help = @import("../settings/help.zig");
const Defaults = @import("../settings/default_args.zig").Defaults;

pub const ConfigPathCandidate: core.configs.ConfigFileCandidates = .{
    .current_dir = ".omelet/configs",
    .home_dir = ".omelet/configs",
    .executable_dir = "configs",
};

pub fn Stage(comptime _ArgId: type) type {
    return struct {
        category: core.configs.StageCategory,
        location: FilePath,
        extra_args: ExtraArgSet,
        managed: bool,

        pub const ArgId = _ArgId;
        pub const ExtraArgSet = Defaults(ArgId).Map;
    };
}

pub fn StageLoader(comptime ArgId: type) type {
    return struct {
        pub fn load(allocator: std.mem.Allocator, contents: [:0]const u8, strategy_map: core.configs.StageStrategy) ![]const Stage(ArgId) {
            var ast = try std.zig.Ast.parse(allocator, contents, .zon);
            defer ast.deinit(allocator);

            const node_datas = ast.nodes.items(.data);
            var buf: [2]std.zig.Ast.Node.Index = undefined;

            if (ast.fullStructInit(&buf, node_datas[0].lhs)) |node| {
                return loadFromZon(allocator, ast, node, strategy_map);
            }
            else {
                return error.InvalidConfig;
            }
        }

        pub fn loadFromFile(allocator: std.mem.Allocator, file: *std.fs.File, strategy_map: core.configs.StageStrategy) ![]const Stage(ArgId) {
            const meta = try file.metadata();

            const contents = try file.readToEndAllocOptions(allocator, meta.size(), null, @alignOf(u8), 0);
            defer allocator.free(contents);

            return load(allocator, contents, strategy_map);
        }

        fn loadFromZon(allocator: std.mem.Allocator, ast: std.zig.Ast, node: std.zig.Ast.full.StructInit, strategy_map: core.configs.StageStrategy) ![]const Stage(ArgId) {
            var stages = std.ArrayList(Stage(ArgId)).init(allocator);
            defer stages.deinit();

            const node_tags = ast.nodes.items(.tag);
            var buf: [2]std.zig.Ast.Node.Index = undefined;

            for (node.ast.fields) |cat_index| {
                const token_index = ast.firstToken(cat_index) - 2;
                const ident_name = ast.tokenSlice(token_index);
                const category = std.meta.stringToEnum(core.configs.StageCategory, ident_name) orelse return error.InvalidCategory;
                const strategy = strategy_map.get(category).?;

                if (ast.fullStructInit(&buf, cat_index)) |cat_node| {
                    if (isInconsistentCount(strategy, cat_node.ast.fields.len)) return error.InvalidStageCount;

                    try loadStage(allocator, ast, node_tags, cat_node, category, &stages);
                }
            }

            return try stages.toOwnedSlice();
        }

        fn loadStage(allocator: std.mem.Allocator, ast: std.zig.Ast, tags: []const std.zig.Ast.Node.Tag, node: std.zig.Ast.full.StructInit, category: core.configs.StageCategory, stages: *std.ArrayList(Stage(ArgId))) !void {
            for (node.ast.fields) |stage_index| {
                const stage_name = name: {
                        const token_index = ast.firstToken(stage_index) - 2;
                        const name = ast.tokenSlice(token_index);
                        if (name[0] == '@') {
                            break:name try std.zig.string_literal.parseAlloc(allocator, name[1..]);
                        }
                        else {
                            break:name try allocator.dupe(u8, name);
                        }
                };

                var buf: [2]std.zig.Ast.Node.Index = undefined;

                if (ast.fullStructInit(&buf, stage_index)) |stage_node| {
                    var stage: Stage(ArgId) = .{
                        .category = category,
                        .location = undefined,
                        .extra_args = undefined,
                        .managed = true,
                    };
                    try loadStageInternal(allocator, ast, tags, stage_node, stage_name, &stage);
                    try stages.append(stage);
                }
            }
        }

        fn loadStageInternal(allocator: std.mem.Allocator, ast: std.zig.Ast, tags: []const std.zig.Ast.Node.Tag, node: std.zig.Ast.full.StructInit, stage_name: Symbol, stage: *Stage(ArgId)) !void {
            var status = std.enums.EnumSet(std.meta.FieldEnum(Stage(ArgId))).initFull();
            status.remove(.managed); // has default value

            for (node.ast.fields) |field_index| {
                const token_index = ast.firstToken(field_index) - 2;
                const field_name = ast.tokenSlice(token_index);
                const field = std.meta.stringToEnum(std.meta.FieldEnum(Stage(ArgId)), field_name) orelse {
                    log.err("Unexpected field: {s} in configration file", .{field_name});
                    return error.InvalidConfigFieldKey;
                };
                defer status.remove(field);

                switch (field) {
                    .location => {
                        stage.location = try resolveStagePath(allocator, ast, tags, field_index, stage_name);
                    },
                    .extra_args => {
                        // var extra_args = std.ArrayList().init(allocator);
                        // defer extra_args.deinit();
                        stage.extra_args = try resolveExtraArgs(allocator, ast, field_index);
                    },
                    .managed => {
                        stage.managed = try resolveManaged(allocator, ast, field_index);
                    },
                    else => {
                        return error.InvalidConfigFieldKey;
                    },
                }
            }

            if (status.count() > 0) {
                // TODO ERROR,
            }
        }

        fn resolveStagePath(allocator: std.mem.Allocator, ast: std.zig.Ast, tags: []const std.zig.Ast.Node.Tag, node_index: std.zig.Ast.Node.Index, stage_name: Symbol) !FilePath {
            const dir_path = path: {
                if ((tags[node_index] == .struct_init_dot_two) or (tags[node_index] == .struct_init_dot_two_comma)) {
                    var buf: [2]std.zig.Ast.Node.Index = undefined;
                    if (ast.fullStructInit(&buf, node_index)) |path_node| {
                        std.debug.assert(path_node.ast.fields.len == 1);

                        const path_node_index = path_node.ast.fields[0];
                        const path_key_index = ast.firstToken(path_node_index) - 2;
                        const path_key_token = ast.tokenSlice(path_key_index);
                        _ = std.meta.stringToEnum(enum {path}, path_key_token) orelse return error.InvalidConfigFieldValue;

                        const path_value_index = ast.firstToken(path_node_index);
                        const path_value = ast.tokenSlice(path_value_index);

                        break:path try std.zig.string_literal.parseAlloc(allocator, path_value);
                    }
                    else {
                        return error.InvalidPathConfig;
                    }
                }
                else if (tags[node_index] == .enum_literal) {
                    const path_key_index = ast.firstToken(node_index) + 1;
                    const path_key_token = ast.tokenSlice(path_key_index);
                    _ = std.meta.stringToEnum(enum {default}, path_key_token) orelse return error.InvalidConfigFieldValue;

                    break:path try std.fs.selfExeDirPathAlloc(allocator);
                }
                else {
                    return error.InvalidConfigFieldValue;
                }
            };
            defer allocator.free(dir_path);
            
            return std.fs.path.join(allocator, &.{dir_path, stage_name});
        }

        fn resolveExtraArgs(allocator: std.mem.Allocator, ast: std.zig.Ast, node_index: std.zig.Ast.Node.Index) !Stage(ArgId).ExtraArgSet {
            var buf: [2]std.zig.Ast.Node.Index = undefined;
            if (ast.fullStructInit(&buf, node_index)) |args_node| {
                return Defaults(ArgId).loadFromZon(allocator, ast, args_node);
            }
            else {
                return error.InvalidConfigExtraArgs;
            }
        }

        const StringBollMap = std.StaticStringMap(bool).initComptime(.{
            .{"false", false},
            .{"true", true},
        });

        fn resolveManaged(allocator: std.mem.Allocator, ast: std.zig.Ast, node_index: std.zig.Ast.Node.Index) !bool {
            const token_index = ast.firstToken(node_index);
            const value = try std.ascii.allocLowerString(allocator, ast.tokenSlice(token_index));
            defer allocator.free(value);

            return StringBollMap.get(value) orelse return error.InvalidConfigFieldValue;
        }

        fn isInconsistentCount(strategy: core.configs.StageStrategy.Value, count: usize) bool {
            return switch (strategy) {
                .one => count != 1,
                .many => count == 0,
                .optional => count > 1,
            };
        }
    };
}

const TestStage = Stage(enum {source_dir_set, filter_set, watch});
const TestLoader = StageLoader(TestStage.ArgId);

test "Toplevel node" {
    const source: [:0]const u8 = 
        \\[]
    ;
    const strategies = core.configs.StageStrategy.init(.{});

    const allocator = std.testing.allocator;
    try std.testing.expectError(error.InvalidConfig, TestLoader.load(allocator, source, strategies));
}

test "category node#1 (invalid)" {
    const source: [:0]const u8 = 
        \\.{
        \\  .stage_unknown = .{},
        \\}
    ;
    const strategies = core.configs.StageStrategy.init(.{});

    const allocator = std.testing.allocator;
    try std.testing.expectError(error.InvalidCategory, TestLoader.load(allocator, source, strategies));
}

test "category node#2 (valid)" {
    const source: [:0]const u8 = 
        \\.{
        \\  .stage_watch = .{},
        \\  .stage_extract = .{},
        \\  .stage_generate = .{},
        \\}
    ;
    const strategies = core.configs.StageStrategy.init(.{.stage_watch = .optional, .stage_extract = .optional, .stage_generate = .optional});

    const allocator = std.testing.allocator;
    const stages = try TestLoader.load(allocator, source, strategies);
    defer allocator.free(stages);

    try std.testing.expectEqual(0, stages.len);
}

test "stage name token#1" {
    const source: [:0]const u8 = 
        \\.{
        \\  .stage_generate = .{
        \\      .some_stage = .{
        \\          .location = .default,
        \\          .extra_args = .{},
        \\          .managed = true,
        \\      },
        \\  },
        \\},
    ;
    const strategies = core.configs.StageStrategy.init(.{.stage_generate = .one});

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const stages = try TestLoader.load(allocator, source, strategies);

    try std.testing.expectEqual(1, stages.len);
    try std.testing.expect(std.mem.endsWith(u8, stages[0].location, "some_stage"));
    try std.testing.expectEqual(.stage_generate, stages[0].category);
    try std.testing.expectEqual(0, stages[0].extra_args.count()); 
    try std.testing.expectEqual(true, stages[0].managed);
}

test "stage name token#2" {
    const source: [:0]const u8 = 
        \\.{
        \\  .stage_generate = .{
        \\      .@"some-stage" = .{
        \\          .location = .default,
        \\          .extra_args = .{},
        \\          .managed = true,
        \\      }
        \\  }
        \\},
    ;
    const strategies = core.configs.StageStrategy.init(.{.stage_generate = .one});

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const stages = try TestLoader.load(allocator, source, strategies);

    try std.testing.expectEqual(1, stages.len);
    try std.testing.expect(std.mem.endsWith(u8, stages[0].location, "some-stage"));
    try std.testing.expectEqual(.stage_generate, stages[0].category);
    try std.testing.expectEqual(0, stages[0].extra_args.count()); 
    try std.testing.expectEqual(true, stages[0].managed);
}

test "Invalid stage strategy#1 (one stage#1)" {
    const source: [:0]const u8 = 
        \\.{
        \\  .stage_generate = .{
        \\  }
        \\},
    ;
    const strategies = core.configs.StageStrategy.init(.{.stage_generate = .one});

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try std.testing.expectError(error.InvalidStageCount, TestLoader.load(allocator, source, strategies));
}

test "Invalid stage strategy#1 (one stage#2)" {
    const source: [:0]const u8 = 
        \\.{
        \\  .stage_generate = .{
        \\      .@"some-stage" = .{
        \\          .location = .default,
        \\          .extra_args = .{},
        \\          .managed = true,
        \\      },
        \\      .@"some-stage-2" = .{
        \\          .location = .default,
        \\          .extra_args = .{},
        \\          .managed = true,
        \\      },
        \\  }
        \\},
    ;
    const strategies = core.configs.StageStrategy.init(.{.stage_generate = .one});

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try std.testing.expectError(error.InvalidStageCount, TestLoader.load(allocator, source, strategies));
}

test "Invalid stage strategy#2 (many stage)" {
    const source: [:0]const u8 = 
        \\.{
        \\  .stage_generate = .{
        \\  }
        \\},
    ;
    const strategies = core.configs.StageStrategy.init(.{.stage_generate = .many});

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try std.testing.expectError(error.InvalidStageCount, TestLoader.load(allocator, source, strategies));
}

test "Invalid stage strategy#2 (optional stage)" {
    const source: [:0]const u8 = 
        \\.{
        \\  .stage_generate = .{
        \\      .@"some-stage" = .{
        \\          .location = .default,
        \\          .extra_args = .{},
        \\          .managed = true,
        \\      },
        \\      .@"some-stage-2" = .{
        \\          .location = .default,
        \\          .extra_args = .{},
        \\          .managed = true,
        \\      },
        \\  }
        \\},
    ;
    const strategies = core.configs.StageStrategy.init(.{.stage_generate = .optional});

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try std.testing.expectError(error.InvalidStageCount, TestLoader.load(allocator, source, strategies));
}

test "Invalid stage field#1" {
    const source: [:0]const u8 = 
        \\.{
        \\  .stage_generate = .{
        \\      .@"some-stage" = .{
        \\          .location = .default,
        \\          .extra = .{},
        \\          .managed = true,
        \\      }
        \\  }
        \\},
    ;
    const strategies = core.configs.StageStrategy.init(.{.stage_generate = .optional});

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try std.testing.expectError(error.InvalidConfigFieldKey, TestLoader.load(allocator, source, strategies));
}

test "Invalid stage field#2" {
    const source: [:0]const u8 = 
        \\.{
        \\  .stage_generate = .{
        \\      .@"some-stage" = .{
        \\          .category = .stage_generate,
        \\          .location = .default,
        \\          .extra_args = .{},
        \\          .managed = true,
        \\      }
        \\  }
        \\},
    ;
    const strategies = core.configs.StageStrategy.init(.{.stage_generate = .optional});

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try std.testing.expectError(error.InvalidConfigFieldKey, TestLoader.load(allocator, source, strategies));
}

test "Invalid managed field value#2" {
    const source: [:0]const u8 = 
        \\.{
        \\  .stage_generate = .{
        \\      .@"some-stage" = .{
        \\          .location = .default,
        \\          .extra_args = .{},
        \\          .managed = 0,
        \\      }
        \\  }
        \\},
    ;
    const strategies = core.configs.StageStrategy.init(.{.stage_generate = .optional});

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try std.testing.expectError(error.InvalidConfigFieldValue, TestLoader.load(allocator, source, strategies));
}

test "Invalid location field value#1 (path)" {
    const source: [:0]const u8 = 
        \\.{
        \\  .stage_generate = .{
        \\      .@"some-stage" = .{
        \\          .location = .path,
        \\          .extra_args = .{},
        \\          .managed = false,
        \\      }
        \\  }
        \\},
    ;
    const strategies = core.configs.StageStrategy.init(.{.stage_generate = .optional});

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try std.testing.expectError(error.InvalidConfigFieldValue, TestLoader.load(allocator, source, strategies));
}

test "Invalid location field value#2 (default)" {
    const source: [:0]const u8 = 
        \\.{
        \\  .stage_generate = .{
        \\      .@"some-stage" = .{
        \\          .location = .{.default = "/path/to"},
        \\          .extra_args = .{},
        \\          .managed = false,
        \\      }
        \\  }
        \\},
    ;
    const strategies = core.configs.StageStrategy.init(.{.stage_generate = .optional});

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try std.testing.expectError(error.InvalidConfigFieldValue, TestLoader.load(allocator, source, strategies));
}

test "Custom location field value (path)" {
    const source: [:0]const u8 = 
        \\.{
        \\  .stage_generate = .{
        \\      .@"some-stage" = .{
        \\          .location = .{.path = "/path/to"},
        \\          .extra_args = .{},
        \\          .managed = false,
        \\      }
        \\  }
        \\},
    ;
    const strategies = core.configs.StageStrategy.init(.{.stage_generate = .optional});

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const stages = try TestLoader.load(allocator, source, strategies);

    try std.testing.expectEqual(1, stages.len);
    try std.testing.expectEqualStrings("/path/to/some-stage", stages[0].location);
    try std.testing.expectEqual(.stage_generate, stages[0].category);
    try std.testing.expectEqual(0, stages[0].extra_args.count()); 
    try std.testing.expectEqual(false, stages[0].managed);
}

test "Custom extra_args field value (all default)" {
    const source: [:0]const u8 = 
        \\.{
        \\  .stage_watch = .{
        \\      .@"some-stage" = .{
        \\          .location = .{.path = "/path/to"},
        \\          .extra_args = .{
        \\            .source_dir_set = .default, 
        \\            .filter_set = .default, 
        \\            .watch = .default,
        \\          },
        \\          .managed = true,
        \\      }
        \\  }
        \\},
    ;
    const strategies = core.configs.StageStrategy.init(.{.stage_watch = .one});

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const stages = try TestLoader.load(allocator, source, strategies);

    try std.testing.expectEqual(1, stages.len);
    try std.testing.expectEqualStrings("/path/to/some-stage", stages[0].location);
    try std.testing.expectEqual(.stage_watch, stages[0].category);
    try std.testing.expectEqual(3, stages[0].extra_args.count());

    try std.testing.expectEqual(true, stages[0].managed);
}

    // var ast = try std.zig.Ast.parse(allocator, source, .zon);
    // defer ast.deinit(allocator);

    // const tags = ast.nodes.items(.tag);
    // const main_tokens = ast.nodes.items(.main_token);
    // const data = ast.nodes.items(.data);
    // const extras = ast.extra_data;
    // const tokens = ast.tokens;

    // std.debug.print("*** tags\n", .{});
    // for (tags, 0..) |tag, i| {
    //     std.debug.print("[{}] {}\n", .{i, tag});
    // }
    // std.debug.print("*** main-token\n", .{});
    // for (main_tokens, 0..) |tk, i| {
    //     std.debug.print("[{}] {}\n", .{i, tk});
    // }
    // std.debug.print("*** data\n", .{});
    // for (data, 0..) |d, i| {
    //     std.debug.print("[{}] {}\n", .{i, d});
    // }
    // std.debug.print("*** extras\n", .{});
    // for (extras, 0..) |ex, i| {
    //     std.debug.print("[{}] {}\n", .{i, ex});
    // }
    // std.debug.print("*** tokens\n", .{});
    // for (0..tokens.len) |i| {
    //     const tk = tokens.get(i);
    //     std.debug.print("[{}] tk: {}, at: {} ({c})\n", .{i, tk.tag, tk.start, source[tk.start]});
    // }