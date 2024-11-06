const std = @import("std");
const core = @import("core");

const log = core.Logger.SystemDirect(@import("build_options").app_context);

pub const DufaultArg = union (enum) {
    default: void,
    values: []const core.Symbol,
    enabled: bool,

    pub fn tag(self: DufaultArg) std.meta.FieldEnum(DufaultArg) {
        return std.meta.activeTag(self);
    }
};

pub fn Defaults(comptime ArgId: type) type {
    return struct {
        arena: *std.heap.ArenaAllocator,
        map: Self.Map,

        const Self = @This();

        pub const Map = std.enums.EnumMap(ArgId, Arg);
        pub const Arg = DufaultArg;

        pub const Iterator = Self.Map.Iterator;

        pub fn init(allocator: std.mem.Allocator, map: Self.Map) !Self {
            const arena = try allocator.create(std.heap.ArenaAllocator);
            arena.* = std.heap.ArenaAllocator.init(allocator);
            
            return .{
                .arena = arena,
                .map = map,
            };
        }

        pub fn loadFromFile(allocator: std.mem.Allocator, file: *std.fs.File) !Self {
            const meta = try file.metadata();

            const contents = try file.readToEndAllocOptions(allocator, meta.size(), null, @alignOf(u8), 0);
            defer allocator.free(contents);

            return loadFromFileInternal(allocator, contents);
        }

        fn loadFromFileInternal(allocator: std.mem.Allocator, contents: [:0]const u8) !Self {
            var ast = try std.zig.Ast.parse(allocator, contents, .zon);
            defer ast.deinit(allocator);

            const node_datas = ast.nodes.items(.data);
            var buf: [2]std.zig.Ast.Node.Index = undefined;

            if (ast.fullStructInit(&buf, node_datas[0].lhs)) |node| {
                const arena = try allocator.create(std.heap.ArenaAllocator);
                arena.* = std.heap.ArenaAllocator.init(allocator);
                errdefer {
                    arena.deinit();
                    allocator.destroy(arena);
                }

                return .{
                    .arena = arena,
                    .map = try loadFromZon(arena.allocator(), ast, node),
                };
            }
            else {
                return error.InvalidDefaults;
            }
        }

        pub fn loadFromZon(allocator: std.mem.Allocator, ast: std.zig.Ast, root: std.zig.Ast.full.StructInit) !Self.Map {
            var map = Self.Map{};

            const node_tags = ast.nodes.items(.tag);
            var buf: [2]std.zig.Ast.Node.Index = undefined;

            for (root.ast.fields) |field_index| {
                const key = key: {
                    const token_index = ast.firstToken(field_index) - 2;
                    const ident_name = ast.tokenSlice(token_index);
                    break:key std.meta.stringToEnum(ArgId, ident_name) orelse {
                        log.err("default key not found: {s}", .{ident_name});
                        return error.InvalidDefaultsKey;
                    };
                };

                if (ast.fullStructInit(&buf, field_index)) |arg_node| {
                    if (arg_node.ast.fields.len == 0) {
                        log.err("At least, it needs default entry tag: {s}", .{@tagName(key)});
                        return error.InvalidDefaultsEntry;
                    }

                    const arg_field_index = arg_node.ast.fields[0];

                    const tag = tag: {
                        const token_index = ast.firstToken(arg_field_index) - 2;
                        const ident_name = ast.tokenSlice(token_index);
                        break:tag std.meta.stringToEnum(std.meta.FieldEnum(Arg), ident_name) orelse {
                            log.err("default entry tag not found: {s}", .{ident_name});
                            return error.InvalidDefaultsEntryTag;
                        };
                    };

                    if (tag == .default) {
                        log.err("Invalid payload tag: {s}", .{@tagName(tag)});
                        return error.InvalidDefaultsPayload;
                    }
                    else if (tag == .enabled) {
                        map.put(key, .{.enabled = try loadFixedEnabled(allocator, ast, arg_field_index)});
                    }
                    else if (core.configs.isItemsEmpty(node_tags[arg_field_index])) {
                        map.put(key, .{.values = &.{}});
                    }
                    else if (ast.fullArrayInit(&buf, arg_field_index)) |values_node| {
                        map.put(key, .{.values = try loadFixedValues(allocator, ast, values_node, node_tags)});
                    }
                }
                else if (node_tags[field_index] == .enum_literal) {
                    const token_index = ast.firstToken(field_index);
                    const ident_name = ast.tokenSlice(token_index+1);
                    const tag = std.meta.stringToEnum(std.meta.FieldEnum(Arg), ident_name);
                    if (tag == null) {
                        log.err("default entry tag not found: {s}", .{ident_name});
                        return error.InvalidDefaultsEntryTag;
                    }

                    switch (tag.?) {
                        .default => {
                            map.put(key, .default);
                        },
                        else => {
                            log.err("Invalid payload tag: {s}", .{@tagName(tag.?)});
                            return error.InvalidDefaultsPayload;
                        }
                    }
                }
                else {
                    log.err("invalid default entry (key: {s})", .{@tagName(key)});
                    return error.InvalidDefaults;
                }
            }

            return map;
        }

        fn loadFixedValues(allocator: std.mem.Allocator, ast: std.zig.Ast, node: std.zig.Ast.full.ArrayInit, tags: []const std.zig.Ast.Node.Tag) ![]const core.Symbol {
            const values = try allocator.alloc(core.Symbol, node.ast.elements.len);
            for (node.ast.elements, 0..) |value_index, i| {
                if (tags[value_index] != .string_literal) return error.InvalidDefaultsValue; 

                const token_index = ast.firstToken(value_index);
                values[i] = try std.zig.string_literal.parseAlloc(allocator, ast.tokenSlice(token_index));
            }

            return values;
        }

        const StringToBoolMap = std.StaticStringMap(bool).initComptime(.{
            .{"false", false},
            .{"true", true},
        });

        fn loadFixedEnabled(allocator: std.mem.Allocator, ast: std.zig.Ast, node_index: std.zig.Ast.Node.Index) !bool {
            const token_index = ast.firstToken(node_index);
            const value = try std.ascii.allocLowerString(allocator, ast.tokenSlice(token_index));
            defer allocator.free(value);

            return StringToBoolMap.get(value) orelse return error.InvalidDefaultsValue;
        }

        pub fn deinit(self: Self) void {
            self.arena.deinit();
            self.arena.child_allocator.destroy(self.arena);
        }

        pub fn iterator(self: *Self) Iterator {
            return self.map.iterator();
        }
    };
}

test "All default tag (subcommand: generate)" {
    const allocator = std.testing.allocator;

    const contents: [:0]const u8 =
        \\.{
        \\    .source_dir = .default,
        \\    .schema_dir = .default,
        \\    .include_filter = .default,
        \\    .exclude_filter = .default,
        \\    .output_dir = .default,
        \\    .watch = .default, 
        \\}
    ;
    const ArgId = @import("./commands/Generate.zig").ArgId(.{});
    var expect = try Defaults(ArgId).init(allocator, Defaults(ArgId).Map.initFull(.default));
    defer expect.deinit();

    const defaults = try Defaults(ArgId).loadFromFileInternal(allocator, contents);
    defer defaults.deinit();

    try std.testing.expectEqualDeep(expect.map, defaults.map);
}

test "All fixed args (subcommand: generate)" {
    const allocator = std.testing.allocator;
    const contents: [:0]const u8 =
        \\.{
        \\    .source_dir = .{.values = .{"queries"}},
        \\    .schema_dir = .{.values = .{"./schemas"}},
        \\    .include_filter = .{.values = .{"queries"}},
        \\    .exclude_filter = .{.values = .{"./table", "./indexes"}},
        \\    .output_dir = .{.values = .{"./output"}},
        \\    .watch = .{.enabled = true}, 
        \\}
    ;
    const ArgId = @import("./commands/Generate.zig").ArgId(.{});
    var expect = try Defaults(ArgId).init(allocator, Defaults(ArgId).Map.init(.{
        .source_dir = .{.values = &.{"queries"}},
        .schema_dir = .{.values = &.{"./schemas"}},
        .include_filter = .{.values = &.{"queries"}},
        .exclude_filter = .{.values = &.{"./table", "./indexes"}},
        .output_dir = .{.values = &.{"./output"}},
        .watch = .{.enabled = true},
    }));
    defer expect.deinit();

    const defaults = try Defaults(ArgId).loadFromFileInternal(allocator, contents);
    defer defaults.deinit();

    try std.testing.expectEqualDeep(expect.map, defaults.map);
}

test "Invalid default#1 (root node)" {
    const allocator = std.testing.allocator;
    const contents: [:0]const u8 = "[]";

    const ArgId = @import("./commands/Generate.zig").ArgId(.{});
    try std.testing.expectError(error.InvalidDefaults, Defaults(ArgId).loadFromFileInternal(allocator, contents));
}

test "Invalid default#2 (unknown key)" {
    const allocator = std.testing.allocator;
    const contents: [:0]const u8 = ".{.qwerty = .default}";

    const ArgId = @import("./commands/Generate.zig").ArgId(.{});
    try std.testing.expectError(error.InvalidDefaultsKey, Defaults(ArgId).loadFromFileInternal(allocator, contents));
}

test "Invalid default#3 (unknown arg tag#1)" {
    const allocator = std.testing.allocator;
    const contents: [:0]const u8 = ".{.output_dir = .xyz}";

    const ArgId = @import("./commands/Generate.zig").ArgId(.{});
    try std.testing.expectError(error.InvalidDefaultsEntryTag, Defaults(ArgId).loadFromFileInternal(allocator, contents));
}

test "Invalid default#4 (unknown arg tag#2)" {
    const allocator = std.testing.allocator;
    const contents: [:0]const u8 = ".{.output_dir = .{.xyz = null}}";

    const ArgId = @import("./commands/Generate.zig").ArgId(.{});
    try std.testing.expectError(error.InvalidDefaultsEntryTag, Defaults(ArgId).loadFromFileInternal(allocator, contents));
}

test "Invalid default#5 (arg payload#1)" {
    const allocator = std.testing.allocator;
    const contents: [:0]const u8 = 
        \\.{.output_dir = .{.default = .{""} } }
    ;

    const ArgId = @import("./commands/Generate.zig").ArgId(.{});
    try std.testing.expectError(error.InvalidDefaultsPayload, Defaults(ArgId).loadFromFileInternal(allocator, contents));
}

test "Invalid default#6 (arg payload#2)" {
    const allocator = std.testing.allocator;
    const contents: [:0]const u8 = 
        \\.{.output_dir = .values }
    ;

    const ArgId = @import("./commands/Generate.zig").ArgId(.{});
    try std.testing.expectError(error.InvalidDefaultsPayload, Defaults(ArgId).loadFromFileInternal(allocator, contents));
}

test "Invalid default#6 (arg value)" {
    const allocator = std.testing.allocator;
    const contents: [:0]const u8 = 
        \\.{.output_dir = .{.values = .{12345} } }
    ;

    const ArgId = @import("./commands/Generate.zig").ArgId(.{});
    try std.testing.expectError(error.InvalidDefaultsValue, Defaults(ArgId).loadFromFileInternal(allocator, contents));
}
