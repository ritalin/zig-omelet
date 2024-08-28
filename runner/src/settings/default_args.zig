const std = @import("std");
const core = @import("core");

pub fn Defaults(comptime ArgId: type) type {
    return struct {
        arena: *std.heap.ArenaAllocator,
        map: std.enums.EnumMap(ArgId, Arg),

        const Self = @This();
        const Map = std.enums.EnumMap(ArgId, Arg);

        pub const Arg = union (enum) {
            default: void,
            fixed: []const core.Symbol,

            pub fn tag(self: Arg) std.meta.FieldEnum(Arg) {
                return std.meta.activeTag(self);
            }
        };
        pub const Iterator = Map.Iterator;

        pub fn init(allocator: std.mem.Allocator) !Self {
            const arena = try allocator.create(std.heap.ArenaAllocator);
            arena.* = std.heap.ArenaAllocator.init(allocator);

            return .{
                .arena = arena,
                .map = Map.initFull(.default),
            };
        }

        pub fn loadFromFile(allocator: std.mem.Allocator, file: *std.fs.File) !Self {
            const meta = try file.metadata();

            const contents = try file.readToEndAllocOptions(allocator, meta.size(), null, @alignOf(u8), 0);
            defer allocator.free(contents);

            return loadFromFileInternal(allocator, contents);
        }

        pub fn loadFromFileInternal(allocator: std.mem.Allocator, contents: [:0]const u8) !Self {
            var ast = try std.zig.Ast.parse(allocator, contents, .zon);
            defer ast.deinit(allocator);

            const node_datas = ast.nodes.items(.data);
            var buf: [2]std.zig.Ast.Node.Index = undefined;

            if (ast.fullStructInit(&buf, node_datas[0].lhs)) |node| {
                return loadFromZon(allocator, ast, node);
            }
            else {
                return error.InvalidDefaults;
            }
        }

        pub fn loadFromZon(allocator: std.mem.Allocator, ast: std.zig.Ast, root: std.zig.Ast.full.StructInit) !Self {
            var self = try init(allocator);
            errdefer self.deinit();

            const tmp_allocator = self.arena.allocator();

            const node_tags = ast.nodes.items(.tag);
            var buf: [2]std.zig.Ast.Node.Index = undefined;

            for (root.ast.fields) |field_index| {
                const key = key: {
                    const token_index = ast.firstToken(field_index) - 2;
                    const ident_name = ast.tokenSlice(token_index);
                    break:key std.meta.stringToEnum(ArgId, ident_name) orelse return error.InvalidDefaultsKey;
                };

                if (ast.fullStructInit(&buf, field_index)) |arg_node| {
                    if (arg_node.ast.fields.len == 0) return error.InvalidDefaultsEntry;

                    const arg_field_index = arg_node.ast.fields[0];

                    const tag = tag: {
                        const token_index = ast.firstToken(arg_field_index) - 2;
                        const ident_name = ast.tokenSlice(token_index);
                        break:tag std.meta.stringToEnum(std.meta.FieldEnum(Arg), ident_name) orelse return error.InvalidDefaultsEntryTag;
                    };

                    if (tag == .default) return error.InvalidDefaultsPayload;

                    if (isItemsEmpty(node_tags[arg_field_index])) {
                        self.map.put(key, .{.fixed = &.{}});
                    }
                    else if (ast.fullArrayInit(&buf, arg_field_index)) |values_node| {
                        self.map.put(key, .{.fixed = try loadFixedValues(tmp_allocator, ast, values_node, node_tags)});
                    }
                }
                else if (node_tags[field_index] == .enum_literal) {
                    const token_index = ast.firstToken(field_index);
                    const ident_name = ast.tokenSlice(token_index+1);
                    const tag = std.meta.stringToEnum(std.meta.FieldEnum(Arg), ident_name);
                    if (tag == null) return error.InvalidDefaultsEntryTag;
                    if (tag.? != .default) return error.InvalidDefaultsPayload;
                }
                else {
                    return error.InvalidDefaults;
                }
            }

            return self;
        }

        fn isItemsEmpty(tag: std.zig.Ast.Node.Tag) bool {
            const tags = std.enums.EnumSet(std.zig.Ast.Node.Tag).init(.{
                .struct_init_one = true,
                .struct_init_one_comma = true,
                .struct_init_dot_two = true,
                .struct_init_dot_two_comma = true,
                .struct_init_dot = true,
                .struct_init_dot_comma = true,
                .struct_init = true,
                .struct_init_comma = true,
            });

            return tags.contains(tag);
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
    var expect = try Defaults(ArgId).init(allocator);
    defer expect.deinit();

    const defaults = try Defaults(ArgId).loadFromFileInternal(allocator, contents);
    defer defaults.deinit();

    try std.testing.expectEqualDeep(expect.map, defaults.map);
}

test "All fixed args (subcommand: generate)" {
    const allocator = std.testing.allocator;
    const contents: [:0]const u8 =
        \\.{
        \\    .source_dir = .{.fixed = .{"queries"}},
        \\    .schema_dir = .{.fixed = .{"./schemas"}},
        \\    .include_filter = .{.fixed = .{"queries"}},
        \\    .exclude_filter = .{.fixed = .{"./table", "./indexes"}},
        \\    .output_dir = .{.fixed = .{"./output"}},
        \\    .watch = .{.fixed = .{}}, 
        \\}
    ;
    const ArgId = @import("./commands/Generate.zig").ArgId(.{});
    var expect = try Defaults(ArgId).init(allocator);
    defer expect.deinit();

    expect.map = Defaults(ArgId).Map.init(.{
        .source_dir = .{.fixed = &.{"queries"}},
        .schema_dir = .{.fixed = &.{"./schemas"}},
        .include_filter = .{.fixed = &.{"queries"}},
        .exclude_filter = .{.fixed = &.{"./table", "./indexes"}},
        .output_dir = .{.fixed = &.{"./output"}},
        .watch = .{.fixed = &.{}},
    });

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
        \\.{.output_dir = .fixed }
    ;

    const ArgId = @import("./commands/Generate.zig").ArgId(.{});
    try std.testing.expectError(error.InvalidDefaultsPayload, Defaults(ArgId).loadFromFileInternal(allocator, contents));
}

test "Invalid default#6 (arg value)" {
    const allocator = std.testing.allocator;
    const contents: [:0]const u8 = 
        \\.{.output_dir = .{.fixed = .{12345} } }
    ;

    const ArgId = @import("./commands/Generate.zig").ArgId(.{});
    try std.testing.expectError(error.InvalidDefaultsValue, Defaults(ArgId).loadFromFileInternal(allocator, contents));
}
