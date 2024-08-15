const std = @import("std");
const core = @import("core");
const Symbol = core.Symbol;
const FilePath = core.FilePath;

const Self = @This();
const CodeBuilder = Self;

const ResultEntryMap = std.enums.EnumMap(std.meta.FieldEnum(Target), Symbol);

const Namespace = "Sql";
const ToArrayFn = "export const toArray = (parameter: Parameter) => Object.values(parameter)";

const IdentifierFormatter = @import("./IdentifierFormatter.zig");

allocator: std.mem.Allocator,
output_dir: std.fs.Dir,
entries: ResultEntryMap,

pub fn init(allocator: std.mem.Allocator, prefix_dir_path: core.FilePath, dest_dir_path: FilePath) !Self {
    const dir = dir: {
        var parent_dir = try std.fs.cwd().makeOpenPath(prefix_dir_path, .{});
        defer parent_dir.close();

        const path = try toPascalCasePath(allocator, dest_dir_path);
        defer allocator.free(path);
        break :dir try parent_dir.makeOpenPath(path, .{});
    };

    return .{
        .allocator = allocator,
        .output_dir = dir,
        .entries = ResultEntryMap{},
    };
}

fn toPascalCasePath(allocator: std.mem.Allocator, file_path: FilePath) !FilePath {
    var iter = std.mem.splitBackwards(u8, file_path, std.fs.path.sep_str);

    const last = try IdentifierFormatter.format(allocator, iter.first(), .pascal_case);
    defer allocator.free(last);

    const rest = iter.rest();

    return if (rest.len > 0) std.fs.path.join(allocator, &.{rest, last}) else allocator.dupe(u8, last);
}

pub fn deinit(self: *Self) void {
    self.output_dir.close();

    inline for (std.meta.tags(std.meta.FieldEnum(Target))) |tag| {
        if (self.entries.get(tag)) |v| {
            self.allocator.free(v);
        }
    }
}

pub fn applyQuery(self: *Self, query: Symbol) !void {
    self.entries.put(.query, try self.allocator.dupe(u8, query));
}

pub fn applyPlaceholder(self: *Self, parameters: []const FieldTypePair) !void {
    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();

    var buf = std.ArrayList(u8).init(self.allocator);
    defer buf.deinit();
    var writer = buf.writer();

    const INDENT_LEVEL = 2;
    const temp_allocator = arena.allocator();

    try writer.writeAll((" " ** INDENT_LEVEL) ++ "export type Parameter = {\n");

    // `  1: number | null`,
    for (parameters) |p| {
        const field = try IdentifierFormatter.format(temp_allocator, p.field_name, .camel_case);
        const key = key: {
            if (p.field_type) |t| {
                break :key try std.ascii.allocUpperString(temp_allocator, t);
            }
            else {
                break :key "ANY";
            }
        };

        const ts_type = TypeMappingRules.get(key) orelse {
            return error.UnsupportedDbType;
        };
        try writer.print((" " ** (INDENT_LEVEL * 2)) ++ "{s}: {s} | null,\n", .{field, ts_type});
    }

    try writer.writeAll((" " ** INDENT_LEVEL) ++ "}");
    
    self.entries.put(.parameter, try buf.toOwnedSlice());
}

pub fn applyResultSets(self: *Self, result_set: []const ResultSetColumn) !void {
    if (result_set.len == 0) return;

    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();

    var buf = std.ArrayList(u8).init(self.allocator);
    defer buf.deinit();
    var writer = buf.writer();
    
    const INDENT_LEVEL = 2;
    const temp_allocator = arena.allocator();

    try writer.writeAll((" " ** INDENT_LEVEL) ++ "export type ResultSet = {\n");

    // a: number | null
    for (result_set) |c| {
        // TODO: supports camelCase
        const field = name: {
            if (try isLiteral(temp_allocator, c.field_name)) {
                break:name try IdentifierFormatter.format(temp_allocator, c.field_name, .camel_case);
            }
            else {
                break:name try std.fmt.allocPrint(temp_allocator, "\"{s}\"", .{c.field_name});
            }
        };

        const key = try std.ascii.allocUpperString(temp_allocator, c.field_type);

        const ts_type = TypeMappingRules.get(key) orelse {
            return error.UnsupportedDbType;
        };
        try writer.print((" " ** (INDENT_LEVEL*2)) ++ "{s}: {s}{s},\n", .{
            field, ts_type,
            if (c.nullable) " | null" else ""
        });
    }
    try writer.writeAll((" " ** INDENT_LEVEL) ++ "}");
    
    self.entries.put(.result_set, try buf.toOwnedSlice());
}

fn isLiteral(allocator: std.mem.Allocator, symbol: Symbol) !bool {
    var tz = std.zig.Tokenizer.init(try allocator.dupeZ(u8, symbol));
    
    return tz.next().loc.end == symbol.len;
}

fn writeLiteral(writer: *std.ArrayList(u8).Writer, text: Symbol) !void {
    try writer.writeByte('\'');
    try writer.writeAll(text);
    try writer.writeByte('\'');
}

pub fn applyUserType(self: *Self, user_type: UserTypeDef) !void {
    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();

    var buf = std.ArrayList(u8).init(self.allocator);
    defer buf.deinit();
    var writer = buf.writer();
    
    const INDENT_LEVEL = 2;

    if (user_type.header.kind == .@"enum") {
        try writer.print((" " ** INDENT_LEVEL) ++ "export type {s} = ", .{
            try IdentifierFormatter.format(arena.allocator(), user_type.header.name, .pascal_case)
        });

        try writeLiteral(&writer, user_type.fields[0].field_name);

        var i: usize = 1;
        while (i < user_type.fields.len): (i += 1) {
            try writer.writeAll(" | ");
            try writeLiteral(&writer, user_type.fields[i].field_name);
        }

        self.entries.put(.user_type, try buf.toOwnedSlice());
    }
}

fn writeQuery(output_dir: std.fs.Dir, query: Symbol) !void {
    var file = try output_dir.createFile("query.sql", .{});
    defer file.close();

    try file.writeAll(query);
}

fn writeTypescriptTypes(output_dir: std.fs.Dir, type_defs: []const ?Symbol) !void {
    var file = try output_dir.createFile("types.ts", .{});
    defer file.close();
    var writer = file.writer();

    try writer.print("export namespace {s} {{\n", .{Namespace});

    for (type_defs) |t_| {
        if (t_) |type_def| {
            try writer.print("{s}\n", .{type_def});
        }
    }

    try writer.writeAll("}");
}

pub fn buildWith(self: *Self, handler: *const fn (builder: *CodeBuilder) anyerror!void) !void {
    try handler(self);
}

pub const SourceGenerator = struct {
    pub fn build(builder: *CodeBuilder) anyerror!void {
        if (builder.entries.get(.query)) |query| {
            writeQuery(builder.output_dir, query) catch {
                return error.QueryFileGenerationFailed;
            };
        }
        types: {
            writeTypescriptTypes(builder.output_dir, &.{builder.entries.get(.parameter), builder.entries.get(.result_set)}) catch {
                return error.TypeFileGenerationFailed;
            };
            break:types;
        }
    }
};

pub const UserTypeGenerator = struct {
    pub fn build(builder: *CodeBuilder) anyerror!void {
        var out_dir = try builder.output_dir.makeOpenPath("user-types", .{});
        defer out_dir.close();

        writeTypescriptTypes(out_dir, &.{builder.entries.get(.user_type)}) catch {
            return error.TypeFileGenerationFailed;
        };
    }
};

pub const Target = union(enum) {
    query: Symbol,
    parameter: []const FieldTypePair,
    result_set: []const ResultSetColumn,
    user_type: UserTypeDef,
};

pub const FieldTypePair = struct {
    field_name: Symbol,
    field_type: ?Symbol = null,
};

pub const ResultSetColumn = struct {
    field_name: Symbol,
    field_type: Symbol,
    nullable: bool,
};

pub const UserTypeDef = struct {
    header: Header,
    fields: []const FieldTypePair,

    pub const Header = struct {
        kind: enum {@"enum"},
        name: Symbol,
    };
};

pub const Parser = struct {
    pub fn beginParse(allocator: std.mem.Allocator, source_bodies: []const core.Event.Payload.TopicBody.Item) !ResultWalker {
        const arena = try allocator.create(std.heap.ArenaAllocator);
        arena.* = std.heap.ArenaAllocator.init(allocator);

        return .{
            .arena = arena,
            .source_bodies = source_bodies,
            .index = 0,
        };
    }

    fn parsePlaceholder(allocator: std.mem.Allocator, content: Symbol) ![]const FieldTypePair {
        var reader = core.CborStream.Reader.init(content);

        const values = try reader.readSlice(allocator, core.StructView(FieldTypePair)); 
        var result = try allocator.alloc(FieldTypePair, values.len);

        for (values, 0..) |v, i| {
            result[i] = .{
                .field_name = v[0],
                .field_type = v[1],
            };
        }

        return result;
    }

    fn parseResultSet(allocator: std.mem.Allocator, content: Symbol) ![]const ResultSetColumn {
        var reader = core.CborStream.Reader.init(content);

        const values = try reader.readSlice(allocator, core.StructView(ResultSetColumn));

        var result_set = try allocator.alloc(ResultSetColumn, values.len);

        for (values, 0..) |v, i| {
            result_set[i] = .{
                .field_name = v[0],
                .field_type = v[1],
                .nullable = v[2],
            };
        }

        return result_set;
    }

    fn parseUserTypeDefinition(allocator: std.mem.Allocator, content: Symbol) !UserTypeDef {
        var reader = core.CborStream.Reader.init(content);

        const header = try reader.readTuple(core.StructView(UserTypeDef.Header));
        const values = try reader.readSlice(allocator, core.StructView(FieldTypePair));

        var fields = try allocator.alloc(FieldTypePair, values.len);

        for (values, 0..) |v, i| {
            fields[i] = .{
                .field_name = v[0],
                .field_type = v[1],
            };
        }

        return .{
            .header = .{ .kind = header[0], .name = header[1] },
            .fields = fields,
        };
    }

    pub const ResultWalker = struct {
        arena: *std.heap.ArenaAllocator,
        source_bodies: []const core.Event.Payload.TopicBody.Item,
        index: usize,

        pub fn deinit(self: *ResultWalker) void {
            self.arena.deinit();
            self.arena.child_allocator.destroy(self.arena);
            self.* = undefined;
        }

        const TargetKindMap = std.StaticStringMap(std.meta.FieldEnum(Target)).initComptime(.{ 
            .{ "query", .query }, .{ "placeholder", .parameter }, .{ "select-list", .result_set },
            .{ "user-type", .user_type },
        });

        pub fn walk(self: *ResultWalker) !?Target {
            while (self.index < self.source_bodies.len) {
                defer self.index += 1;

                const body = self.source_bodies[self.index];

                switch (TargetKindMap.get(body.topic) orelse continue) {
                    .query => {
                        return .{ .query = body.content };
                    },
                    .parameter => {
                        return .{ .parameter = try Parser.parsePlaceholder(self.arena.allocator(), body.content) };
                    },
                    .result_set => {
                        return .{ .result_set = try Parser.parseResultSet(self.arena.allocator(), body.content) };
                    },
                    .user_type => {
                        return .{ .user_type = try Parser.parseUserTypeDefinition(self.arena.allocator(), body.content) };
                    }
                }
            }

            return null;
        }
    };
};

test "parse query" {
    const allocator = std.testing.allocator;
    const arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);
    
    const source_bodies: []const core.Event.Payload.TopicBody.Item = &.{.{
        .topic = "query",
        .content = "select $1, $2 from foo where kind = $3",
    }};

    var iter: Parser.ResultWalker = .{
        .arena = arena,
        .source_bodies = source_bodies,
        .index = 0,
    };
    defer iter.deinit();

    assert: {
        const walk_result = try iter.walk();
        try std.testing.expect(walk_result != null);

        const result = walk_result.?;
        try std.testing.expectEqual(.query, std.meta.activeTag(result));
        try std.testing.expectEqualStrings(source_bodies[0].content, result.query);
        break :assert;
    }
    assert: {
        const walk_result = try iter.walk();
        try std.testing.expect(walk_result == null);
        break :assert;
    }
}

const TypeMappingRules = std.StaticStringMap(Symbol).initComptime(.{
    // BIGINT 	INT8, LONG 	signed eight-byte integer
    .{"BIGINT", "number"}, 
    .{"INT8", "number"}, 
    .{"LONG", "number"}, 
    // BIT 	BITSTRING 	string of 1s and 0s
    .{"BIT", "string"}, 
    .{"BITSTRING", "string"}, 
    // BLOB BYTEA, BINARY, VARBINARY 	variable-length binary data
    .{"BLOB", "string"}, 
    .{"BYTEA", "string"}, 
    .{"BINARY", "string"}, 
    .{"VARBINARY", "string"}, 
    // BOOLEAN 	BOOL, LOGICAL 	logical boolean (true/false)
    .{"BOOLEAN", "boolean"}, 
    .{"BOOL", "boolean"}, 
    .{"LOGICAL", "boolean"}, 
    // DATE calendar date (year, month day)
    .{"DATE", "string"}, 
    // DECIMAL(prec, scale), NUMERIC(prec, scale) 	fixed-precision number
    .{"DECIMAL", "number"}, 
    .{"NUMERIC", "number"}, 
    // DOUBLE 	FLOAT8, 	double precision floating-point number (8 bytes)
    .{"DOUBLE", "number"}, 
    .{"FLOAT8", "number"}, 
    // HUGEINT 	  	signed sixteen-byte integer
    .{"HUGEINT", "number"}, 
    // INTEGER 	INT4, INT, SIGNED 	signed four-byte integer
    .{"INTEGER", "number"}, 
    .{"INT4", "number"}, 
    .{"INT", "number"}, 
    .{"SIGNED", "number"}, 
    // INTERVAL date / time delta
    .{"INTERVAL", "string"}, 
    // REAL FLOAT4, FLOAT 	single precision floating-point number (4 bytes)
    .{"REAL", "number"}, 
    .{"FLOAT4", "number"}, 
    .{"FLOAT", "number"}, 
    // INT2, SHORT 	signed two-byte integer
    .{"SMALLINT", "number"}, 
    .{"SHORT", "number"}, 
    // TIME time of day (no time zone)
    .{"TIME", "string"}, 
    // TIMESTAMP WITH TIME ZONE, TIMESTAMPTZ 	combination of time and date
    .{"TIMESTAMP WITH TIME ZONE", "string"}, 
    .{"TIMESTAMPZ", "string"}, 
    // TIMESTAMP, DATETIME 	combination of time and date
    .{"TIMESTAMP", "string"}, 
    .{"DATETIME", "string"}, 
    // TINYINT 	INT1 signed one-byte integer
    .{"TINYINT", "number"}, 
    .{"INT1", "number"}, 
    // UBIGINT unsigned eight-byte integer
    .{"UBIGINT", "number"}, 
    // UHUGEINT unsigned sixteen-byte integer
    .{"UHUGEINT", "number"}, 
    // UINTEGER unsigned four-byte integer
    .{"UINTEGER", "number"}, 
    // USMALLINT unsigned two-byte integer
    .{"USMALLINT", "number"}, 
    // UTINYINT unsigned one-byte integer
    .{"UTINYINT", "number"}, 
    // UUID UUID data type
    .{"UUID", "string"}, 
    // VARCHAR 	CHAR, BPCHAR, TEXT, STRING 	variable-length character string
    .{"VARCHAR", "string"}, 
    .{"CHAR", "string"}, 
    .{"BPCHAR", "string"}, 
    .{"TEXT", "string"}, 
    .{"STRING", "string"}, 
    // Other
    .{"ANY", "any"},
});

fn placeholderToCbor(allocator: std.mem.Allocator, items: []const FieldTypePair) !core.Symbol {
    var writer = try core.CborStream.Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeSliceHeader(items.len);

    for (items) |c| {
        _ = try writer.writeTuple(core.StructView(FieldTypePair), .{c.field_name, c.field_type});
    }

    return writer.buffer.toOwnedSlice();
}

test "parse parameter#1" {
    const allocator = std.testing.allocator;
    const arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);

    const expect: []const FieldTypePair = &.{
        .{.field_name = "id", .field_type = "bigint"},
        .{.field_name = "name", .field_type = "varchar"},
    };
    const source_bodies = try placeholderToCbor(arena.allocator(), expect);
    
    var iter: Parser.ResultWalker = .{
        .arena = arena,
        .source_bodies = &.{
            .{ .topic = "placeholder", .content = source_bodies }
        },
        .index = 0,
    };
    defer iter.deinit();

    assert: {
        const walk_result = try iter.walk();
        try std.testing.expect(walk_result != null);

        const result = walk_result.?;
        try std.testing.expectEqual(.parameter, std.meta.activeTag(result));
        try std.testing.expectEqualDeep(expect, result.parameter);
        break :assert;
    }
    assert: {
        const walk_result = try iter.walk();
        try std.testing.expect(walk_result == null);
        break :assert;
    }
}

test "parse parameter#2 (with any type)" {
    const allocator = std.testing.allocator;
    const arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);

    const expect: []const FieldTypePair = &.{
        .{.field_name = "id", .field_type = "bigint"},
        .{.field_name = "name", .field_type = null},
    };
    const source_bodies = try placeholderToCbor(arena.allocator(), expect);

    var iter: Parser.ResultWalker = .{
        .arena = arena,
        .source_bodies = &.{
            .{ .topic = "placeholder", .content = source_bodies }
        },
        .index = 0,
    };
    defer iter.deinit();

    assert: {
        const walk_result = try iter.walk();
        try std.testing.expect(walk_result != null);

        const result = walk_result.?;
        try std.testing.expectEqual(.parameter, std.meta.activeTag(result));
        try std.testing.expectEqualDeep(expect, result.parameter);
        break :assert;
    }
    assert: {
        const walk_result = try iter.walk();
        try std.testing.expect(walk_result == null);
        break :assert;
    }
}

fn resultSetToCbor(allocator: std.mem.Allocator, result_set: []const ResultSetColumn) !core.Symbol {
    var writer = try core.CborStream.Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeSliceHeader(result_set.len);

    for (result_set) |c| {
        _ = try writer.writeTuple(core.StructView(ResultSetColumn), .{c.field_name, c.field_type, c.nullable});
    }

    return writer.buffer.toOwnedSlice();
}

test "parse empty result set" {
    const allocator = std.testing.allocator;
    const arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);

    const expect: []const ResultSetColumn = &.{};

    const source_bodies: []const core.Event.Payload.TopicBody.Item = &.{.{
        .topic = "select-list",
        .content = try resultSetToCbor(arena.allocator(), expect),
    }};

    var iter: Parser.ResultWalker = .{
        .arena = arena,
        .source_bodies = source_bodies,
        .index = 0,
    };
    defer iter.deinit();

    assert: {
        const walk_result = try iter.walk();
        try std.testing.expect(walk_result != null);

        const result = walk_result.?;
        try std.testing.expectEqual(.result_set, std.meta.activeTag(result));
        try std.testing.expectEqualDeep(expect, result.result_set);
        break :assert;
    }
    assert: {
        const walk_result = try iter.walk();
        try std.testing.expect(walk_result == null);
        break :assert;
    }
}

test "parse result set" {
    const allocator = std.testing.allocator;
    const arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);

    const expect: []const ResultSetColumn = &.{
        .{.field_name = "a", .field_type = "INTEGER", .nullable = false},
        .{.field_name = "b", .field_type = "VARCHAR", .nullable = true},
    };

    const source_bodies: []const core.Event.Payload.TopicBody.Item = &.{.{
        .topic = "select-list",
        .content = try resultSetToCbor(arena.allocator(), expect),
    }};

    var iter: Parser.ResultWalker = .{
        .arena = arena,
        .source_bodies = source_bodies,
        .index = 0,
    };
    defer iter.deinit();

    assert: {
        const walk_result = try iter.walk();
        try std.testing.expect(walk_result != null);

        const result = walk_result.?;
        try std.testing.expectEqual(.result_set, std.meta.activeTag(result));
        try std.testing.expectEqualDeep(expect, result.result_set);
        break :assert;
    }
    assert: {
        const walk_result = try iter.walk();
        try std.testing.expect(walk_result == null);
        break :assert;
    }
}

test "parse result set with aliasless field name" {
    const allocator = std.testing.allocator;
    const arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);

    const source: []const ResultSetColumn = &.{
        .{.field_name = "Cast(a as INTEGER)", .field_type = "INTEGER", .nullable = false},
        .{.field_name = "bar_baz", .field_type = "VARCHAR", .nullable = true},
    };

    const expect: []const ResultSetColumn = &.{
        .{.field_name = "Cast(a as INTEGER)", .field_type = "INTEGER", .nullable = false},
        .{.field_name = "bar_baz", .field_type = "VARCHAR", .nullable = true},
    };

    const source_bodies: []const core.Event.Payload.TopicBody.Item = &.{.{
        .topic = "select-list",
        .content = try resultSetToCbor(arena.allocator(), source),
    }};

    var iter: Parser.ResultWalker = .{
        .arena = arena,
        .source_bodies = source_bodies,
        .index = 0,
    };
    defer iter.deinit();

    assert: {
        const walk_result = try iter.walk();
        try std.testing.expect(walk_result != null);

        const result = walk_result.?;
        try std.testing.expectEqual(.result_set, std.meta.activeTag(result));
        try std.testing.expectEqualDeep(expect, result.result_set);
        break:assert;
    }
    assert: {
        const walk_result = try iter.walk();
        try std.testing.expect(walk_result == null);
        break:assert;
    }
}

fn userTypeToCbor(allocator: std.mem.Allocator, user_type: UserTypeDef) !Symbol {
    var writer = try core.CborStream.Writer.init(allocator);
    defer writer.deinit();

    header: {
        _ = try writer.writeSliceHeader(2);
        _ = try writer.writeString(@tagName(user_type.header.kind));
        _ = try writer.writeString(user_type.header.name);
        break:header;
    }
    bodies: {
        _ = try writer.writeSliceHeader(user_type.fields.len);

        for (user_type.fields) |c| {
            _ = try writer.writeTuple(core.StructView(FieldTypePair), .{c.field_name, c.field_type});
        }
        break:bodies;
    }

    return writer.buffer.toOwnedSlice();
}

test "parse enum user type" {
    const allocator = std.testing.allocator;
    const arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);

    const expect: UserTypeDef = .{
        .header = .{
            .kind = .@"enum", .name = "Visibility",
        },
        .fields = &.{
            .{.field_name = "hide", .field_type = null}, 
            .{.field_name = "visible", .field_type = null}, 
        },
    };

    const source_bodies: []const core.Event.Payload.TopicBody.Item = &.{.{
        .topic = "user-type",
        .content = try userTypeToCbor(arena.allocator(), expect),
    }};

    var iter: Parser.ResultWalker = .{
        .arena = arena,
        .source_bodies = source_bodies,
        .index = 0,
    };
    defer iter.deinit();

    assert: {
        const walk_result = try iter.walk();
        try std.testing.expect(walk_result != null);

        const result = walk_result.?;
        try std.testing.expectEqual(.user_type, std.meta.activeTag(result));
        try std.testing.expectEqualDeep(expect, result.user_type);
        break:assert;
    }
    assert: {
        const walk_result = try iter.walk();
        try std.testing.expect(walk_result == null);
        break:assert;
    }
}

fn runApplyPlaceholder(parameters: []const FieldTypePair, expect: Symbol) !void {
    const allocator = std.testing.allocator;

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    const parent_path = try dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(parent_path);

    var builder = try Self.init(allocator, parent_path, "foo");
    defer builder.deinit();

    try builder.applyPlaceholder(parameters);

    const apply_result = builder.entries.get(.parameter);
    try std.testing.expect(apply_result != null);

    const result = apply_result.?;

    try std.testing.expectEqualStrings(expect, result);
}

test "generate name parameter code#1" {
    const parameters: []const FieldTypePair = &.{
        .{.field_name = "id", .field_type = "BIGINT"},
        .{.field_name = "name", .field_type = "VARCHAR"},
    };
    const expect = 
        \\  export type Parameter = {
        \\    id: number | null,
        \\    name: string | null,
        \\  }
    ;

    try runApplyPlaceholder(parameters, expect);
}

test "generate name parameter code#2 (upper case field)" {
    const parameters: []const FieldTypePair = &.{
        .{.field_name = "ID", .field_type = "BIGINT"},
        .{.field_name = "NAME", .field_type = "VARCHAR"},
    };
    const expect = 
        \\  export type Parameter = {
        \\    id: number | null,
        \\    name: string | null,
        \\  }
    ;

    try runApplyPlaceholder(parameters, expect);
}

test "generate name parameter code#3 (lower-case)" {
    const parameters: []const FieldTypePair = &.{
        .{.field_name = "id", .field_type = "int"},
        .{.field_name = "name", .field_type = "varchar"},
    };
    const expect = 
        \\  export type Parameter = {
        \\    id: number | null,
        \\    name: string | null,
        \\  }
    ;

    try runApplyPlaceholder(parameters, expect);
}

test "generate name parameter code#4 (with any type)" {
    const parameters: []const FieldTypePair = &.{
        .{.field_name = "id", .field_type = null},
        .{.field_name = "name", .field_type = null},
    };
    const expect = 
        \\  export type Parameter = {
        \\    id: any | null,
        \\    name: any | null,
        \\  }
    ;

    try runApplyPlaceholder(parameters, expect);
}

test "generate name parameter code#5 (with snake_case)" {
    const parameters: []const FieldTypePair = &.{
        .{.field_name = "user_id", .field_type = "int"},
        .{.field_name = "user_name", .field_type = "text"},
    };
    const expect = 
        \\  export type Parameter = {
        \\    userId: number | null,
        \\    userName: string | null,
        \\  }
    ;

    try runApplyPlaceholder(parameters, expect);
}

test "generate name parameter code#5 (with PascalCase)" {
    const parameters: []const FieldTypePair = &.{
        .{.field_name = "UserId", .field_type = "int"},
        .{.field_name = "UserName", .field_type = "text"},
    };
    const expect = 
        \\  export type Parameter = {
        \\    userId: number | null,
        \\    userName: string | null,
        \\  }
    ;

    try runApplyPlaceholder(parameters, expect);
}

test "generate positional parameter code" {
    const parameters: []const FieldTypePair = &.{
        .{.field_name = "1", .field_type = "float"},
        .{.field_name = "2", .field_type = "text"},
    };
    const expect = 
        \\  export type Parameter = {
        \\    1: number | null,
        \\    2: string | null,
        \\  }
    ;

    try runApplyPlaceholder(parameters, expect);
}

test "generate positional parameter code with any type" {
    const parameters: []const FieldTypePair = &.{
        .{.field_name = "1", .field_type = null},
        .{.field_name = "2", .field_type = null},
    };
    const expect = 
        \\  export type Parameter = {
        \\    1: any | null,
        \\    2: any | null,
        \\  }
    ;

    try runApplyPlaceholder(parameters, expect);
}

fn runApplyResultSets(parameters: []const ResultSetColumn, expect: Symbol) !void {
    const allocator = std.testing.allocator;

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    const parent_path = try dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(parent_path);

    var builder = try Self.init(allocator, parent_path, "foo");
    defer builder.deinit();

    try builder.applyResultSets(parameters);

    const apply_result = builder.entries.get(.result_set);
    try std.testing.expect(apply_result != null);
    try std.testing.expectEqualStrings(expect, apply_result.?);
}

test "generate select list#1 (lowercase field)" {
    const result_set: []const ResultSetColumn = &.{
        .{.field_name = "id", .field_type = "INTEGER", .nullable = false},
        .{.field_name = "kind", .field_type = "INTEGER", .nullable = false},
        .{.field_name = "value", .field_type = "VARCHAR", .nullable = false},
    };
    const expect = 
        \\  export type ResultSet = {
        \\    id: number,
        \\    kind: number,
        \\    value: string,
        \\  }
    ;
    try runApplyResultSets(result_set, expect);
}

test "generate select list#2 (PascalCase field)" {
    const result_set: []const ResultSetColumn = &.{
        .{.field_name = "userId", .field_type = "INTEGER", .nullable = false},
        .{.field_name = "profileKind", .field_type = "INTEGER", .nullable = false},
        .{.field_name = "remarks", .field_type = "VARCHAR", .nullable = false},
    };
    const expect = 
        \\  export type ResultSet = {
        \\    userId: number,
        \\    profileKind: number,
        \\    remarks: string,
        \\  }
    ;
    try runApplyResultSets(result_set, expect);
}

test "generate select list#3 (lower snake_case field)" {
    const result_set: []const ResultSetColumn = &.{
        .{.field_name = "user_id", .field_type = "INTEGER", .nullable = false},
        .{.field_name = "profile_kind", .field_type = "INTEGER", .nullable = false},
        .{.field_name = "remarks", .field_type = "VARCHAR", .nullable = false},
    };
    const expect = 
        \\  export type ResultSet = {
        \\    userId: number,
        \\    profileKind: number,
        \\    remarks: string,
        \\  }
    ;
    try runApplyResultSets(result_set, expect);
}

test "generate select list#4 (upper snake_case field)" {
    const result_set: []const ResultSetColumn = &.{
        .{.field_name = "USER_ID", .field_type = "INTEGER", .nullable = false},
        .{.field_name = "PROFILE_KIND", .field_type = "INTEGER", .nullable = false},
        .{.field_name = "REMARKS", .field_type = "VARCHAR", .nullable = false},
    };
    const expect = 
        \\  export type ResultSet = {
        \\    userId: number,
        \\    profileKind: number,
        \\    remarks: string,
        \\  }
    ;
    try runApplyResultSets(result_set, expect);
}

test "generate select list#5 (nullable field)" {
    const result_set: []const ResultSetColumn = &.{
        .{.field_name = "id", .field_type = "INTEGER", .nullable = false},
        .{.field_name = "kind", .field_type = "INTEGER", .nullable = true},
        .{.field_name = "value", .field_type = "VARCHAR", .nullable = true},
    };
    const expect = 
        \\  export type ResultSet = {
        \\    id: number,
        \\    kind: number | null,
        \\    value: string | null,
        \\  }
    ;
    try runApplyResultSets(result_set, expect);
}

test "generate select list#6 (field without alias)" {

}

fn runApplyUserType(enum_type: UserTypeDef, expect: Symbol) !void {
    const allocator = std.testing.allocator;

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    const parent_path = try dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(parent_path);

    var builder = try Self.init(allocator, parent_path, "foo");
    defer builder.deinit();

    try builder.applyUserType(enum_type);

    const apply_result = builder.entries.get(.user_type);
    try std.testing.expect(apply_result != null);
    try std.testing.expectEqualStrings(expect, apply_result.?);
}

test "generate enum user type#1 (PascalCase type name)" {
    const enum_type: UserTypeDef = .{
        .header = .{
            .kind = .@"enum", .name = "Visibility",
        },
        .fields = &.{
            .{.field_name = "hide", .field_type = null}, 
            .{.field_name = "visible", .field_type = null}, 
        },
    };
    const expect = 
        \\  export type Visibility = 'hide' | 'visible'
    ;

    try runApplyUserType(enum_type, expect);
}

test "generate enum user type#2 (lowercase type name)" {
    const enum_type: UserTypeDef = .{
        .header = .{
            .kind = .@"enum", .name = "visibility",
        },
        .fields = &.{
            .{.field_name = "hide", .field_type = null}, 
            .{.field_name = "visible", .field_type = null}, 
        },
    };
    const expect = 
        \\  export type Visibility = 'hide' | 'visible'
    ;

    try runApplyUserType(enum_type, expect);
}

test "generate enum user type#3 (UPPER CASE type name)" {
    const enum_type: UserTypeDef = .{
        .header = .{
            .kind = .@"enum", .name = "VISIBILITY",
        },
        .fields = &.{
            .{.field_name = "hide", .field_type = null}, 
            .{.field_name = "visible", .field_type = null},
        },
    };
    const expect = 
        \\  export type Visibility = 'hide' | 'visible'
    ;

    try runApplyUserType(enum_type, expect);
}

test "generate enum user type#3 (snake_case type name)" {
    const enum_type: UserTypeDef = .{
        .header = .{
            .kind = .@"enum", .name = "USER_PROFILE_KIND",
        },
        .fields = &.{
            .{.field_name = "admin", .field_type = null}, 
            .{.field_name = "general", .field_type = null},
        },
    };
    const expect = 
        \\  export type UserProfileKind = 'admin' | 'general'
    ;

    try runApplyUserType(enum_type, expect);
}

test "Output build result" {
    const allocator = std.testing.allocator;

    var output_dir = std.testing.tmpDir(.{});
    defer output_dir.cleanup();
    const parent_path = try output_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(parent_path);

    var builder = try Self.init(allocator, parent_path, "foo");
    defer builder.deinit();

    builder.entries.put(.query, try allocator.dupe(u8, "select $1::id, $2::name from foo where value = $3::value"));
    builder.entries.put(.parameter, try allocator.dupe(u8,"export type P = { id:number|null, name:string|null, value:string|null}"));

    try builder.buildWith(SourceGenerator.build);

    query: {
        var file = try output_dir.dir.openFile("Foo/query.sql", .{.mode = .read_only});
        defer file.close();

        const meta = try file.metadata();
        const content = try file.readToEndAlloc(allocator, meta.size());
        defer allocator.free(content);

        try std.testing.expectEqualStrings(builder.entries.get(.query).?, content);

        break :query;
    }
    placeholder: {
        var file = try output_dir.dir.openFile("Foo/types.ts", .{});
        defer file.close();

        const meta = try file.metadata();
        const content = try file.readToEndAlloc(allocator, meta.size());
        defer allocator.free(content);

        try std.testing.expect(
            std.mem.containsAtLeast(u8, content, 1, builder.entries.get(.parameter).?)
        );

        break :placeholder;
    }
}

test "Output build enum user type" {
    const allocator = std.testing.allocator;

    var output_dir = std.testing.tmpDir(.{});
    defer output_dir.cleanup();
    const parent_path = try output_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(parent_path);

    var builder = try Self.init(allocator, parent_path, "foo");
    defer builder.deinit();

    builder.entries.put(.user_type, try allocator.dupe(u8, "export type Visibility = 'hide' | 'visible'"));

    try builder.buildWith(UserTypeGenerator.build);

    user_type: {
        var file = try output_dir.dir.openFile("Foo/user-types/types.ts", .{});
        defer file.close();

        const meta = try file.metadata();
        const content = try file.readToEndAlloc(allocator, meta.size());
        defer allocator.free(content);

        try std.testing.expect(
            std.mem.containsAtLeast(u8, content, 1, builder.entries.get(.user_type).?)
        );

        break:user_type;
    }
}
