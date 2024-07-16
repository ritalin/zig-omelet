const std = @import("std");
const core = @import("core");
const Symbol = core.Symbol;
const FilePath = core.FilePath;

const Self = @This();

const Namespace = "Sql";
const ToArrayFn = "export const toArray = (parameter: Parameter) => Object.values(parameter)";

allocator: std.mem.Allocator,
output_dir: std.fs.Dir,

query: ?Symbol,
parameters: ?Symbol,
result_set: ?Symbol,

pub fn init(allocator: std.mem.Allocator, prefix_dir_path: core.FilePath, dest_dir_path: FilePath) !Self {
    const dir = dir: {
        var parent_dir = try std.fs.cwd().makeOpenPath(prefix_dir_path, .{});
        defer parent_dir.close();
        break :dir try parent_dir.makeOpenPath(if (dest_dir_path.len > 0) dest_dir_path else prefix_dir_path, .{});
    };

    return .{
        .allocator = allocator,
        .output_dir = dir,
        .query = null,
        .parameters = null,
        .result_set = null,
    };
}

pub fn deinit(self: *Self) void {
    self.output_dir.close();

    if (self.query) |x| self.allocator.free(x);
    if (self.parameters) |x| self.allocator.free(x);
    if (self.result_set) |x| self.allocator.free(x);
}

pub fn applyQuery(self: *Self, query: Symbol) !void {
    self.query = try self.allocator.dupe(u8, query);
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
        const field = try std.ascii.allocLowerString(temp_allocator, p.field_name);
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
    
    self.parameters = try buf.toOwnedSlice();
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
        const field = try std.ascii.allocLowerString(temp_allocator, c.field_name);
        const key = try std.ascii.allocUpperString(temp_allocator, c.field_type);

        const ts_type = TypeMappingRules.get(key) orelse {
            return error.UnsupportedDbType;
        };
        try writer.print((" " ** (INDENT_LEVEL*2)) ++ "{s}: {s} {s},\n", .{
            field, ts_type,
            if (c.nullable) "| null" else ""
        });
    }
    try writer.writeAll((" " ** INDENT_LEVEL) ++ "}");
    
    self.result_set = try buf.toOwnedSlice();
}

pub fn build(self: Self) !void {
    if (self.query) |query| {
        writeQuery(self.output_dir, query) catch {
            return error.QueryFileGenerationFailed;
        };
    }
    types: {
        writeTypescriptTypes(self.output_dir, &.{self.parameters, self.result_set}) catch {
            return error.TypeFileGenerationFailed;
        };

        break :types;
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

pub const Target = union(enum) {
    query: Symbol,
    parameter: []const FieldTypePair,
    result_set: []const ResultSetColumn,
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
        const result = try std.json.parseFromSlice([]const FieldTypePair, allocator, content, .{});
        return result.value;
    }

    fn parseResultSet(allocator: std.mem.Allocator, content: Symbol) ![]const ResultSetColumn {
        var reader = core.CborStream.Reader.init(content);

        const values = try reader.readSlice(allocator, core.StructView(ResultSetColumn));

        var result_set = try allocator.alloc(ResultSetColumn, values.len);

        for (values, 0..) |v, i| {
            const field_name = 
                if (try isLiteral(allocator, v[0])) try allocator.dupe(u8, v[0]) 
                else try std.fmt.allocPrint(allocator, "\"{s}\"", .{v[0]})
            ;

            result_set[i] = .{
                .field_name = field_name,
                .field_type = v[1],
                .nullable = v[2],
            };
        }

        return result_set;
    }

    fn isLiteral(allocator: std.mem.Allocator, symbol: Symbol) !bool {
        var tz = std.zig.Tokenizer.init(try allocator.dupeZ(u8, symbol));
        
        return tz.next().loc.end == symbol.len;
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
            .{ "query", .query }, .{ "placeholder", .parameter }, .{ "select-list", .result_set } 
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

test "parse parameter" {
    const allocator = std.testing.allocator;
    const arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);

    const source_bodies: []const core.Event.Payload.TopicBody.Item = &.{.{
        .topic = "placeholder",
        .content = "[{\"field_name\":\"id\", \"field_type\":\"bigint\"}, {\"field_name\":\"name\", \"field_type\":\"varchar\"}]"
    }};

    var iter: Parser.ResultWalker = .{
        .arena = arena,
        .source_bodies = source_bodies,
        .index = 0,
    };
    defer iter.deinit();

    assert: {
        const expect: []const FieldTypePair = &.{
            .{.field_name = "id", .field_type = "bigint"},
            .{.field_name = "name", .field_type = "varchar"},
        };

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

test "parse parameter with any type" {
    const allocator = std.testing.allocator;
    const arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);

    const source_bodies: []const core.Event.Payload.TopicBody.Item = &.{.{
        .topic = "placeholder",
        .content = "[{\"field_name\":\"id\", \"field_type\":\"bigint\"}, {\"field_name\":\"name\"}]"
    }};

    var iter: Parser.ResultWalker = .{
        .arena = arena,
        .source_bodies = source_bodies,
        .index = 0,
    };
    defer iter.deinit();

    assert: {
        const expect: []const FieldTypePair = &.{
            .{.field_name = "id", .field_type = "bigint"},
            .{.field_name = "name", .field_type = null},
        };

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
        .{.field_name = "\"Cast(a as INTEGER)\"", .field_type = "INTEGER", .nullable = false},
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
        break :assert;
    }
    assert: {
        const walk_result = try iter.walk();
        try std.testing.expect(walk_result == null);
        break :assert;
    }
}

test "generate name parameter code" {
    const allocator = std.testing.allocator;

    const parameters: []const FieldTypePair = &.{
        .{.field_name = "id", .field_type = "BIGINT"},
        .{.field_name = "name", .field_type = "VARCHAR"},
    };

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    const parent_path = try dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(parent_path);

    var builder = try Self.init(allocator, parent_path, "foo");
    defer builder.deinit();

    try builder.applyPlaceholder(parameters);

    const apply_result = builder.parameters;
    try std.testing.expect(apply_result != null);

    const result = apply_result.?;

    const expect = 
        \\  export type Parameter = {
        \\    id: number | null,
        \\    name: string | null,
        \\  }
    ;

    try std.testing.expectEqualStrings(expect, result);
}

test "generate name parameter code for upper case field" {
    const allocator = std.testing.allocator;

    const parameters: []const FieldTypePair = &.{
        .{.field_name = "ID", .field_type = "BIGINT"},
        .{.field_name = "NAME", .field_type = "VARCHAR"},
    };

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    const parent_path = try dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(parent_path);

    var builder = try Self.init(allocator, parent_path, "foo");
    defer builder.deinit();

    try builder.applyPlaceholder(parameters);

    const apply_result = builder.parameters;
    try std.testing.expect(apply_result != null);

    const result = apply_result.?;

    const expect = 
        \\  export type Parameter = {
        \\    id: number | null,
        \\    name: string | null,
        \\  }
    ;

    try std.testing.expectEqualStrings(expect, result);
}

test "generate name parameter code from lower-case" {
    const allocator = std.testing.allocator;

    const parameters: []const FieldTypePair = &.{
        .{.field_name = "id", .field_type = "int"},
        .{.field_name = "name", .field_type = "varchar"},
    };

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    const parent_path = try dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(parent_path);

    var builder = try Self.init(allocator, parent_path, "foo");
    defer builder.deinit();

    try builder.applyPlaceholder(parameters);

    const apply_result = builder.parameters;
    try std.testing.expect(apply_result != null);

    const result = apply_result.?;

    const expect = 
        \\  export type Parameter = {
        \\    id: number | null,
        \\    name: string | null,
        \\  }
    ;

    try std.testing.expectEqualStrings(expect, result);
}

test "generate name parameter code with any type" {
    const allocator = std.testing.allocator;

    const parameters: []const FieldTypePair = &.{
        .{.field_name = "id", .field_type = null},
        .{.field_name = "name", .field_type = null},
    };

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    const parent_path = try dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(parent_path);

    var builder = try Self.init(allocator, parent_path, "foo");
    defer builder.deinit();

    try builder.applyPlaceholder(parameters);

    const apply_result = builder.parameters;
    try std.testing.expect(apply_result != null);

    const result = apply_result.?;

    const expect = 
        \\  export type Parameter = {
        \\    id: any | null,
        \\    name: any | null,
        \\  }
    ;

    try std.testing.expectEqualStrings(expect, result);
}

test "generate positional parameter code" {
    const allocator = std.testing.allocator;

    const parameters: []const FieldTypePair = &.{
        .{.field_name = "1", .field_type = "float"},
        .{.field_name = "2", .field_type = "text"},
    };

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    const parent_path = try dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(parent_path);

    var builder = try Self.init(allocator, parent_path, "foo");
    defer builder.deinit();

    try builder.applyPlaceholder(parameters);

    const apply_result = builder.parameters;
    try std.testing.expect(apply_result != null);

    const result = apply_result.?;

    const expect = 
        \\  export type Parameter = {
        \\    1: number | null,
        \\    2: string | null,
        \\  }
    ;

    try std.testing.expectEqualStrings(expect, result);
}

test "generate positional parameter code with any type" {
    const allocator = std.testing.allocator;

    const parameters: []const FieldTypePair = &.{
        .{.field_name = "1", .field_type = null},
        .{.field_name = "2", .field_type = null},
    };

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    const parent_path = try dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(parent_path);

    var builder = try Self.init(allocator, parent_path, "foo");
    defer builder.deinit();

    try builder.applyPlaceholder(parameters);

    const apply_result = builder.parameters;
    try std.testing.expect(apply_result != null);

    const result = apply_result.?;

    const expect = 
        \\  export type Parameter = {
        \\    1: any | null,
        \\    2: any | null,
        \\  }
    ;

    try std.testing.expectEqualStrings(expect, result);
}

test "Output build result" {
    const allocator = std.testing.allocator;

    var output_dir = std.testing.tmpDir(.{});
    defer output_dir.cleanup();
    const parent_path = try output_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(parent_path);

    var builder = try Self.init(allocator, parent_path, "foo");
    defer builder.deinit();

    builder.query = try allocator.dupe(u8, "select $1::id, $2::name from foo where value = $3::value");
    builder.parameters = try allocator.dupe(u8,"export type P = { id:number|null, name:string|null, value:string|null}");

    try builder.build();

    var dir = try output_dir.dir.openDir("foo", .{});
    defer dir.close();

    query: {
        var file = try output_dir.dir.openFile("foo/query.sql", .{.mode = .read_only});
        defer file.close();

        const meta = try file.metadata();
        const content = try file.readToEndAlloc(allocator, meta.size());
        defer allocator.free(content);

        try std.testing.expectEqualStrings(builder.query.?, content);

        break :query;
    }
    placeholder: {
        var file = try output_dir.dir.openFile("foo/types.ts", .{});
        defer file.close();

        const meta = try file.metadata();
        const content = try file.readToEndAlloc(allocator, meta.size());
        defer allocator.free(content);

        try std.testing.expect(
            std.mem.containsAtLeast(u8, content, 1, builder.parameters.?)
        );

        break :placeholder;
    }
}