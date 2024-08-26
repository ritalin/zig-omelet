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
entries: ResultEntryMap,
user_type_names: std.BufSet,
anon_user_types: std.StringHashMap(UserTypeDef),

pub fn init(allocator: std.mem.Allocator) !*Self {
    const self = try allocator.create(Self);

    self.* = .{
        .allocator = allocator,
        .entries = ResultEntryMap{},
        .user_type_names = std.BufSet.init(allocator),
        .anon_user_types = std.StringHashMap(UserTypeDef).init(allocator),
    };

    return self;
}

fn toPascalCasePath(allocator: std.mem.Allocator, file_path: FilePath) !FilePath {
    var iter = std.mem.splitBackwards(u8, file_path, std.fs.path.sep_str);

    const last = try IdentifierFormatter.format(allocator, iter.first(), .pascal_case);
    defer allocator.free(last);

    const rest = iter.rest();

    return if (rest.len > 0) std.fs.path.join(allocator, &.{rest, last}) else allocator.dupe(u8, last);
}

pub fn deinit(self: *Self) void {
    inline for (std.meta.tags(std.meta.FieldEnum(Target))) |tag| {
        if (self.entries.get(tag)) |v| {
            self.allocator.free(v);
        }
    }
    self.user_type_names.deinit();
    self.anon_user_types.deinit();

    self.allocator.destroy(self);
}

pub fn applyQuery(self: *Self, query: Symbol) !void {
    self.entries.put(.query, try self.allocator.dupe(u8, query));
}

fn writeLiteral(writer: *std.ArrayList(u8).Writer, text: Symbol) !void {
    try writer.writeByte('\'');
    try writer.writeAll(text);
    try writer.writeByte('\'');
}

fn buildUserTypeMemberRecursive(allocator: std.mem.Allocator, writer: *std.ArrayList(u8).Writer, user_type: UserTypeDef, user_type_names: std.BufSet) !void {
    if (user_type_names.contains(user_type.header.name)) {
        return writer.writeAll(user_type.header.name);
    }

    switch (user_type.header.kind) {
        .@"enum" => {
            if (user_type.fields.len == 0) return;
            try writeLiteral(writer, user_type.fields[0].field_name);
            if (user_type.fields.len == 1) return;

            for (user_type.fields[1..]) |field| {
                try writer.writeAll(" | ");
                try writeLiteral(writer, field.field_name);
            }
        },
        .array => {
            std.debug.assert((user_type.fields.len == 1) and (user_type.fields[0].field_type != null));

            try buildUserTypeMemberRecursive(allocator, writer, user_type.fields[0].field_type.?, user_type_names);
            try writer.writeAll("[]");
        },
        .primitive => {
            const key = try std.ascii.allocUpperString(allocator, user_type.header.name);
            const ts_type = TypeMappingRules.get(key) orelse {
                return error.UnsupportedDbType;
            };
            try writer.writeAll(ts_type);
        },
    }
}

fn buildUserTypeMemberInternal(
    allocator: std.mem.Allocator, 
    writer: *std.ArrayList(u8).Writer, 
    user_type: UserTypeDef, 
    user_type_names: std.BufSet, 
    opt: std.enums.EnumFieldStruct(enum{anon}, bool, false)) !void 
{
    if (user_type.fields.len == 0) {
        try writer.writeAll("undefined");
        return;
    }

    var buf = std.ArrayList(u8).init(allocator);
    defer buf.deinit();
    var member_writer = buf.writer();

    try buildUserTypeMemberRecursive(allocator, &member_writer, user_type, user_type_names);
    
    if (opt.anon) {
        try writer.writeAll(buf.items);
    }
    else {
        const type_name = try IdentifierFormatter.format(allocator, user_type.header.name, .pascal_case);
        try  writer.print("({s}) & {{_brand: '{s}'}}", .{buf.items, type_name});
    }
}

fn buildAnonymousTypeMember(allocator: std.mem.Allocator, user_type: UserTypeDef, user_type_names: std.BufSet) !Symbol {
    var buf = std.ArrayList(u8).init(allocator);
    defer buf.deinit();
    var writer = buf.writer();

    try buildUserTypeMemberInternal(allocator, &writer, user_type, user_type_names, .{.anon = true});

    return buf.toOwnedSlice();
}

fn buildTypeMember(allocator: std.mem.Allocator, field_type: Symbol, user_type_names: std.BufSet, anon_user_types: std.StringHashMap(UserTypeDef)) !Symbol {
    return ts_type: {
        if (user_type_names.contains(field_type)) {
            // predefined user type
            break:ts_type field_type;
        }
        else if (anon_user_types.get(field_type)) |anon_type| {
            // anonymous user type
            break:ts_type try buildAnonymousTypeMember(allocator, anon_type, user_type_names);
        }
        else {
            // builtin type
            const key = try std.ascii.allocUpperString(allocator, field_type);
            break:ts_type TypeMappingRules.get(key) orelse {
                return error.UnsupportedDbType;
            };
        }
    };
}

pub fn applyPlaceholder(self: *Self, parameters: []const FieldTypePair, user_type_names: std.BufSet, anon_user_types: std.StringHashMap(UserTypeDef)) !void {
    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();
    
    var buf = std.ArrayList(u8).init(self.allocator);
    defer buf.deinit();
    var writer = buf.writer();

    const INDENT_LEVEL = 0;
    const temp_allocator = arena.allocator();

    try writer.writeAll(("  " ** INDENT_LEVEL) ++ "export type Parameter = {\n");

    // `  1: number | null`,
    for (parameters) |p| {
        const field = name: {
            if (try isLiteral(temp_allocator, p.field_name)) {
                break:name try IdentifierFormatter.format(temp_allocator, p.field_name, .camel_case);
            }
            else {
                break:name try std.fmt.allocPrint(temp_allocator, "\"{s}\"", .{p.field_name});
            }
        };
        const ts_type = ts_type: {
            if (p.field_type) |t| {
                break:ts_type try buildTypeMember(temp_allocator, t, user_type_names, anon_user_types);
            }
            else {
                break:ts_type "any";
            }
        };
        try writer.print(("  " ** (INDENT_LEVEL+1)) ++ "{s}: {s} | null,\n", .{field, ts_type});
    }

    try writer.writeAll(("  " ** INDENT_LEVEL) ++ "}");
    
    self.entries.put(.parameter, try buf.toOwnedSlice());
}

pub fn applyPlaceholderOrder(self: *Self, orders: []const Symbol) !void {
    var buf = std.ArrayList(u8).init(self.allocator);
    defer buf.deinit();
    var writer = buf.writer();
    
    try writer.writeAll("export const ParameterOrder: (keyof Parameter)[] = ");
    try writer.writeByte('[');

    if (orders.len > 0) {
        try writeLiteral(&writer, orders[0]);
    }
    if (orders.len > 1) {
        for (orders[1..]) |order| {
            try writer.writeAll(", ");
            try writeLiteral(&writer, order);
        }
    }
    try writer.writeByte(']');

    self.entries.put(.parameter_order, try buf.toOwnedSlice());
}

pub fn applyResultSets(self: *Self, result_set: []const ResultSetColumn, user_type_names: std.BufSet, anon_user_types: std.StringHashMap(UserTypeDef)) !void {
    if (result_set.len == 0) return;

    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();

    var buf = std.ArrayList(u8).init(self.allocator);
    defer buf.deinit();
    var writer = buf.writer();
    
    const INDENT_LEVEL = 0;
    const temp_allocator = arena.allocator();

    try writer.writeAll((" " ** INDENT_LEVEL) ++ "export type ResultSet = {\n");

    // a: number | null
    for (result_set) |c| {
        const field = name: {
            if (try isLiteral(temp_allocator, c.field_name)) {
                break:name try IdentifierFormatter.format(temp_allocator, c.field_name, .camel_case);
            }
            else {
                break:name try std.fmt.allocPrint(temp_allocator, "\"{s}\"", .{c.field_name});
            }
        };
        const ts_type = try buildTypeMember(temp_allocator, c.field_type, user_type_names, anon_user_types);

        try writer.print(("  " ** (INDENT_LEVEL+1)) ++ "{s}: {s}{s},\n", .{
            field, ts_type,
            if (c.nullable) " | null" else ""
        });
    }
    try writer.writeAll(("  " ** INDENT_LEVEL) ++ "}");
    
    self.entries.put(.result_set, try buf.toOwnedSlice());
}

fn isLiteral(allocator: std.mem.Allocator, symbol: Symbol) !bool {
    var tz = std.zig.Tokenizer.init(try allocator.dupeZ(u8, symbol));
    
    return tz.next().loc.end == symbol.len;
}

pub fn applyUserType(self: *Self, user_type: UserTypeDef) !void {
    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();

    var buf = std.ArrayList(u8).init(self.allocator);
    defer buf.deinit();
    var writer = buf.writer();
    
    const INDENT_LEVEL = 0;

    try writer.print((" " ** INDENT_LEVEL) ++ "export type {s} = ", .{
        try IdentifierFormatter.format(arena.allocator(), user_type.header.name, .pascal_case)
    });

    try buildUserTypeMemberInternal(arena.allocator(), &writer, user_type, self.user_type_names, .{});

    self.entries.put(.user_type, try buf.toOwnedSlice());
}

pub fn applyBoundUserType(self: *Self, user_type_names: []const Symbol) !void {
    try self.user_type_names.hash_map.ensureTotalCapacity(@intCast(user_type_names.len));

    for (user_type_names) |name| {
        try self.user_type_names.insert(name);
    }
}

pub fn applyAnonymousUserType(self: *Self, anon_user_types: []const UserTypeDef) !void {
    try self.anon_user_types.ensureTotalCapacity(@intCast(anon_user_types.len));

    for (anon_user_types) |user_type| {
        try self.anon_user_types.put(user_type.header.name, user_type);
    }
}

fn writeQuery(output_dir: std.fs.Dir, query: Symbol) !void {
    var file = try output_dir.createFile("query.sql", .{});
    defer file.close();

    try file.writeAll(query);
}

fn writeTypescriptTypes(writer: *std.fs.File.Writer, type_defs: []const ?Symbol) !void {
    for (type_defs, 0..) |t_, i| {
        if (i > 0) try writer.writeByte('\n');

        if (t_) |type_def| {
            try writer.print("{s}\n", .{type_def});
        }
    }
}

fn writeImports(self: Self, writer: *std.fs.File.Writer, user_type_dir: std.fs.Dir, base_dir_path: FilePath) !void {
    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();
    const tmp_allocator = arena.allocator();

    var names = try tmp_allocator.alloc(Symbol, self.user_type_names.count());
    
    var iter = self.user_type_names.iterator();
    var i: usize = 0;

    while(iter.next()) |name| :(i += 1) {
        names[i] = name.*;
    }

    std.mem.sort(Symbol, names, .{}, 
        struct {
            pub fn lessThan(_: @TypeOf(.{}), lhs: Symbol, rhs: Symbol) bool {
                return std.mem.order(u8, lhs, rhs) == .lt;
            }
        }.lessThan
    );

    const user_type_dir_path = try user_type_dir.realpathAlloc(tmp_allocator, ".");

    for (names) |name| {
        const import_path = try std.fs.path.join(tmp_allocator, &.{user_type_dir_path, name});
        const import_path_rel = try std.fs.path.relative(tmp_allocator, base_dir_path, import_path);
        try writer.print("import {{ {s} }} from '{s}'\n", .{name, import_path_rel});
    }

    if (names.len > 0) try writer.writeByte('\n');
}

pub const OnBuild = *const fn (builder: *CodeBuilder, root_dir: std.fs.Dir, name: core.Symbol) anyerror!ResultStatus;

pub const ResultStatus = enum {
    new_file,
    update_file,
    generate_failed,
};

pub const SourceGenerator = struct {
    pub const log_fmt: Symbol = "{s}/*";

    pub fn build(builder: *CodeBuilder, root_dir: std.fs.Dir, name: core.Symbol) anyerror!ResultStatus {
        const is_new = if (root_dir.statFile(name)) |_| false else |_| true;

        var output_dir = try root_dir.makeOpenPath(name, .{});
        defer output_dir.close();
        const output_dir_path = try output_dir.realpathAlloc(builder.allocator, ".");
        defer builder.allocator.free(output_dir_path);

        if (builder.entries.get(.query)) |query| {
            writeQuery(output_dir, query) catch {
                return error.QueryFileGenerationFailed;
            };
        }
        types: {
            var file = try output_dir.createFile("types.ts", .{});
            defer file.close();
            var writer = file.writer();

            try builder.writeImports(&writer, try UserTypeGenerator.outputDir(root_dir), output_dir_path);

            writeTypescriptTypes(&writer, &.{
                builder.entries.get(.parameter), 
                builder.entries.get(.parameter_order), 
                builder.entries.get(.result_set)
            })
            catch {
                return error.TypeFileGenerationFailed;
            };
            break:types;
        }

        return if (is_new) .new_file else .update_file;
    }
};

pub const UserTypeGenerator = struct {
    pub const output_root: Symbol = "user-types";
    pub const log_fmt: Symbol = output_root ++ "/{s}.ts";

    pub fn build(builder: *CodeBuilder, root_dir: std.fs.Dir, name: core.Symbol) anyerror!ResultStatus {
        var output_dir = try outputDir(root_dir);
        defer output_dir.close();
        const output_dir_path = try output_dir.realpathAlloc(builder.allocator, ".");
        defer builder.allocator.free(output_dir_path);

        const file_name = try std.fmt.allocPrint(builder.allocator, "{s}.ts", .{name});
        defer builder.allocator.free(file_name);

        const is_new = if (output_dir.statFile(file_name)) |_| false else |_| true;

        var file = try output_dir.createFile(file_name, .{});
        defer file.close();
        var writer = file.writer();

        try builder.writeImports(&writer, output_dir, output_dir_path);

        writeTypescriptTypes(&writer, &.{
            builder.entries.get(.user_type)}
        ) 
        catch {
            return error.TypeFileGenerationFailed;
        };

        return if (is_new) .new_file else .update_file;
    }

    pub fn outputDir(root_dir: std.fs.Dir) !std.fs.Dir {
        return root_dir.makeOpenPath(output_root, .{});
    }
};

pub const Target = union(enum) {
    query: Symbol,
    parameter: []const FieldTypePair,
    parameter_order: []const Symbol,
    result_set: []const ResultSetColumn,
    user_type: UserTypeDef,
    bound_user_type: []const Symbol,
    anon_user_type: []const UserTypeDef,
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
    fields: []const Member,

    pub const Kind = enum {@"enum", array, primitive};
    pub const Header = struct {
        kind: Kind,
        name: Symbol,
    };

    pub const Member = struct {
        field_name: Symbol,
        field_type: ?UserTypeDef = null,
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

    fn parsePlaceholderOrder(allocator: std.mem.Allocator, content: Symbol) ![]const Symbol {
        var reader = core.CborStream.Reader.init(content);

        return reader.readSlice(allocator, Symbol); 
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

    fn parseUserTypeDefinitionInternal(allocator: std.mem.Allocator, reader: *core.CborStream.Reader, user_type: *UserTypeDef) !void {
        const HeaderType = struct {Symbol, Symbol};

        const v = try reader.readTuple(core.StructView(HeaderType));
        const header: UserTypeDef.Header = .{ .kind = std.meta.stringToEnum(UserTypeDef.Kind, v[0]).?, .name = v[1] };

        const filed_len = try reader.readSliceHeader();
        var fields = try allocator.alloc(UserTypeDef.Member, filed_len);

        for (0..filed_len) |i| {
            const tuple_len = try reader.readSliceHeader();
            std.debug.assert(tuple_len == 2);

            fields[i].field_name = try reader.readString();
            fields[i].field_type = field_type: {
                if (!try reader.nextNull()) {
                    var field_type: UserTypeDef = undefined;
                    try parseUserTypeDefinitionInternal(allocator, reader, &field_type);
                    break:field_type field_type;
                }
                else {
                    break:field_type try reader.readNull(UserTypeDef);
                }
            };
        }

        user_type.* = .{
            .header = header,
            .fields = fields,
        };
    }

    fn parseUserTypeDefinition(allocator: std.mem.Allocator, content: Symbol) !UserTypeDef {
        var reader = core.CborStream.Reader.init(content);
        var user_type: UserTypeDef = undefined;

        try parseUserTypeDefinitionInternal(allocator, &reader, &user_type);

        return user_type;
    }

    fn parseBoundUserDef(allocator: std.mem.Allocator, content: Symbol) ![]const Symbol {
        var reader = core.CborStream.Reader.init(content);

        return reader.readSlice(allocator, Symbol);
    }

    fn parseAnonymousUserTypeDef(allocator: std.mem.Allocator, content: Symbol) ![]const UserTypeDef {
        var reader = core.CborStream.Reader.init(content);
        
        const values_len = try reader.readSliceHeader();
        var values = try allocator.alloc(UserTypeDef, values_len);

        for (0..values_len) |i| {
            try parseUserTypeDefinitionInternal(allocator, &reader, &values[i]);
        }

        return values;
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
            .{ "query", .query }, 
            .{ "placeholder", .parameter }, .{ "placeholder-order", .parameter_order }, 
            .{ "select-list", .result_set },
            .{ "bound-user-type", .bound_user_type }, .{ "anon-user-type", .anon_user_type },
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
                    .parameter_order => {
                        return .{ .parameter_order = try Parser.parsePlaceholderOrder(self.arena.allocator(), body.content) };
                    },
                    .result_set => {
                        return .{ .result_set = try Parser.parseResultSet(self.arena.allocator(), body.content) };
                    },
                    .user_type => {
                        return .{ .user_type = try Parser.parseUserTypeDefinition(self.arena.allocator(), body.content) };
                    },
                    .bound_user_type => {
                        return .{ .bound_user_type = try Parser.parseBoundUserDef(self.arena.allocator(), body.content) };
                    },
                    .anon_user_type => {
                        return .{ .anon_user_type = try Parser.parseAnonymousUserTypeDef(self.arena.allocator(), body.content) };
                    },
                }
            }

            return null;
        }
    };
};

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

test "parse parameter order" {
    const allocator = std.testing.allocator;
    const arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);

    const expect: []const Symbol = &.{"id", "name", "kind"};
    
    var writer = try core.CborStream.Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeSlice(Symbol, expect);

    const source_bodies: []const core.Event.Payload.TopicBody.Item = &.{.{
        .topic = "placeholder-order",
        .content = writer.buffer.items,
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
        try std.testing.expectEqual(.parameter_order, std.meta.activeTag(result));
        try std.testing.expectEqualDeep(expect, result.parameter_order);
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

fn userTypeToCborInternal(writer: *core.CborStream.Writer, user_type: UserTypeDef) !void {
    header: {
        _ = try writer.writeSliceHeader(2);
        _ = try writer.writeString(@tagName(user_type.header.kind));
        _ = try writer.writeString(user_type.header.name);
        break:header;
    }
    bodies: {
        _ = try writer.writeSliceHeader(user_type.fields.len);

        for (user_type.fields) |c| {
            _ = try writer.writeSliceHeader(2);
            _ = try writer.writeString(c.field_name);

            if (c.field_type) |ft| {
                try userTypeToCborInternal(writer, ft);
            }
            else {
                _ = try writer.writeNull();
            }
        }
        break:bodies;
    }
}

fn userTypeToCbor(allocator: std.mem.Allocator, user_type: UserTypeDef) !Symbol {
    var writer = try core.CborStream.Writer.init(allocator);
    defer writer.deinit();

    try userTypeToCborInternal(&writer, user_type);

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

test "parse bound enum user type" {
    const allocator = std.testing.allocator;
    const arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);
    
    const expects: []const Symbol = &.{"Visibility", "Status"};
    
    var writer = try core.CborStream.Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeSlice(Symbol, expects);

    const source_bodies: []const core.Event.Payload.TopicBody.Item = &.{.{
        .topic = "bound-user-type",
        .content = writer.buffer.items,
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
        try std.testing.expectEqual(.bound_user_type, std.meta.activeTag(result));
        try std.testing.expectEqualDeep(expects, result.bound_user_type);
        break:assert;
    }
    assert: {
        const walk_result = try iter.walk();
        try std.testing.expect(walk_result == null);
        break:assert;
    }
}

fn anonymousTypeToCbor(allocator: std.mem.Allocator, user_types: []const UserTypeDef) !Symbol {
    var writer = try core.CborStream.Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeSliceHeader(user_types.len);

    for (user_types) |user_type| {
        try userTypeToCborInternal(&writer, user_type);
    }

    return writer.buffer.toOwnedSlice();
}

test "parse anonymous user type#1 (enum)" {
    const allocator = std.testing.allocator;
    const arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);

    const expects: []const UserTypeDef = &.{
        .{
            .header = .{ .kind = .@"enum", .name = "Visibility" },
            .fields = &.{
                .{.field_name = "hide", .field_type = null}, 
                .{.field_name = "visible", .field_type = null}, 
            },
        },
        .{
            .header = .{ .kind = .@"enum", .name = "Status" },
            .fields = &.{
                .{.field_name = "succes", .field_type = null}, 
                .{.field_name = "failed", .field_type = null}, 
            },
        },
    };

    const source_bodies: []const core.Event.Payload.TopicBody.Item = &.{.{
        .topic = "anon-user-type",
        .content = try anonymousTypeToCbor(arena.allocator(), expects),
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
        try std.testing.expectEqual(.anon_user_type, std.meta.activeTag(result));
        try std.testing.expectEqualDeep(expects, result.anon_user_type);
        break:assert;
    }
    assert: {
        const walk_result = try iter.walk();
        try std.testing.expect(walk_result == null);
        break:assert;
    }
}

test "parse anonymous user type#2 (primitive list)" {
    const allocator = std.testing.allocator;
    const arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);

    const expects: []const UserTypeDef = &.{
        .{
            .header = .{ .kind = .array, .name = "SelList::Array#1" },
            .fields = &.{
                .{.field_name = "Anon::Primitive#1", .field_type = .{.header = .{.kind = .primitive, .name = "INTEGER"}, .fields = &.{}}}, 
            },
        },
    };

    const source_bodies: []const core.Event.Payload.TopicBody.Item = &.{.{
        .topic = "anon-user-type",
        .content = try anonymousTypeToCbor(arena.allocator(), expects),
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
        try std.testing.expectEqual(.anon_user_type, std.meta.activeTag(result));
        try std.testing.expectEqualDeep(expects, result.anon_user_type);
        break:assert;
    }
    assert: {
        const walk_result = try iter.walk();
        try std.testing.expect(walk_result == null);
        break:assert;
    }
}

test "parse anonymous user type#3 (enum list)" {
    const allocator = std.testing.allocator;
    const arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);

    const expects: []const UserTypeDef = &.{
        .{
            .header = .{ .kind = .@"enum", .name = "SelList::Array#1" },
            .fields = &.{
                .{.field_name = "Anon::Enum#1", .field_type = .{.header = .{.kind = .@"enum", .name = "Visibility"}, .fields = &.{}}}, 
            },
        },
    };

    const source_bodies: []const core.Event.Payload.TopicBody.Item = &.{.{
        .topic = "anon-user-type",
        .content = try anonymousTypeToCbor(arena.allocator(), expects),
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
        try std.testing.expectEqual(.anon_user_type, std.meta.activeTag(result));
        try std.testing.expectEqualDeep(expects, result.anon_user_type);
        break:assert;
    }
    assert: {
        const walk_result = try iter.walk();
        try std.testing.expect(walk_result == null);
        break:assert;
    }
}

test "apply bound user type name#1" {
    const allocator = std.testing.allocator;

    const expects: []const Symbol = &.{};

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();

    const parent_path = try dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(parent_path);

    var builder = try CodeBuilder.init(allocator);
    defer builder.deinit();

    try std.testing.expectEqual(0, builder.user_type_names.count());

    try builder.applyBoundUserType(expects);

    try std.testing.expectEqual(0, builder.user_type_names.count());
}

test "apply bound user type name#2" {
    const allocator = std.testing.allocator;

    const expects: []const Symbol = &.{"Visibility", "Status"};

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();

    const parent_path = try dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(parent_path);

    var builder = try CodeBuilder.init(allocator);
    defer builder.deinit();

    try std.testing.expectEqual(0, builder.user_type_names.count());

    try builder.applyBoundUserType(expects);

    try std.testing.expectEqual(2, builder.user_type_names.count());
    try std.testing.expect(builder.user_type_names.contains("Visibility"));
    try std.testing.expect(builder.user_type_names.contains("Status"));
}

test "apply anonymous user type" {
    const allocator = std.testing.allocator;

    const expects: []const UserTypeDef = &.{
        .{
            .header = .{ .kind = .@"enum", .name = "Visibility" },
            .fields = &.{
                .{ .field_name = "hide", .field_type = null },
                .{ .field_name = "visible", .field_type = null },
            }
        },
        .{
            .header = .{ .kind = .@"enum", .name = "Status" },
            .fields = &.{
                .{ .field_name = "failed", .field_type = null },
                .{ .field_name = "success", .field_type = null },
            }
        },
    };

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();

    const parent_path = try dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(parent_path);

    var builder = try CodeBuilder.init(allocator);
    defer builder.deinit();

    try std.testing.expectEqual(0, builder.anon_user_types.count());

    try builder.applyAnonymousUserType(expects);

    try std.testing.expectEqual(expects.len, builder.anon_user_types.count());
    try std.testing.expectEqualDeep(expects[0], builder.anon_user_types.get("Visibility").?);
    try std.testing.expectEqualDeep(expects[1], builder.anon_user_types.get("Status").?);
}

fn runApplyPlaceholder(parameters: []const FieldTypePair, expect: Symbol, user_type_names: std.BufSet, anon_user_types: std.StringHashMap(UserTypeDef)) !void {
    const allocator = std.testing.allocator;

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    const parent_path = try dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(parent_path);

    var builder = try CodeBuilder.init(allocator);
    defer builder.deinit();

    try builder.applyPlaceholder(parameters, user_type_names, anon_user_types);

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
        \\export type Parameter = {
        \\  id: number | null,
        \\  name: string | null,
        \\}
    ;

    var user_type_names = std.BufSet.init(std.testing.allocator);
    defer user_type_names.deinit();

    var anon_user_types = std.StringHashMap(UserTypeDef).init(std.testing.allocator);
    defer anon_user_types.deinit();

    try runApplyPlaceholder(parameters, expect, user_type_names, anon_user_types);
}

test "generate name parameter code#2 (upper case field)" {
    const parameters: []const FieldTypePair = &.{
        .{.field_name = "ID", .field_type = "BIGINT"},
        .{.field_name = "NAME", .field_type = "VARCHAR"},
    };
    const expect = 
        \\export type Parameter = {
        \\  id: number | null,
        \\  name: string | null,
        \\}
    ;

    var user_type_names = std.BufSet.init(std.testing.allocator);
    defer user_type_names.deinit();

    var anon_user_types = std.StringHashMap(UserTypeDef).init(std.testing.allocator);
    defer anon_user_types.deinit();

    try runApplyPlaceholder(parameters, expect, user_type_names, anon_user_types);
}

test "generate name parameter code#3 (lower-case)" {
    const parameters: []const FieldTypePair = &.{
        .{.field_name = "id", .field_type = "int"},
        .{.field_name = "name", .field_type = "varchar"},
    };
    const expect = 
        \\export type Parameter = {
        \\  id: number | null,
        \\  name: string | null,
        \\}
    ;

    var user_type_names = std.BufSet.init(std.testing.allocator);
    defer user_type_names.deinit();

    var anon_user_types = std.StringHashMap(UserTypeDef).init(std.testing.allocator);
    defer anon_user_types.deinit();

    try runApplyPlaceholder(parameters, expect, user_type_names, anon_user_types);
}

test "generate name parameter code#4 (with any type)" {
    const parameters: []const FieldTypePair = &.{
        .{.field_name = "id", .field_type = null},
        .{.field_name = "name", .field_type = null},
    };
    const expect = 
        \\export type Parameter = {
        \\  id: any | null,
        \\  name: any | null,
        \\}
    ;

    var user_type_names = std.BufSet.init(std.testing.allocator);
    defer user_type_names.deinit();

    var anon_user_types = std.StringHashMap(UserTypeDef).init(std.testing.allocator);
    defer anon_user_types.deinit();

    try runApplyPlaceholder(parameters, expect, user_type_names, anon_user_types);
}

test "generate name parameter code#5 (with snake_case)" {
    const parameters: []const FieldTypePair = &.{
        .{.field_name = "user_id", .field_type = "int"},
        .{.field_name = "user_name", .field_type = "text"},
    };
    const expect = 
        \\export type Parameter = {
        \\  userId: number | null,
        \\  userName: string | null,
        \\}
    ;

    var user_type_names = std.BufSet.init(std.testing.allocator);
    defer user_type_names.deinit();

    var anon_user_types = std.StringHashMap(UserTypeDef).init(std.testing.allocator);
    defer anon_user_types.deinit();

    try runApplyPlaceholder(parameters, expect, user_type_names, anon_user_types);
}

test "generate name parameter code#5 (with PascalCase)" {
    const parameters: []const FieldTypePair = &.{
        .{.field_name = "UserId", .field_type = "int"},
        .{.field_name = "UserName", .field_type = "text"},
    };
    const expect = 
        \\export type Parameter = {
        \\  userId: number | null,
        \\  userName: string | null,
        \\}
    ;

    var user_type_names = std.BufSet.init(std.testing.allocator);
    defer user_type_names.deinit();

    var anon_user_types = std.StringHashMap(UserTypeDef).init(std.testing.allocator);
    defer anon_user_types.deinit();

    try runApplyPlaceholder(parameters, expect, user_type_names, anon_user_types);
}

test "generate name parameter code#5 (without alias)" {
    const parameters: []const FieldTypePair = &.{
        .{.field_name = "UserId", .field_type = "int"},
        .{.field_name = "CAST(name AS VARCHAR)", .field_type = "text"},
    };
    const expect = 
        \\export type Parameter = {
        \\  userId: number | null,
        \\  "CAST(name AS VARCHAR)": string | null,
        \\}
    ;

    var user_type_names = std.BufSet.init(std.testing.allocator);
    defer user_type_names.deinit();

    var anon_user_types = std.StringHashMap(UserTypeDef).init(std.testing.allocator);
    defer anon_user_types.deinit();

    try runApplyPlaceholder(parameters, expect, user_type_names, anon_user_types);
}

test "generate positional parameter code" {
    const parameters: []const FieldTypePair = &.{
        .{.field_name = "1", .field_type = "float"},
        .{.field_name = "2", .field_type = "text"},
    };
    const expect = 
        \\export type Parameter = {
        \\  1: number | null,
        \\  2: string | null,
        \\}
    ;

    var user_type_names = std.BufSet.init(std.testing.allocator);
    defer user_type_names.deinit();

    var anon_user_types = std.StringHashMap(UserTypeDef).init(std.testing.allocator);
    defer anon_user_types.deinit();

    try runApplyPlaceholder(parameters, expect, user_type_names, anon_user_types);
}

test "generate positional parameter code with any type" {
    const parameters: []const FieldTypePair = &.{
        .{.field_name = "1", .field_type = null},
        .{.field_name = "2", .field_type = null},
    };
    const expect = 
        \\export type Parameter = {
        \\  1: any | null,
        \\  2: any | null,
        \\}
    ;

    var user_type_names = std.BufSet.init(std.testing.allocator);
    defer user_type_names.deinit();

    var anon_user_types = std.StringHashMap(UserTypeDef).init(std.testing.allocator);
    defer anon_user_types.deinit();

    try runApplyPlaceholder(parameters, expect, user_type_names, anon_user_types);
}

test "generate positional parameter code with enum user type" {
    const parameters: []const FieldTypePair = &.{
        .{.field_name = "vis1", .field_type = "Visibility"},
        .{.field_name = "vis2", .field_type = "Param::Enum#1"},
    };
    const expect = 
        \\export type Parameter = {
        \\  vis1: Visibility | null,
        \\  vis2: 'hide' | 'visible' | null,
        \\}
    ;

    var user_type_names = map: {
        var map = std.BufSet.init(std.testing.allocator);

        try map.insert("Visibility");
        try map.insert("Status");
        break:map map;
    };
    defer user_type_names.deinit();

    var anon_user_types = map: {
        var map = std.StringHashMap(UserTypeDef).init(std.testing.allocator);

        try map.put("Param::Enum#1", .{  
            .header = .{ .kind = .@"enum", .name = "Param::Enum#1" },
            .fields = &.{ .{.field_name = "hide"}, .{.field_name = "visible"} },
        });
        break:map map;
    };
    defer anon_user_types.deinit();

    try runApplyPlaceholder(parameters, expect, user_type_names, anon_user_types);
}

test "generate parameter order" {
    const allocator = std.testing.allocator;

    const orders: []const Symbol = &.{"id", "name", "kind"};
    const expect = "export const ParameterOrder: (keyof Parameter)[] = ['id', 'name', 'kind']";

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();

    const parent_path = try dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(parent_path);

    var builder = try CodeBuilder.init(allocator);
    defer builder.deinit();

    try builder.applyPlaceholderOrder(orders);

    const apply_result = builder.entries.get(.parameter_order);
    try std.testing.expect(apply_result != null);
    try std.testing.expectEqualStrings(expect, apply_result.?);
}

fn runApplyResultSets(parameters: []const ResultSetColumn, expect: Symbol, user_type_names: std.BufSet, anon_user_types: std.StringHashMap(UserTypeDef)) !void {
    const allocator = std.testing.allocator;

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();

    const parent_path = try dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(parent_path);

    var builder = try CodeBuilder.init(allocator);
    defer builder.deinit();

    try builder.applyResultSets(parameters, user_type_names, anon_user_types);

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
        \\export type ResultSet = {
        \\  id: number,
        \\  kind: number,
        \\  value: string,
        \\}
    ;

    var user_type_names = std.BufSet.init(std.testing.allocator);
    defer user_type_names.deinit();

    var anon_user_types = std.StringHashMap(UserTypeDef).init(std.testing.allocator);
    defer anon_user_types.deinit();

    try runApplyResultSets(result_set, expect, user_type_names, anon_user_types);
}

test "generate select list#2 (PascalCase field)" {
    const result_set: []const ResultSetColumn = &.{
        .{.field_name = "userId", .field_type = "INTEGER", .nullable = false},
        .{.field_name = "profileKind", .field_type = "INTEGER", .nullable = false},
        .{.field_name = "remarks", .field_type = "VARCHAR", .nullable = false},
    };
    const expect = 
        \\export type ResultSet = {
        \\  userId: number,
        \\  profileKind: number,
        \\  remarks: string,
        \\}
    ;

    var user_type_names = std.BufSet.init(std.testing.allocator);
    defer user_type_names.deinit();

    var anon_user_types = std.StringHashMap(UserTypeDef).init(std.testing.allocator);
    defer anon_user_types.deinit();

    try runApplyResultSets(result_set, expect, user_type_names, anon_user_types);
}

test "generate select list#3 (lower snake_case field)" {
    const result_set: []const ResultSetColumn = &.{
        .{.field_name = "user_id", .field_type = "INTEGER", .nullable = false},
        .{.field_name = "profile_kind", .field_type = "INTEGER", .nullable = false},
        .{.field_name = "remarks", .field_type = "VARCHAR", .nullable = false},
    };
    const expect = 
        \\export type ResultSet = {
        \\  userId: number,
        \\  profileKind: number,
        \\  remarks: string,
        \\}
    ;

    var user_type_names = std.BufSet.init(std.testing.allocator);
    defer user_type_names.deinit();

    var anon_user_types = std.StringHashMap(UserTypeDef).init(std.testing.allocator);
    defer anon_user_types.deinit();

    try runApplyResultSets(result_set, expect, user_type_names, anon_user_types);
}

test "generate select list#4 (upper snake_case field)" {
    const result_set: []const ResultSetColumn = &.{
        .{.field_name = "USER_ID", .field_type = "INTEGER", .nullable = false},
        .{.field_name = "PROFILE_KIND", .field_type = "INTEGER", .nullable = false},
        .{.field_name = "REMARKS", .field_type = "VARCHAR", .nullable = false},
    };
    const expect = 
        \\export type ResultSet = {
        \\  userId: number,
        \\  profileKind: number,
        \\  remarks: string,
        \\}
    ;

    var user_type_names = std.BufSet.init(std.testing.allocator);
    defer user_type_names.deinit();

    var anon_user_types = std.StringHashMap(UserTypeDef).init(std.testing.allocator);
    defer anon_user_types.deinit();

    try runApplyResultSets(result_set, expect, user_type_names, anon_user_types);
}

test "generate select list#5 (nullable field)" {
    const result_set: []const ResultSetColumn = &.{
        .{.field_name = "id", .field_type = "INTEGER", .nullable = false},
        .{.field_name = "kind", .field_type = "INTEGER", .nullable = true},
        .{.field_name = "value", .field_type = "VARCHAR", .nullable = true},
    };
    const expect = 
        \\export type ResultSet = {
        \\  id: number,
        \\  kind: number | null,
        \\  value: string | null,
        \\}
    ;

    var user_type_names = std.BufSet.init(std.testing.allocator);
    defer user_type_names.deinit();

    var anon_user_types = std.StringHashMap(UserTypeDef).init(std.testing.allocator);
    defer anon_user_types.deinit();

    try runApplyResultSets(result_set, expect, user_type_names, anon_user_types);
}

test "generate select list#6 (field without alias)" {
    const result_set: []const ResultSetColumn = &.{
        .{.field_name = "id", .field_type = "INTEGER", .nullable = false},
        .{.field_name = "kind", .field_type = "INTEGER", .nullable = true},
        .{.field_name = "CAST($val AS VARCHAR)", .field_type = "VARCHAR", .nullable = true},
    };
    const expect = 
        \\export type ResultSet = {
        \\  id: number,
        \\  kind: number | null,
        \\  "CAST($val AS VARCHAR)": string | null,
        \\}
    ;

    var user_type_names = std.BufSet.init(std.testing.allocator);
    defer user_type_names.deinit();

    var anon_user_types = std.StringHashMap(UserTypeDef).init(std.testing.allocator);
    defer anon_user_types.deinit();

    try runApplyResultSets(result_set, expect, user_type_names, anon_user_types);
}

test "generate select list#7 (with enum user type)" {
    const result_set: []const ResultSetColumn = &.{
        .{.field_name = "id", .field_type = "INTEGER", .nullable = false},
        .{.field_name = "vis1", .field_type = "SelList::Enum#1", .nullable = true},
        .{.field_name = "vis2", .field_type = "Visibility", .nullable = false},
    };
    const expect = 
        \\export type ResultSet = {
        \\  id: number,
        \\  vis1: 'hide' | 'visible' | null,
        \\  vis2: Visibility,
        \\}
    ;

    var user_type_names = map: {
        var map = std.BufSet.init(std.testing.allocator);

        try map.insert("Visibility");
        try map.insert("Status");
        break:map map;
    };
    defer user_type_names.deinit();

    var anon_user_types = map: {
        var map = std.StringHashMap(UserTypeDef).init(std.testing.allocator);

        try map.put("SelList::Enum#1", .{  
            .header = .{ .kind = .@"enum", .name = "SelList::Enum#1" },
            .fields = &.{ .{.field_name = "hide"}, .{.field_name = "visible"} },
        });
        break:map map;
    };
    defer anon_user_types.deinit();

    try runApplyResultSets(result_set, expect, user_type_names, anon_user_types);
}

test "generate select list#8 (primitive list)" {
    const result_set: []const ResultSetColumn = &.{
        .{.field_name = "id", .field_type = "INTEGER", .nullable = false},
        .{.field_name = "numbers", .field_type = "SelList::Array#2", .nullable = true},
    };
    const expect = 
        \\export type ResultSet = {
        \\  id: number,
        \\  numbers: number[] | null,
        \\}
    ;

    var user_type_names = map: {
        const map = std.BufSet.init(std.testing.allocator);
        break:map map;
    };
    defer user_type_names.deinit();

    var anon_user_types = map: {
        var map = std.StringHashMap(UserTypeDef).init(std.testing.allocator);

        try map.put("SelList::Array#2", .{  
            .header = .{ .kind = .array, .name = "Anon::Primitive#1" },
            .fields = &.{ .{.field_name = "Anon::Primitive#1", .field_type = .{.header = .{.kind = .primitive, .name = "INTEGER"}, .fields = &.{} }} },
        });
        break:map map;
    };
    defer anon_user_types.deinit();

    try runApplyResultSets(result_set, expect, user_type_names, anon_user_types);
}

test "generate select list#8 (predefined rnum list)" {
    const result_set: []const ResultSetColumn = &.{
        .{.field_name = "id", .field_type = "INTEGER", .nullable = false},
        .{.field_name = "vis2", .field_type = "SelList::Array#2", .nullable = true},
    };
    const expect = 
        \\export type ResultSet = {
        \\  id: number,
        \\  vis2: Visibility[] | null,
        \\}
    ;

    var user_type_names = map: {
        var map = std.BufSet.init(std.testing.allocator);
        try map.insert("Visibility");
        break:map map;
    };
    defer user_type_names.deinit();

    var anon_user_types = map: {
        var map = std.StringHashMap(UserTypeDef).init(std.testing.allocator);

        try map.put("SelList::Array#2", .{  
            .header = .{ .kind = .array, .name = "Anon::Enum#1" },
            .fields = &.{ .{.field_name = "Anon::Enum#1", .field_type = .{.header = .{.kind = .@"enum", .name = "Visibility"}, .fields = &.{} }} },
        });
        break:map map;
    };
    defer anon_user_types.deinit();

    try runApplyResultSets(result_set, expect, user_type_names, anon_user_types);
}

fn runApplyUserType(enum_type: UserTypeDef, expect: Symbol) !void {
    const allocator = std.testing.allocator;

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    const parent_path = try dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(parent_path);

    var builder = try CodeBuilder.init(allocator);
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
        \\export type Visibility = ('hide' | 'visible') & {_brand: 'Visibility'}
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
        \\export type Visibility = ('hide' | 'visible') & {_brand: 'Visibility'}
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
        \\export type Visibility = ('hide' | 'visible') & {_brand: 'Visibility'}
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
        \\export type UserProfileKind = ('admin' | 'general') & {_brand: 'UserProfileKind'}
    ;

    try runApplyUserType(enum_type, expect);
}

test "Output build result#1" {
    const allocator = std.testing.allocator;

    var output_dir = std.testing.tmpDir(.{});
    defer output_dir.cleanup();

    var builder = try CodeBuilder.init(allocator);
    defer builder.deinit();

    builder.entries.put(.query, try allocator.dupe(u8, "select $1::id, $2::name from foo where value = $3::value"));
    builder.entries.put(.parameter, try allocator.dupe(u8,"export type P = { id:number|null, name:string|null, value:string|null}"));
    builder.entries.put(.parameter_order, try allocator.dupe(u8,"export const O: (keyof P)[] = ['id', 'name', 'vis', 'status']"));
    builder.entries.put(.result_set, try allocator.dupe(u8, "export type R = { id:number, name:string|null }"));
    _ = try SourceGenerator.build(builder, output_dir.dir, "Foo");

    query: {
        var file = try output_dir.dir.openFile("Foo/query.sql", .{.mode = .read_only});
        defer file.close();

        const meta = try file.metadata();
        const content = try file.readToEndAlloc(allocator, meta.size());
        defer allocator.free(content);

        try std.testing.expectEqualStrings(builder.entries.get(.query).?, content);

        break:query;
    }
    placeholder: {
        var file = try output_dir.dir.openFile("Foo/types.ts", .{});
        defer file.close();
        var reader = file.reader();

        const meta = try file.metadata();
        const file_size = meta.size();

        expect_placeholder: {
            const line = try reader.readUntilDelimiterOrEofAlloc(allocator, '\n', file_size);
            defer if (line) |x| allocator.free(x);
            try std.testing.expect(line != null);
            try std.testing.expectEqualStrings(builder.entries.get(.parameter).?, line.?);
            break:expect_placeholder;
        }
        expect_blank: {
            const line = try reader.readUntilDelimiterOrEofAlloc(allocator, '\n', file_size);
            defer if (line) |x| allocator.free(x);
            try std.testing.expect(line != null);
            try std.testing.expectEqualStrings("", line.?);
            break:expect_blank;
        }
        expect_placeholder_order: {
            const line = try reader.readUntilDelimiterOrEofAlloc(allocator, '\n', file_size);
            defer if (line) |x| allocator.free(x);
            try std.testing.expect(line != null);
            try std.testing.expectEqualStrings(builder.entries.get(.parameter_order).?, line.?);
            break:expect_placeholder_order;
        }
        expect_blank: {
            const line = try reader.readUntilDelimiterOrEofAlloc(allocator, '\n', file_size);
            defer if (line) |x| allocator.free(x);
            try std.testing.expect(line != null);
            try std.testing.expectEqualStrings("", line.?);
            break:expect_blank;
        }
        expect_result_set: {
            const line = try reader.readUntilDelimiterOrEofAlloc(allocator, '\n', file_size);
            defer if (line) |x| allocator.free(x);
            try std.testing.expect(line != null);
            try std.testing.expectEqualStrings(builder.entries.get(.result_set).?, line.?);
            break:expect_result_set;
        }
        expect_eof: {
            try std.testing.expectError(error.EndOfStream, reader.readByte());
            break:expect_eof;
        }
        break:placeholder;
    }
}

test "Output build result#2 (with predefined user type)" {
    const allocator = std.testing.allocator;

    var output_dir = std.testing.tmpDir(.{});
    defer output_dir.cleanup();

    var builder = try CodeBuilder.init(allocator);
    defer builder.deinit();

    builder.entries.put(.parameter, try allocator.dupe(u8,"export type P = { id:number|null, name:string|null, vis:Visibility|null, status: Status|null}"));
    builder.entries.put(.parameter_order, try allocator.dupe(u8,"export const O: (keyof P)[] = ['id', 'name', 'vis', 'status']"));
    builder.entries.put(.result_set, try allocator.dupe(u8, "export type R = { id:number, name:string|null }"));
    try builder.user_type_names.insert("Visibility");
    try builder.user_type_names.insert("Status");

    _ = try SourceGenerator.build(builder, output_dir.dir, "Foo");

    placeholder: {
        var file = try output_dir.dir.openFile("Foo/types.ts", .{});
        defer file.close();
        var reader = file.reader();

        const meta = try file.metadata();
        const file_size = meta.size();

        expect_import: {
            const line = try reader.readUntilDelimiterOrEofAlloc(allocator, '\n', file_size);
            defer if (line) |x| allocator.free(x);
            try std.testing.expect(line != null);
            try std.testing.expectEqualStrings("import { Status } from '../user-types/Status'", line.?);
            break:expect_import;
        }
        expect_import: {
            const line = try reader.readUntilDelimiterOrEofAlloc(allocator, '\n', file_size);
            defer if (line) |x| allocator.free(x);
            try std.testing.expect(line != null);
            try std.testing.expectEqualStrings("import { Visibility } from '../user-types/Visibility'", line.?);
            break:expect_import;
        }
        expect_blank: {
            const line = try reader.readUntilDelimiterOrEofAlloc(allocator, '\n', file_size);
            defer if (line) |x| allocator.free(x);
            try std.testing.expect(line != null);
            try std.testing.expectEqualStrings("", line.?);
            break:expect_blank;
        }
        expect_placeholder: {
            const line = try reader.readUntilDelimiterOrEofAlloc(allocator, '\n', file_size);
            defer if (line) |x| allocator.free(x);
            try std.testing.expect(line != null);
            try std.testing.expectEqualStrings(builder.entries.get(.parameter).?, line.?);
            break:expect_placeholder;
        }
        expect_blank: {
            const line = try reader.readUntilDelimiterOrEofAlloc(allocator, '\n', file_size);
            defer if (line) |x| allocator.free(x);
            try std.testing.expect(line != null);
            try std.testing.expectEqualStrings("", line.?);
            break:expect_blank;
        }
        expect_placeholder_order: {
            const line = try reader.readUntilDelimiterOrEofAlloc(allocator, '\n', file_size);
            defer if (line) |x| allocator.free(x);
            try std.testing.expect(line != null);
            try std.testing.expectEqualStrings(builder.entries.get(.parameter_order).?, line.?);
            break:expect_placeholder_order;
        }
        expect_blank: {
            const line = try reader.readUntilDelimiterOrEofAlloc(allocator, '\n', file_size);
            defer if (line) |x| allocator.free(x);
            try std.testing.expect(line != null);
            try std.testing.expectEqualStrings("", line.?);
            break:expect_blank;
        }
        expect_result_set: {
            const line = try reader.readUntilDelimiterOrEofAlloc(allocator, '\n', file_size);
            defer if (line) |x| allocator.free(x);
            try std.testing.expect(line != null);
            try std.testing.expectEqualStrings(builder.entries.get(.result_set).?, line.?);
            break:expect_result_set;
        }
        expect_eof: {
            try std.testing.expectError(error.EndOfStream, reader.readByte());
            break:expect_eof;
        }
        break:placeholder;
    }
}

test "Output build enum user type" {
    const allocator = std.testing.allocator;

    var output_dir = std.testing.tmpDir(.{});
    defer output_dir.cleanup();

    var builder = try CodeBuilder.init(allocator);
    defer builder.deinit();

    builder.entries.put(.user_type, try allocator.dupe(u8, "export type Visibility = ('hide' | 'visible') & {_brand = 'Visibility'}"));

    _ = try UserTypeGenerator.build(builder, output_dir.dir, "Foo");

    user_type: {
        var file = try output_dir.dir.openFile("user-types/Foo.ts", .{});
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
