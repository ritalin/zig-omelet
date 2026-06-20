const std = @import("std");
const core = @import("core");
const cbor = @import("cbor");

const events = core.events;

const Symbol = core.types.Symbol;
const BinaryData = core.types.BinaryData;
const FilePath = core.types.FilePath;

const Event = core.events.Event;
const GenerateStatus = Event.Payload.GenerateResponse.Status;

const Self = @This();
const CodeBuilder = Self;

const ResultEntryMap = std.enums.EnumMap(std.meta.FieldEnum(Target), Symbol);

const Namespace = "Sql";
const ToArrayFn = "export const toArray = (parameter: Parameter) => Object.values(parameter)";

const IdentifierFormatter = @import("./IdentifierFormatter.zig");

entries: ResultEntryMap,
user_type_names: std.BufSet,
anon_user_types: std.StringHashMap(UserTypeDef),

pub fn init(allocator: std.mem.Allocator) Self {
    return .{
        .entries = ResultEntryMap{},
        .user_type_names = std.BufSet.init(allocator),
        .anon_user_types = std.StringHashMap(UserTypeDef).init(allocator),
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
    self.user_type_names.deinit();
    self.anon_user_types.deinit();
}

const TargetFields = std.enums.EnumFieldStruct(std.meta.FieldEnum(CodeBuilder.Target), bool, false);

pub fn apply(self: *CodeBuilder, allocator: std.mem.Allocator, walker: *CodeBuilder.Parser.ResultWalker) !void {
    var passed_set: TargetFields = .{};
    try self.applyInternal(allocator, walker, &passed_set);
}

fn applyInternal(self: *CodeBuilder, allocator: std.mem.Allocator, walker: *CodeBuilder.Parser.ResultWalker, passed_set: *TargetFields) !void {
    while (try walker.walk(allocator)) |target| switch (target) {
        .query => |q| {
            try self.applyQuery(q);
            passed_set.query = true;
        },
        .parameter => |placeholder| {
            if (!passed_set.bound_user_type) {
                try self.applyInternal(allocator, walker, passed_set);
            }
            if (!passed_set.anon_user_type) {
                try self.applyInternal(allocator, walker, passed_set);
            }

            try self.applyPlaceholder(allocator, placeholder, self.user_type_names, self.anon_user_types);
            passed_set.parameter = true;
        },
        .parameter_order => |orders| {
            try self.applyPlaceholderOrder(allocator, orders);
            passed_set.parameter_order = true;
        },
        .result_set => |field_types| {
            if (!passed_set.bound_user_type) {
                try self.applyInternal(allocator, walker, passed_set);
            }
            if (!passed_set.anon_user_type) {
                try self.applyInternal(allocator, walker, passed_set);
            }

            try self.applyResultSets(allocator, field_types, self.user_type_names, self.anon_user_types);
            passed_set.result_set = true;
        },
        .user_type => |definition| {
            if (!passed_set.bound_user_type) {
                try self.applyInternal(allocator, walker, passed_set);
            }
            if (!passed_set.anon_user_type) {
                try self.applyInternal(allocator, walker, passed_set);
            }

            try self.applyUserType(allocator, definition);
            passed_set.user_type = true;
        },
        .bound_user_type => |names| {
            try self.applyBoundUserType(names);
            passed_set.bound_user_type = true;
        },
        .anon_user_type => |definitions| {
            try self.applyAnonymousUserType(definitions);
            passed_set.anon_user_type = true;
        },
    };
}

fn applyQuery(self: *Self, query: Symbol) !void {
    self.entries.put(.query, query);
}

fn writeLiteral(writer: *std.Io.Writer, text: Symbol) !void {
    try writer.writeByte('\'');
    try writer.writeAll(text);
    try writer.writeByte('\'');
}

fn buildUserTypeMemberRecursive(
    allocator: std.mem.Allocator, 
    indent_level: usize,
    writer: *std.Io.Writer, 
    user_type: UserTypeDef, 
    user_type_names: std.BufSet, 
    anon_user_types: std.StringHashMap(UserTypeDef)) anyerror!void 
{
    switch (user_type.header.kind) {
        .@"enum" => {
            if (user_type.fields.len == 0) return;

            try writeLiteral(writer, user_type.fields[0].field_name);
            if (user_type.fields.len > 1) {
                for (user_type.fields[1..]) |field| {
                    try writer.writeAll(" | ");
                    try writeLiteral(writer, field.field_name);
                }
            }
        },
        .@"struct" => {
            try writer.writeAll("{\n");
            for (user_type.fields) |field| {
                const field_name = name: {
                    if (try isLiteral(allocator, field.field_name)) {
                        break:name try IdentifierFormatter.format(allocator, field.field_name, .camel_case);
                    }
                    else {
                        break:name try std.fmt.allocPrint(allocator, "\"{s}\"", .{field.field_name});
                    }
                };

                try writer.splatBytesAll("  ", indent_level+1);
                try writer.print("{s}: ", .{field_name});
                if (field.field_type) |field_type| {
                    const field_type_name = try buildTypeMember(allocator, indent_level+1, field_type.header.name, user_type_names, anon_user_types, .{.always_null = true});
                    try writer.print("{s};", .{field_type_name});
                }
                else {
                     try writer.writeAll("any;");
                }
                try writer.writeAll("\n");
            }
            
            try writer.splatBytesAll("  ", indent_level);
            try writer.writeAll("}");
        },
        .array => {
            std.debug.assert((user_type.fields.len == 1));

            if (user_type.fields[0].field_type) |field_type| {
                const field_type_name = try buildTypeMember(allocator, indent_level, field_type.header.name, user_type_names, anon_user_types, .{});
                try writer.writeAll(field_type_name);
            }
            else if (user_type.header.type_category == .anonymous) {
                const field_type_name = try buildTypeMember(allocator, indent_level, user_type.fields[0].field_name, user_type_names, anon_user_types, .{});
                try writer.print("({s})", .{field_type_name});
            }
            else {
                try writer.writeAll("any");
            }
            try writer.writeAll("[]");
        },
        .alias => {
            std.debug.assert((user_type.fields.len == 1) and (user_type.fields[0].field_type != null));

            const field_type_name = try buildTypeMember(allocator, indent_level, user_type.fields[0].field_type.?.header.name, user_type_names, anon_user_types, .{});
            try writer.writeAll(field_type_name);
        },
        .primitive => {
            const key = try std.ascii.allocUpperString(allocator, user_type.header.name);
            const ts_type = TypeMappingRules.get(key) orelse {
                return error.UnsupportedDbType;
            };
            try writer.writeAll(ts_type);
        },
        .user => {
            unreachable;
        },
    }
}

const UserTypeMemberOptions = std.enums.EnumFieldStruct(enum{always_null}, bool, false);

fn buildTypeMember(
    allocator: std.mem.Allocator, 
    indent_level: usize, 
    field_type: Symbol, 
    user_type_names: std.BufSet, 
    anon_user_types: std.StringHashMap(UserTypeDef),
    opt: UserTypeMemberOptions) !Symbol 
{
    // It does not deinit beacouse of supplying from the arena.
    var buffer = std.Io.Writer.Allocating.init(allocator);

    if (user_type_names.contains(field_type)) {
        // predefined user type
        const ts_type = try IdentifierFormatter.format(allocator, field_type, .pascal_case);
        try buffer.writer.writeAll(ts_type);
    }
    else if (anon_user_types.get(field_type)) |anon_type| {
        // anonymous user type
        if (anon_type.fields.len == 0) {
            try buffer.writer.writeAll("undefined");
        }
        try buildUserTypeMemberRecursive(allocator, indent_level, &buffer.writer, anon_type, user_type_names, anon_user_types);
    }
    else {
        // builtin type
        const key = try std.ascii.allocUpperString(allocator, field_type);
        const ts_type = TypeMappingRules.get(key) orelse {
            return error.UnsupportedDbType;
        };
        try buffer.writer.writeAll(ts_type);
    }

    if (opt.always_null) {
        try buffer.writer.writeAll(" | null");
    }

    try buffer.writer.flush();

    return buffer.written();
}

fn applyPlaceholder(self: *Self, allocator: std.mem.Allocator, parameters: []const FieldTypePair, user_type_names: std.BufSet, anon_user_types: std.StringHashMap(UserTypeDef)) !void {
    // It does not deinit beacouse of supplying from the arena.
    var buffer = std.Io.Writer.Allocating.init(allocator);

    const INDENT_LEVEL = 0;

    try buffer.writer.writeAll(("  " ** INDENT_LEVEL) ++ "export type Parameter = {\n");

    // `  1: number | null`,
    for (parameters) |p| {
        const field = name: {
            if (try isLiteral(allocator, p.field_name)) {
                break:name try IdentifierFormatter.format(allocator, p.field_name, .camel_case);
            }
            else {
                break:name try std.fmt.allocPrint(allocator, "\"{s}\"", .{p.field_name});
            }
        };
        const ts_type = ts_type: {
            if (p.field_type) |t| {
                break:ts_type try buildTypeMember(allocator, INDENT_LEVEL, t, user_type_names, anon_user_types, .{.always_null = true});
            }
            else {
                break:ts_type "any";
            }
        };
        try buffer.writer.print(("  " ** (INDENT_LEVEL+1)) ++ "{s}: {s},\n", .{field, ts_type});
    }

    try buffer.writer.writeAll(("  " ** INDENT_LEVEL) ++ "}");
    
    self.entries.put(.parameter, buffer.written());
}

fn writeOrderSymbol(allocator: std.mem.Allocator, writer: *std.Io.Writer, order: Symbol) !void {
    var is_num = true;
    for (order) |c| {
        if (! std.ascii.isDigit(c)) {
            is_num = false;
            break;
        }
    }

    if (is_num) {
        try writer.writeAll(order);
    }
    else {
        const name = try IdentifierFormatter.format(allocator, order, .camel_case);
        defer allocator.free(name);
        try writeLiteral(writer, name);
    }
}

fn applyPlaceholderOrder(self: *Self, allocator: std.mem.Allocator, orders: []const Symbol) !void {
    // It does not deinit beacouse of supplying from the arena.
    var buffer = std.Io.Writer.Allocating.init(allocator);
    
    try buffer.writer.writeAll("export const ParameterOrder: (keyof Parameter)[] = ");
    try buffer.writer.writeByte('[');

    if (orders.len > 0) {
        try writeOrderSymbol(allocator, &buffer.writer, orders[0]);
    }
    if (orders.len > 1) {
        for (orders[1..]) |order| {
            try buffer.writer.writeAll(", ");
            try writeOrderSymbol(allocator, &buffer.writer, order);
        }
    }
    try buffer.writer.writeByte(']');

    self.entries.put(.parameter_order, buffer.written());
}

fn applyResultSets(self: *Self, allocator: std.mem.Allocator, result_set: []const ResultSetColumn, user_type_names: std.BufSet, anon_user_types: std.StringHashMap(UserTypeDef)) !void {
    if (result_set.len == 0) return;

    // It does not deinit beacouse of supplying from the arena.
    var buffer = std.Io.Writer.Allocating.init(allocator);
    
    const INDENT_LEVEL = 0;

    try buffer.writer.writeAll((" " ** INDENT_LEVEL) ++ "export type ResultSet = {\n");

    // a: number | null
    for (result_set) |c| {
        const field = name: {
            if (try isLiteral(allocator, c.field_name)) {
                break:name try IdentifierFormatter.format(allocator, c.field_name, .camel_case);
            }
            else {
                break:name try std.fmt.allocPrint(allocator, "\"{s}\"", .{c.field_name});
            }
        };
        const ts_type = try buildTypeMember(allocator, INDENT_LEVEL+1, c.field_type, user_type_names, anon_user_types, .{});

        try buffer.writer.print(("  " ** (INDENT_LEVEL+1)) ++ "{s}: {s}{s};\n", .{
            field, ts_type,
            if (c.nullable) " | null" else "",
        });
    }
    try buffer.writer.writeAll(("  " ** INDENT_LEVEL) ++ "}");
    
    self.entries.put(.result_set, buffer.written());
}

fn isLiteral(allocator: std.mem.Allocator, symbol: Symbol) !bool {
    var tz = std.zig.Tokenizer.init(try allocator.dupeZ(u8, symbol));
    
    return tz.next().loc.end == symbol.len;
}

fn applyUserType(self: *Self, allocator: std.mem.Allocator, user_type: UserTypeDef) !void {
    // It does not deinit beacouse of supplying from the arena.
    var buffer = std.Io.Writer.Allocating.init(allocator);
    
    const INDENT_LEVEL = 0;

    const type_name = try IdentifierFormatter.format(allocator, user_type.header.name, .pascal_case);

    try buffer.writer.print((" " ** INDENT_LEVEL) ++ "export type {s} = ", .{type_name});

    write_member: {
        var member_buf = std.Io.Writer.Allocating.init(allocator);
        defer member_buf.deinit();
     
        try buildUserTypeMemberRecursive(allocator, INDENT_LEVEL, &member_buf.writer, user_type, self.user_type_names, self.anon_user_types);
        if (user_type.header.kind == .@"enum") {
            try  buffer.writer.print("({s})", .{member_buf.written()});
        }
        else {
            try  buffer.writer.print("{s}", .{member_buf.written()});
        }
        break:write_member;
    }
    write_brand: {
        try  buffer.writer.print(" & {{_brand: '{s}'}}", .{type_name});
        break:write_brand;
    }
    self.entries.put(.user_type, buffer.written());
}

fn applyBoundUserType(self: *Self, user_type_names: []const Symbol) !void {
    try self.user_type_names.hash_map.ensureTotalCapacity(@intCast(user_type_names.len));

    for (user_type_names) |name| {
        try self.user_type_names.insert(name);
    }
}

fn applyAnonymousUserType(self: *Self, anon_user_types: []const UserTypeDef) !void {
    try self.anon_user_types.ensureTotalCapacity(@intCast(anon_user_types.len));

    for (anon_user_types) |user_type| {
        try self.anon_user_types.put(user_type.header.name, user_type);
    }
}

fn writeQuery(io: std.Io, output_dir: std.Io.Dir, query: Symbol) !void {
    var file = try output_dir.createFile(io, "query.sql", .{});
    defer file.close(io);

    try file.writeStreamingAll(io, query);
}

fn writeTypescriptTypes(writer: *std.Io.Writer, type_defs: []const ?Symbol) !void {
    for (type_defs, 0..) |t_, i| {
        if (i > 0) try writer.writeByte('\n');

        if (t_) |type_def| {
            try writer.print("{s}\n", .{type_def});
        }
    }
}

fn writeImports(self: Self, io: std.Io, allocator: std.mem.Allocator, writer: *std.Io.Writer, user_type_dir: std.Io.Dir, base_dir_path: FilePath) !void {
    var names = try allocator.alloc(Symbol, self.user_type_names.count());
    
    var iter = self.user_type_names.iterator();
    var i: usize = 0;

    while(iter.next()) |name| :(i += 1) {
        names[i] = try IdentifierFormatter.format(allocator, name.*, .pascal_case);
    }

    std.mem.sort(Symbol, names, .{}, 
        struct {
            pub fn lessThan(_: @TypeOf(.{}), lhs: Symbol, rhs: Symbol) bool {
                return std.mem.order(u8, lhs, rhs) == .lt;
            }
        }.lessThan
    );

    const user_type_dir_path = try user_type_dir.realPathFileAlloc(io, ".", allocator);

    for (names) |name| {
        const import_path = try std.fs.path.join(allocator, &.{user_type_dir_path, name});
        const import_path_rel = try std.fs.path.relative(allocator, ".", null, base_dir_path, import_path);
        try writer.print("import {{ type {s} }} from '{s}'\n", .{name, import_path_rel});
    }

    if (names.len > 0) try writer.writeByte('\n');
}

pub const OnBuild = *const fn (builder: *CodeBuilder, io: std.Io, allocator: std.mem.Allocator, root_dir: std.Io.Dir, name: core.types.Symbol) anyerror!GenerateStatus;

pub const ResultStatus = enum {
    new_file,
    update_file,
    generate_failed,
};

pub const SourceGenerator = struct {
    pub const success_log_fmt: Symbol = "Successful/name: {s}, dialect: {s}, category: {s}";

    pub fn build(builder: *CodeBuilder, io: std.Io, allocator: std.mem.Allocator, root_dir: std.Io.Dir, name: Symbol) !GenerateStatus {
        const is_new = if (root_dir.statFile(io, name, .{})) |_| false else |_| true;

        var output_dir = try root_dir.createDirPathOpen(io, name, .{});
        defer output_dir.close(io);
        const output_dir_path = try output_dir.realPathFileAlloc(io, ".", allocator);
        defer allocator.free(output_dir_path);

        if (builder.entries.get(.query)) |query| {
            writeQuery(io, output_dir, query) catch {
                return error.QueryFileGenerationFailed;
            };
        }
        types: {
            var file = try output_dir.createFile(io, "types.ts", .{});
            defer file.close(io);

            var buffer: [1024]u8 = undefined;
            var writer = file.writer(io, &buffer);

            try builder.writeImports(io, allocator, &writer.interface, try UserTypeGenerator.outputDir(io, root_dir), output_dir_path);

            writeTypescriptTypes(&writer.interface, &.{
                builder.entries.get(.parameter), 
                builder.entries.get(.parameter_order), 
                builder.entries.get(.result_set)
            })
            catch {
                return error.TypeFileGenerationFailed;
            };
            try writer.interface.flush();
            break:types;
        }

        return if (is_new) .new_file else .update_file;
    }
};

pub const UserTypeGenerator = struct {
    pub const output_root: Symbol = "user-types";
    pub const success_log_fmt: Symbol = "Successful/name: {s}/*, dialect: {s}, category: {s}";

    pub fn build(builder: *CodeBuilder, io: std.Io, allocator: std.mem.Allocator, root_dir: std.Io.Dir, name: Symbol) !GenerateStatus {
        var output_dir = try outputDir(io, root_dir);
        defer output_dir.close(io);
        const output_dir_path = try output_dir.realPathFileAlloc(io, ".", allocator);
        defer allocator.free(output_dir_path);

        const pascalcase_name = try IdentifierFormatter.format(allocator, name, .pascal_case);
        defer allocator.free(pascalcase_name);

        const file_name = try std.fmt.allocPrint(allocator, "{s}.ts", .{pascalcase_name});
        defer allocator.free(file_name);

        const is_new = if (output_dir.statFile(io, file_name, .{})) |_| false else |_| true;

        var file = try output_dir.createFile(io, file_name, .{});
        defer file.close(io);

        var buffer: [1024]u8 = undefined;
        var writer = file.writer(io, &buffer);

        try builder.writeImports(io, allocator, &writer.interface, output_dir, output_dir_path);

        writeTypescriptTypes(&writer.interface, &.{
            builder.entries.get(.user_type)}
        ) 
        catch {
            return error.TypeFileGenerationFailed;
        };

        try writer.interface.flush();

        return if (is_new) .new_file else .update_file;
    }

    pub fn outputDir(io: std.Io, root_dir: std.Io.Dir) !std.Io.Dir {
        return root_dir.createDirPathOpen(io, output_root, .{});
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

pub const UserTYpeCategory = enum { none, user, anonymous };
pub const UserTypeKind = core.events.UserTypeKind;

pub const FieldTypePair = struct {
    field_name: Symbol,
    type_kind: UserTypeKind,
    field_type: ?Symbol = null,
};

pub const ResultSetColumn = struct {
    field_name: Symbol,
    type_kind: UserTypeKind,
    field_type: Symbol,
    nullable: bool,
};

pub const UserTypeDef = struct {
    header: Header,
    fields: []const Member,

    pub const Header = struct {
        type_category: UserTYpeCategory,
        kind: UserTypeKind,
        name: Symbol,
    };

    pub const Member = struct {
        field_name: Symbol,
        field_type: ?UserTypeDef = null,
    };
};

pub const Parser = struct {
    pub fn beginParse(source_bodies: []const Event.Payload.TopicBody.Encoded) ResultWalker {
        return .{
            .source_bodies = source_bodies,
            .index = 0,
        };
    }

    fn parseQuery(data: BinaryData) !Symbol {
        var reader = cbor.CborStream.Reader.createFromSlice(data);
        return reader.readString();
    }

    fn parsePlaceholder(allocator: std.mem.Allocator, data: BinaryData) ![]const FieldTypePair {
        var reader = cbor.CborStream.Reader.createFromSlice(data);

        const values = try reader.readSlice(allocator, core.events.StructView(FieldTypePair)); 
        var result = try allocator.alloc(FieldTypePair, values.len);

        for (values, 0..) |v, i| {
            result[i] = .{
                .field_name = v[0],
                .type_kind = v[1],
                .field_type = v[2],
            };
        }

        return result;
    }

    fn parsePlaceholderOrder(allocator: std.mem.Allocator, data: BinaryData) ![]const Symbol {
        var reader = cbor.CborStream.Reader.createFromSlice(data);

        return reader.readSlice(allocator, Symbol); 
    }

    fn parseResultSet(allocator: std.mem.Allocator, data: BinaryData) ![]const ResultSetColumn {
        var reader = cbor.CborStream.Reader.createFromSlice(data);

        const values = try reader.readSlice(allocator, core.events.StructView(ResultSetColumn));

        var result_set = try allocator.alloc(ResultSetColumn, values.len);

        for (values, 0..) |v, i| {
            result_set[i] = .{
                .field_name = v[0],
                .type_kind = v[1],
                .field_type = v[2],
                .nullable = v[3],
            };
        }

        return result_set;
    }

    fn parseUserTypeDefinitionInternal(allocator: std.mem.Allocator, reader: *cbor.CborStream.Reader, type_category: UserTYpeCategory, user_type: *UserTypeDef) !void {
        const HeaderType = struct {UserTypeKind, Symbol};

        const v = try reader.readTuple(core.events.StructView(HeaderType));
        const header: UserTypeDef.Header = .{ .type_category = type_category, .kind = v[0], .name = v[1] };

        const filed_len = try reader.readSliceHeader();
        var fields = try allocator.alloc(UserTypeDef.Member, filed_len);

        for (0..filed_len) |i| {
            const tuple_len = try reader.readSliceHeader();
            std.debug.assert(tuple_len == 2);

            fields[i].field_name = try reader.readString();
            fields[i].field_type = field_type: {
                if (!try reader.nextNull()) {
                    var field_type: UserTypeDef = undefined;
                    try parseUserTypeDefinitionInternal(allocator, reader, .none, &field_type);
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

    fn parseUserTypeDefinition(allocator: std.mem.Allocator, data: BinaryData) !UserTypeDef {
        var reader = cbor.CborStream.Reader.createFromSlice(data);
        var user_type: UserTypeDef = undefined;

        try parseUserTypeDefinitionInternal(allocator, &reader, .user, &user_type);

        return user_type;
    }

    fn parseBoundUserDef(allocator: std.mem.Allocator, data: BinaryData) ![]const Symbol {
        var reader = cbor.CborStream.Reader.createFromSlice(data);

        return reader.readSlice(allocator, Symbol);
    }

    fn parseAnonymousUserTypeDef(allocator: std.mem.Allocator, data: BinaryData) ![]const UserTypeDef {
        var reader = cbor.CborStream.Reader.createFromSlice(data);
        
        const values_len = try reader.readSliceHeader();
        var values = try allocator.alloc(UserTypeDef, values_len);

        for (0..values_len) |i| {
            try parseUserTypeDefinitionInternal(allocator, &reader, .anonymous, &values[i]);
        }

        return values;
    }

    pub const ResultWalker = struct {
        source_bodies: []const Event.Payload.TopicBody.Encoded,
        index: usize,

        pub fn deinit(_: *ResultWalker) void {}

        const TargetKindMap = std.StaticStringMap(std.meta.FieldEnum(Target)).initComptime(.{ 
            .{ "query", .query }, 
            .{ "placeholder", .parameter }, .{ "placeholder-order", .parameter_order }, 
            .{ "select-list", .result_set },
            .{ "bound-user-type", .bound_user_type }, .{ "anon-user-type", .anon_user_type },
            .{ "user-type", .user_type },
        });

        pub fn walk(self: *ResultWalker, allocator: std.mem.Allocator) !?Target {
            while (self.index < self.source_bodies.len) {
                defer self.index += 1;

                const body = self.source_bodies[self.index];

                switch (TargetKindMap.get(body.topic) orelse continue) {
                    .query => {
                        return .{ .query = try Parser.parseQuery(body.data) };
                    },
                    .parameter => {
                        return .{ .parameter = try Parser.parsePlaceholder(allocator, body.data) };
                    },
                    .parameter_order => {
                        return .{ .parameter_order = try Parser.parsePlaceholderOrder(allocator, body.data) };
                    },
                    .result_set => {
                        return .{ .result_set = try Parser.parseResultSet(allocator, body.data) };
                    },
                    .user_type => {
                        return .{ .user_type = try Parser.parseUserTypeDefinition(allocator, body.data) };
                    },
                    .bound_user_type => {
                        return .{ .bound_user_type = try Parser.parseBoundUserDef(allocator, body.data) };
                    },
                    .anon_user_type => {
                        return .{ .anon_user_type = try Parser.parseAnonymousUserTypeDef(allocator, body.data) };
                    },
                }
            }

            return null;
        }
    };
};

const TypeMappingRules = std.StaticStringMap(Symbol).initComptime(.{
    // BIGINT INT8, LONG signed eight-byte integer
    .{"BIGINT", "number"}, 
    .{"INT8", "number"}, 
    .{"LONG", "number"}, 
    // BIT  BITSTRING  string of 1s and 0s
    .{"BIT", "string"}, 
    .{"BITSTRING", "string"}, 
    // BLOB BYTEA, BINARY, VARBINARY variable-length binary data
    .{"BLOB", "string"}, 
    .{"BYTEA", "string"}, 
    .{"BINARY", "string"}, 
    .{"VARBINARY", "string"}, 
    // BOOLEAN BOOL, LOGICAL logical boolean (true/false)
    .{"BOOLEAN", "boolean"}, 
    .{"BOOL", "boolean"}, 
    .{"LOGICAL", "boolean"}, 
    // DATE calendar date (year, month day)
    .{"DATE", "string"}, 
    // DECIMAL(prec, scale), NUMERIC(prec, scale) fixed-precision number
    .{"DECIMAL", "number"}, 
    .{"NUMERIC", "number"}, 
    // DOUBLE FLOAT8, double precision floating-point number (8 bytes)
    .{"DOUBLE", "number"}, 
    .{"FLOAT8", "number"}, 
    // HUGEINT signed sixteen-byte integer
    .{"HUGEINT", "number"}, 
    // INTEGER INT4, INT, SIGNED signed four-byte integer
    .{"INTEGER", "number"}, 
    .{"INT4", "number"}, 
    .{"INT", "number"}, 
    .{"SIGNED", "number"}, 
    // INTERVAL date / time delta
    .{"INTERVAL", "string"}, 
    // REAL FLOAT4, FLOAT single precision floating-point number (4 bytes)
    .{"REAL", "number"}, 
    .{"FLOAT4", "number"}, 
    .{"FLOAT", "number"}, 
    // INT2, SHORT signed two-byte integer
    .{"SMALLINT", "number"}, 
    .{"SHORT", "number"}, 
    // TIME time of day (no time zone)
    .{"TIME", "string"}, 
    // TIMESTAMP WITH TIME ZONE, TIMESTAMPTZ combination of time and date
    .{"TIMESTAMP WITH TIME ZONE", "string"}, 
    .{"TIMESTAMPZ", "string"}, 
    // TIMESTAMP, DATETIME combination of time and date
    .{"TIMESTAMP", "string"}, 
    .{"DATETIME", "string"}, 
    // TINYINT INT1 signed one-byte integer
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
    // VARCHAR CHAR, BPCHAR, TEXT, STRING variable-length character string
    .{"VARCHAR", "string"}, 
    .{"CHAR", "string"}, 
    .{"BPCHAR", "string"}, 
    .{"TEXT", "string"}, 
    .{"STRING", "string"}, 
    // Other
    .{"ANY", "any"}
});

test "ts generatot test" {
    std.testing.refAllDecls(@This());
}

pub const tests = struct {
    fn queryToCbor(allocator: std.mem.Allocator, query: Symbol) !Symbol {
        var buffer = std.Io.Writer.Allocating.init(allocator);
        defer buffer.deinit();

        var writer = try cbor.CborStream.Writer.init(&buffer.writer);
        defer writer.deinit();

        _ = try writer.writeString(query);

        return buffer.toOwnedSlice();
    }

    test "parse query" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const expect_query = "select $1, $2 from foo where kind = $3";
        const source_bodies: []const Event.Payload.TopicBody.Encoded = &.{.{
            .topic = "query",
            .data = try queryToCbor(allocator, expect_query),
        }};

        var iter: Parser.ResultWalker = .{
            .source_bodies = source_bodies,
            .index = 0,
        };
        defer iter.deinit();

        assert: {
            const walk_result = try iter.walk(allocator);
            try std.testing.expect(walk_result != null);

            const result = walk_result.?;
            try std.testing.expectEqual(.query, std.meta.activeTag(result));
            try std.testing.expectEqualStrings(expect_query, result.query);
            break :assert;
        }
        assert: {
            const walk_result = try iter.walk(allocator);
            try std.testing.expect(walk_result == null);
            break :assert;
        }
    }

    fn placeholderToCbor(allocator: std.mem.Allocator, items: []const FieldTypePair) !Symbol {
        var buffer = std.Io.Writer.Allocating.init(allocator);
        defer buffer.deinit();

        var writer = try cbor.CborStream.Writer.init(&buffer.writer);
        defer writer.deinit();

        _ = try writer.writeSliceHeader(items.len);

        for (items) |c| {
            _ = try writer.writeTuple(events.StructView(FieldTypePair), .{c.field_name, c.type_kind, c.field_type});
        }

        return buffer.toOwnedSlice();
    }

    test "parse parameter#1" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();
        
        const expect: []const FieldTypePair = &.{
            .{.field_name = "id", .type_kind = .primitive, .field_type = "bigint"},
            .{.field_name = "name", .type_kind = .primitive, .field_type = "varchar"},
        };
        const source_bodies = try placeholderToCbor(arena.allocator(), expect);
        
        var iter: Parser.ResultWalker = .{
            .source_bodies = &.{
                .{ .topic = "placeholder", .data = source_bodies }
            },
            .index = 0,
        };
        defer iter.deinit();

        assert: {
            const walk_result = try iter.walk(allocator);
            try std.testing.expect(walk_result != null);

            const result = walk_result.?;
            try std.testing.expectEqual(.parameter, std.meta.activeTag(result));
            try std.testing.expectEqualDeep(expect, result.parameter);
            break :assert;
        }
        assert: {
            const walk_result = try iter.walk(allocator);
            try std.testing.expect(walk_result == null);
            break :assert;
        }
    }

    test "parse parameter#2 (with any type)" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const expect: []const FieldTypePair = &.{
            .{.field_name = "id", .type_kind = .primitive, .field_type = "bigint"},
            .{.field_name = "name", .type_kind = .primitive, .field_type = null},
        };
        const source_bodies = try placeholderToCbor(arena.allocator(), expect);

        var iter: Parser.ResultWalker = .{
            .source_bodies = &.{
                .{ .topic = "placeholder", .data = source_bodies }
            },
            .index = 0,
        };
        defer iter.deinit();

        assert: {
            const walk_result = try iter.walk(allocator);
            try std.testing.expect(walk_result != null);

            const result = walk_result.?;
            try std.testing.expectEqual(.parameter, std.meta.activeTag(result));
            try std.testing.expectEqualDeep(expect, result.parameter);
            break :assert;
        }
        assert: {
            const walk_result = try iter.walk(allocator);
            try std.testing.expect(walk_result == null);
            break :assert;
        }
    }

    test "parse parameter order" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const expect: []const Symbol = &.{"id", "name", "kind"};
        
        var buffer = std.Io.Writer.Allocating.init(allocator);
        defer buffer.deinit();

        var writer = try cbor.CborStream.Writer.init(&buffer.writer);
        defer writer.deinit();

        _ = try writer.writeSlice(Symbol, expect);

        const source_bodies: []const Event.Payload.TopicBody.Encoded = &.{.{
            .topic = "placeholder-order",
            .data = buffer.written(),
        }};

        var iter: Parser.ResultWalker = .{
            .source_bodies = source_bodies,
            .index = 0,
        };
        defer iter.deinit();

        assert: {
            const walk_result = try iter.walk(allocator);
            try std.testing.expect(walk_result != null);

            const result = walk_result.?;
            try std.testing.expectEqual(.parameter_order, std.meta.activeTag(result));
            try std.testing.expectEqualDeep(expect, result.parameter_order);
            break :assert;
        }
        assert: {
            const walk_result = try iter.walk(allocator);
            try std.testing.expect(walk_result == null);
            break :assert;
        }
    }

    fn resultSetToCbor(allocator: std.mem.Allocator, result_set: []const ResultSetColumn) !Symbol {
        var buffer = std.Io.Writer.Allocating.init(allocator);
        defer buffer.deinit();

        var writer = try cbor.CborStream.Writer.init(&buffer.writer);
        defer writer.deinit();

        _ = try writer.writeSliceHeader(result_set.len);

        for (result_set) |c| {
            _ = try writer.writeTuple(events.StructView(ResultSetColumn), .{c.field_name, c.type_kind, c.field_type, c.nullable});
        }

        return buffer.toOwnedSlice();
    }

    test "parse empty result set" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const expect: []const ResultSetColumn = &.{};

        const source_bodies: []const Event.Payload.TopicBody.Encoded = &.{.{
            .topic = "select-list",
            .data = try resultSetToCbor(arena.allocator(), expect),
        }};

        var iter: Parser.ResultWalker = .{
            .source_bodies = source_bodies,
            .index = 0,
        };
        defer iter.deinit();

        assert: {
            const walk_result = try iter.walk(allocator);
            try std.testing.expect(walk_result != null);

            const result = walk_result.?;
            try std.testing.expectEqual(.result_set, std.meta.activeTag(result));
            try std.testing.expectEqualDeep(expect, result.result_set);
            break :assert;
        }
        assert: {
            const walk_result = try iter.walk(allocator);
            try std.testing.expect(walk_result == null);
            break :assert;
        }
    }

    test "parse result set" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const expect: []const ResultSetColumn = &.{
            .{.field_name = "a", .type_kind = .primitive, .field_type = "INTEGER", .nullable = false},
            .{.field_name = "b", .type_kind = .primitive, .field_type = "VARCHAR", .nullable = true},
        };

        const source_bodies: []const Event.Payload.TopicBody.Encoded = &.{.{
            .topic = "select-list",
            .data = try resultSetToCbor(arena.allocator(), expect),
        }};

        var iter: Parser.ResultWalker = .{
            .source_bodies = source_bodies,
            .index = 0,
        };
        defer iter.deinit();

        assert: {
            const walk_result = try iter.walk(allocator);
            try std.testing.expect(walk_result != null);

            const result = walk_result.?;
            try std.testing.expectEqual(.result_set, std.meta.activeTag(result));
            try std.testing.expectEqualDeep(expect, result.result_set);
            break :assert;
        }
        assert: {
            const walk_result = try iter.walk(allocator);
            try std.testing.expect(walk_result == null);
            break :assert;
        }
    }

    test "parse result set with aliasless field name" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const source: []const ResultSetColumn = &.{
            .{.field_name = "Cast(a as INTEGER)", .type_kind = .primitive, .field_type = "INTEGER", .nullable = false},
            .{.field_name = "bar_baz", .type_kind = .primitive, .field_type = "VARCHAR", .nullable = true},
        };

        const expect: []const ResultSetColumn = &.{
            .{.field_name = "Cast(a as INTEGER)", .type_kind = .primitive, .field_type = "INTEGER", .nullable = false},
            .{.field_name = "bar_baz", .type_kind = .primitive, .field_type = "VARCHAR", .nullable = true},
        };

        const source_bodies: []const Event.Payload.TopicBody.Encoded = &.{.{
            .topic = "select-list",
            .data = try resultSetToCbor(arena.allocator(), source),
        }};

        var iter: Parser.ResultWalker = .{
            .source_bodies = source_bodies,
            .index = 0,
        };
        defer iter.deinit();

        assert: {
            const walk_result = try iter.walk(allocator);
            try std.testing.expect(walk_result != null);

            const result = walk_result.?;
            try std.testing.expectEqual(.result_set, std.meta.activeTag(result));
            try std.testing.expectEqualDeep(expect, result.result_set);
            break:assert;
        }
        assert: {
            const walk_result = try iter.walk(allocator);
            try std.testing.expect(walk_result == null);
            break:assert;
        }
    }

    fn userTypeToCborInternal(writer: *cbor.CborStream.Writer, user_type: UserTypeDef) !void {
        header: {
            _ = try writer.writeSliceHeader(2);
            _ = try writer.writeEnum(UserTypeKind, user_type.header.kind);
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
        var buffer = std.Io.Writer.Allocating.init(allocator);
        defer buffer.deinit();

        var writer = try cbor.CborStream.Writer.init(&buffer.writer);
        defer writer.deinit();

        try userTypeToCborInternal(&writer, user_type);

        return buffer.toOwnedSlice();
    }

    test "parse enum user type" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const expect: UserTypeDef = .{
            .header = .{
                .type_category = .user, .kind = .@"enum", .name = "Visibility",
            },
            .fields = &.{
                .{.field_name = "hide", .field_type = null}, 
                .{.field_name = "visible", .field_type = null}, 
            },
        };

        const source_bodies: []const Event.Payload.TopicBody.Encoded = &.{.{
            .topic = "user-type",
            .data = try userTypeToCbor(allocator, expect),
        }};

        var iter: Parser.ResultWalker = .{
            .source_bodies = source_bodies,
            .index = 0,
        };
        defer iter.deinit();

        assert: {
            const walk_result = try iter.walk(allocator);
            try std.testing.expect(walk_result != null);

            const result = walk_result.?;
            try std.testing.expectEqual(.user_type, std.meta.activeTag(result));
            try std.testing.expectEqualDeep(expect, result.user_type);
            break:assert;
        }
        assert: {
            const walk_result = try iter.walk(allocator);
            try std.testing.expect(walk_result == null);
            break:assert;
        }
    }

    test "parse bound enum user type" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();
        
        const expects: []const Symbol = &.{"Visibility", "Status"};
        
        var buffer = std.Io.Writer.Allocating.init(allocator);
        defer buffer.deinit();

        var writer = try cbor.CborStream.Writer.init(&buffer.writer);
        defer writer.deinit();

        _ = try writer.writeSlice(Symbol, expects);

        const source_bodies: []const Event.Payload.TopicBody.Encoded = &.{.{
            .topic = "bound-user-type",
            .data = buffer.written(),
        }};

        var iter: Parser.ResultWalker = .{
            .source_bodies = source_bodies,
            .index = 0,
        };
        defer iter.deinit();

        assert: {
            const walk_result = try iter.walk(allocator);
            try std.testing.expect(walk_result != null);

            const result = walk_result.?;
            try std.testing.expectEqual(.bound_user_type, std.meta.activeTag(result));
            try std.testing.expectEqualDeep(expects, result.bound_user_type);
            break:assert;
        }
        assert: {
            const walk_result = try iter.walk(allocator);
            try std.testing.expect(walk_result == null);
            break:assert;
        }
    }

    fn anonymousTypeToCbor(allocator: std.mem.Allocator, user_types: []const UserTypeDef) !Symbol {
        var buffer = std.Io.Writer.Allocating.init(allocator);
        defer buffer.deinit();

        var writer = try cbor.CborStream.Writer.init(&buffer.writer);
        defer writer.deinit();

        _ = try writer.writeSliceHeader(user_types.len);

        for (user_types) |user_type| {
            try userTypeToCborInternal(&writer, user_type);
        }

        return buffer.toOwnedSlice();
    }

    test "parse anonymous user type#1 (enum)" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const expects: []const UserTypeDef = &.{
            .{
                .header = .{ .type_category = .anonymous, .kind = .@"enum", .name = "Visibility" },
                .fields = &.{
                    .{.field_name = "hide", .field_type = null}, 
                    .{.field_name = "visible", .field_type = null}, 
                },
            },
            .{
                .header = .{ .type_category = .anonymous, .kind = .@"enum", .name = "Status" },
                .fields = &.{
                    .{.field_name = "succes", .field_type = null}, 
                    .{.field_name = "failed", .field_type = null}, 
                },
            },
        };

        const source_bodies: []const Event.Payload.TopicBody.Encoded = &.{.{
            .topic = "anon-user-type",
            .data = try anonymousTypeToCbor(arena.allocator(), expects),
        }};

        var iter: Parser.ResultWalker = .{
            .source_bodies = source_bodies,
            .index = 0,
        };
        defer iter.deinit();
        
        assert: {
            const walk_result = try iter.walk(arena.allocator());
            try std.testing.expect(walk_result != null);

            const result = walk_result.?;
            try std.testing.expectEqual(.anon_user_type, std.meta.activeTag(result));
            try std.testing.expectEqualDeep(expects, result.anon_user_type);
            break:assert;
        }
        assert: {
            const walk_result = try iter.walk(allocator);
            try std.testing.expect(walk_result == null);
            break:assert;
        }
    }

    test "parse anonymous user type#2 (primitive list)" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const expects: []const UserTypeDef = &.{
            .{
                .header = .{ .type_category = .anonymous, .kind = .array, .name = "SelList::Array#1" },
                .fields = &.{
                    .{
                        .field_name = "Anon::Primitive#1", 
                        .field_type = .{
                            .header = .{.type_category = .none, .kind = .primitive, .name = "INTEGER"}, 
                            .fields = &.{}
                        }
                    }, 
                },
            },
        };

        const source_bodies: []const Event.Payload.TopicBody.Encoded = &.{.{
            .topic = "anon-user-type",
            .data = try anonymousTypeToCbor(allocator, expects),
        }};

        var iter: Parser.ResultWalker = .{
            .source_bodies = source_bodies,
            .index = 0,
        };
        defer iter.deinit();
        
        assert: {
            const walk_result = try iter.walk(allocator);
            try std.testing.expect(walk_result != null);

            const result = walk_result.?;
            try std.testing.expectEqual(.anon_user_type, std.meta.activeTag(result));
            try std.testing.expectEqualDeep(expects, result.anon_user_type);
            break:assert;
        }
        assert: {
            const walk_result = try iter.walk(allocator);
            try std.testing.expect(walk_result == null);
            break:assert;
        }
    }

    test "parse anonymous user type#3 (enum list)" {
        const allocator = std.testing.allocator;
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        const expects: []const UserTypeDef = &.{
            .{
                .header = .{ .type_category = .anonymous, .kind = .@"enum", .name = "SelList::Array#1" },
                .fields = &.{
                    .{
                        .field_name = "Anon::Enum#1", 
                        .field_type = .{
                            .header = .{.type_category = .none, .kind = .@"enum", .name = "Visibility"}, 
                            .fields = &.{}
                        }
                    }, 
                },
            },
        };

        const source_bodies: []const Event.Payload.TopicBody.Encoded = &.{.{
            .topic = "anon-user-type",
            .data = try anonymousTypeToCbor(arena.allocator(), expects),
        }};

        var iter: Parser.ResultWalker = .{
            .source_bodies = source_bodies,
            .index = 0,
        };
        defer iter.deinit();
        
        assert: {
            const walk_result = try iter.walk(arena.allocator());
            try std.testing.expect(walk_result != null);

            const result = walk_result.?;
            try std.testing.expectEqual(.anon_user_type, std.meta.activeTag(result));
            try std.testing.expectEqualDeep(expects, result.anon_user_type);
            break:assert;
        }
        assert: {
            const walk_result = try iter.walk(arena.allocator());
            try std.testing.expect(walk_result == null);
            break:assert;
        }
    }

    test "apply bound user type name#1" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        const expects: []const Symbol = &.{};

        var dir = std.testing.tmpDir(.{});
        defer dir.cleanup();

        const parent_path = try dir.dir.realPathFileAlloc(io, ".", allocator);
        defer allocator.free(parent_path);

        var builder = CodeBuilder.init(allocator);
        defer builder.deinit();

        try std.testing.expectEqual(0, builder.user_type_names.count());

        try builder.applyBoundUserType(expects);

        try std.testing.expectEqual(0, builder.user_type_names.count());
    }

    test "apply bound user type name#2" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        const expects: []const Symbol = &.{"Visibility", "Status"};

        var dir = std.testing.tmpDir(.{});
        defer dir.cleanup();

        const parent_path = try dir.dir.realPathFileAlloc(io, ".", allocator);
        defer allocator.free(parent_path);

        var builder = CodeBuilder.init(allocator);
        defer builder.deinit();

        try std.testing.expectEqual(0, builder.user_type_names.count());

        try builder.applyBoundUserType(expects);

        try std.testing.expectEqual(2, builder.user_type_names.count());
        try std.testing.expect(builder.user_type_names.contains("Visibility"));
        try std.testing.expect(builder.user_type_names.contains("Status"));
    }

    test "apply anonymous user type" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        const expects: []const UserTypeDef = &.{
            .{
                .header = .{ .type_category = .user, .kind = .@"enum", .name = "Visibility" },
                .fields = &.{
                    .{ .field_name = "hide", .field_type = null },
                    .{ .field_name = "visible", .field_type = null },
                }
            },
            .{
                .header = .{ .type_category = .user, .kind = .@"enum", .name = "Status" },
                .fields = &.{
                    .{ .field_name = "failed", .field_type = null },
                    .{ .field_name = "success", .field_type = null },
                }
            },
        };

        var dir = std.testing.tmpDir(.{});
        defer dir.cleanup();

        const parent_path = try dir.dir.realPathFileAlloc(io, ".", allocator);
        defer allocator.free(parent_path);

        var builder = CodeBuilder.init(allocator);
        defer builder.deinit();

        try std.testing.expectEqual(0, builder.anon_user_types.count());

        try builder.applyAnonymousUserType(expects);

        try std.testing.expectEqual(expects.len, builder.anon_user_types.count());
        try std.testing.expectEqualDeep(expects[0], builder.anon_user_types.get("Visibility").?);
        try std.testing.expectEqualDeep(expects[1], builder.anon_user_types.get("Status").?);
    }

    fn runApplyPlaceholder(parameters: []const FieldTypePair, expect: Symbol, user_type_names: []const Symbol, anon_user_types: []const UserTypeDef) !void {
        const io = std.testing.io;
        
        var arena = std.heap.ArenaAllocator.init((std.testing.allocator));
        defer arena.deinit();
        const allocator = arena.allocator();

        var dir = std.testing.tmpDir(.{});
        defer dir.cleanup();
        const parent_path = try dir.dir.realPathFileAlloc(io, ".", allocator);
        defer allocator.free(parent_path);

        var builder = CodeBuilder.init(allocator);
        defer builder.deinit();

        try builder.applyBoundUserType(user_type_names);
        try builder.applyAnonymousUserType(anon_user_types);
        try builder.applyPlaceholder(allocator, parameters, builder.user_type_names, builder.anon_user_types);

        const apply_result = builder.entries.get(.parameter);
        try std.testing.expect(apply_result != null);

        const result = apply_result.?;

        try std.testing.expectEqualStrings(expect, result);
    }

    test "generate name parameter code#1" {
        const parameters: []const FieldTypePair = &.{
            .{.field_name = "id", .type_kind = .primitive, .field_type = "BIGINT"},
            .{.field_name = "name", .type_kind = .primitive, .field_type = "VARCHAR"},
        };
        const anon_user_types = &.{};
        const user_type_names = &.{};

        const expect = 
            \\export type Parameter = {
            \\  id: number | null,
            \\  name: string | null,
            \\}
        ;

        try runApplyPlaceholder(parameters, expect, user_type_names, anon_user_types);
    }

    test "generate name parameter code#2 (upper case field)" {
        const parameters: []const FieldTypePair = &.{
            .{.field_name = "ID", .type_kind = .primitive, .field_type = "BIGINT"},
            .{.field_name = "NAME", .type_kind = .primitive, .field_type = "VARCHAR"},
        };
        const anon_user_types = &.{};
        const user_type_names = &.{};

        const expect = 
            \\export type Parameter = {
            \\  id: number | null,
            \\  name: string | null,
            \\}
        ;

        try runApplyPlaceholder(parameters, expect, user_type_names, anon_user_types);
    }

    test "generate name parameter code#3 (lower-case)" {
        const parameters: []const FieldTypePair = &.{
            .{.field_name = "id", .type_kind = .primitive, .field_type = "int"},
            .{.field_name = "name", .type_kind = .primitive, .field_type = "varchar"},
        };
        const anon_user_types = &.{};
        const user_type_names = &.{};

        const expect = 
            \\export type Parameter = {
            \\  id: number | null,
            \\  name: string | null,
            \\}
        ;

        try runApplyPlaceholder(parameters, expect, user_type_names, anon_user_types);
    }

    test "generate name parameter code#4 (with any type)" {
        const parameters: []const FieldTypePair = &.{
            .{.field_name = "id", .type_kind = .primitive, .field_type = null},
            .{.field_name = "name", .type_kind = .primitive, .field_type = null},
        };
        const anon_user_types = &.{};
        const user_type_names = &.{};

        const expect = 
            \\export type Parameter = {
            \\  id: any,
            \\  name: any,
            \\}
        ;

        try runApplyPlaceholder(parameters, expect, user_type_names, anon_user_types);
    }

    test "generate name parameter code#5 (with snake_case)" {
        const parameters: []const FieldTypePair = &.{
            .{.field_name = "user_id", .type_kind = .primitive, .field_type = "int"},
            .{.field_name = "user_name", .type_kind = .primitive, .field_type = "text"},
        };
        const anon_user_types = &.{};
        const user_type_names = &.{};

        const expect = 
            \\export type Parameter = {
            \\  userId: number | null,
            \\  userName: string | null,
            \\}
        ;

        try runApplyPlaceholder(parameters, expect, user_type_names, anon_user_types);
    }

    test "generate name parameter code#5 (with snake_case type)" {
        const parameters: []const FieldTypePair = &.{
            .{.field_name = "user_id", .type_kind = .primitive, .field_type = "int"},
            .{.field_name = "status", .type_kind = .user, .field_type = "ui_status"},
        };
        const anon_user_types = &.{};
        const user_type_names = &.{"ui_status"};

        const expect = 
            \\export type Parameter = {
            \\  userId: number | null,
            \\  status: UiStatus | null,
            \\}
        ;

        try runApplyPlaceholder(parameters, expect, user_type_names, anon_user_types);
    }

    test "generate name parameter code#5 (with PascalCase)" {
        const parameters: []const FieldTypePair = &.{
            .{.field_name = "UserId", .type_kind = .primitive, .field_type = "int"},
            .{.field_name = "UserName", .type_kind = .primitive, .field_type = "text"},
        };
        const anon_user_types = &.{};
        const user_type_names = &.{};

        const expect = 
            \\export type Parameter = {
            \\  userId: number | null,
            \\  userName: string | null,
            \\}
        ;

        try runApplyPlaceholder(parameters, expect, user_type_names, anon_user_types);
    }

    test "generate name parameter code#5 (without alias)" {
        const parameters: []const FieldTypePair = &.{
            .{.field_name = "UserId", .type_kind = .primitive, .field_type = "int"},
            .{.field_name = "CAST(name AS VARCHAR)", .type_kind = .primitive, .field_type = "text"},
        };
        const anon_user_types = &.{};
        const user_type_names = &.{};

        const expect = 
            \\export type Parameter = {
            \\  userId: number | null,
            \\  "CAST(name AS VARCHAR)": string | null,
            \\}
        ;

        try runApplyPlaceholder(parameters, expect, user_type_names, anon_user_types);
    }

    test "generate positional parameter code" {
        const parameters: []const FieldTypePair = &.{
            .{.field_name = "1", .type_kind = .primitive, .field_type = "float"},
            .{.field_name = "2", .type_kind = .primitive, .field_type = "text"},
        };
        const anon_user_types = &.{};
        const user_type_names = &.{};

        const expect = 
            \\export type Parameter = {
            \\  1: number | null,
            \\  2: string | null,
            \\}
        ;

        try runApplyPlaceholder(parameters, expect, user_type_names, anon_user_types);
    }

    test "generate positional parameter code with any type" {
        const parameters: []const FieldTypePair = &.{
            .{.field_name = "1", .type_kind = .primitive, .field_type = null},
            .{.field_name = "2", .type_kind = .primitive, .field_type = null},
        };
        const anon_user_types = &.{};
        const user_type_names = &.{};

        const expect = 
            \\export type Parameter = {
            \\  1: any,
            \\  2: any,
            \\}
        ;

        try runApplyPlaceholder(parameters, expect, user_type_names, anon_user_types);
    }

    test "generate positional parameter code with enum user type" {
        const parameters: []const FieldTypePair = &.{
            .{.field_name = "vis1", .type_kind = .user, .field_type = "Visibility"},
            .{.field_name = "vis2", .type_kind = .@"enum", .field_type = "Param::Enum#1"},
        };
        const anon_user_types: []const UserTypeDef = &.{
            .{
                .header = .{ .type_category = .anonymous, .kind = .@"enum", .name = "Param::Enum#1" },
                .fields = &.{ .{.field_name = "hide"}, .{.field_name = "visible"} },
            },
        };
        const user_type_names = &.{ "Visibility", "Status"};

        const expect = 
            \\export type Parameter = {
            \\  vis1: Visibility | null,
            \\  vis2: 'hide' | 'visible' | null,
            \\}
        ;

        try runApplyPlaceholder(parameters, expect, user_type_names, anon_user_types);
    }

    test "generate parameter order#1 (named parameter)" {
        const io = std.testing.io;

        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const orders: []const Symbol = &.{"id", "name", "kind"};
        const expect = "export const ParameterOrder: (keyof Parameter)[] = ['id', 'name', 'kind']";

        var dir = std.testing.tmpDir(.{});
        defer dir.cleanup();

        const parent_path = try dir.dir.realPathFileAlloc(io, ".", allocator);
        defer allocator.free(parent_path);

        var builder = CodeBuilder.init(allocator);
        defer builder.deinit();

        try builder.applyPlaceholderOrder(allocator, orders);

        const apply_result = builder.entries.get(.parameter_order);
        try std.testing.expect(apply_result != null);
        try std.testing.expectEqualStrings(expect, apply_result.?);
    }

    test "generate parameter order#2 (positional)" {
        const io = std.testing.io;

        var arena = std.heap.ArenaAllocator.init((std.testing.allocator));
        defer arena.deinit();
        const allocator = arena.allocator();

        const orders: []const Symbol = &.{"3", "1", "2"};
        const expect = "export const ParameterOrder: (keyof Parameter)[] = [3, 1, 2]";

        var dir = std.testing.tmpDir(.{});
        defer dir.cleanup();

        const parent_path = try dir.dir.realPathFileAlloc(io, ".", allocator);
        defer allocator.free(parent_path);

        var builder = CodeBuilder.init(allocator);
        defer builder.deinit();

        try builder.applyPlaceholderOrder(allocator, orders);

        const apply_result = builder.entries.get(.parameter_order);
        try std.testing.expect(apply_result != null);
        try std.testing.expectEqualStrings(expect, apply_result.?);
    }

    fn runApplyResultSets(parameters: []const ResultSetColumn, expect: Symbol, user_type_names: []const Symbol, anon_user_types: []const UserTypeDef) !void {
        const io = std.testing.io;

        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        var dir = std.testing.tmpDir(.{});
        defer dir.cleanup();

        const parent_path = try dir.dir.realPathFileAlloc(io, ".", allocator);
        defer allocator.free(parent_path);

        var builder = CodeBuilder.init(allocator);
        defer builder.deinit();

        try builder.applyBoundUserType(user_type_names);
        try builder.applyAnonymousUserType(anon_user_types);
        try builder.applyResultSets(allocator, parameters, builder.user_type_names, builder.anon_user_types);

        const apply_result = builder.entries.get(.result_set);
        try std.testing.expect(apply_result != null);
        try std.testing.expectEqualStrings(expect, apply_result.?);
    }

    test "generate select list#1 (lowercase field)" {
        const result_set: []const ResultSetColumn = &.{
            .{.field_name = "id", .type_kind = .primitive, .field_type = "INTEGER", .nullable = false},
            .{.field_name = "kind", .type_kind = .primitive, .field_type = "INTEGER", .nullable = false},
            .{.field_name = "value", .type_kind = .primitive, .field_type = "VARCHAR", .nullable = false},
        };
        const anon_user_types = &.{};
        const user_type_names = &.{};

        const expect = 
            \\export type ResultSet = {
            \\  id: number;
            \\  kind: number;
            \\  value: string;
            \\}
        ;

        try runApplyResultSets(result_set, expect, user_type_names, anon_user_types);
    }

    test "generate select list#2 (PascalCase field)" {
        const result_set: []const ResultSetColumn = &.{
            .{.field_name = "userId", .type_kind = .primitive, .field_type = "INTEGER", .nullable = false},
            .{.field_name = "profileKind", .type_kind = .primitive, .field_type = "INTEGER", .nullable = false},
            .{.field_name = "remarks", .type_kind = .primitive, .field_type = "VARCHAR", .nullable = false},
        };
        const anon_user_types = &.{};
        const user_type_names = &.{};

        const expect = 
            \\export type ResultSet = {
            \\  userId: number;
            \\  profileKind: number;
            \\  remarks: string;
            \\}
        ;

        try runApplyResultSets(result_set, expect, user_type_names, anon_user_types);
    }

    test "generate select list#3 (lower snake_case field)" {
        const result_set: []const ResultSetColumn = &.{
            .{.field_name = "user_id", .type_kind = .primitive, .field_type = "INTEGER", .nullable = false},
            .{.field_name = "profile_kind", .type_kind = .primitive, .field_type = "INTEGER", .nullable = false},
            .{.field_name = "remarks", .type_kind = .primitive, .field_type = "VARCHAR", .nullable = false},
        };
        const anon_user_types = &.{};
        const user_type_names = &.{};

        const expect = 
            \\export type ResultSet = {
            \\  userId: number;
            \\  profileKind: number;
            \\  remarks: string;
            \\}
        ;

        try runApplyResultSets(result_set, expect, user_type_names, anon_user_types);
    }

    test "generate select list#4 (upper snake_case field)" {
        const result_set: []const ResultSetColumn = &.{
            .{.field_name = "USER_ID", .type_kind = .primitive, .field_type = "INTEGER", .nullable = false},
            .{.field_name = "PROFILE_KIND", .type_kind = .primitive, .field_type = "INTEGER", .nullable = false},
            .{.field_name = "REMARKS", .type_kind = .primitive, .field_type = "VARCHAR", .nullable = false},
        };
        const anon_user_types = &.{};
        const user_type_names = &.{};

        const expect = 
            \\export type ResultSet = {
            \\  userId: number;
            \\  profileKind: number;
            \\  remarks: string;
            \\}
        ;

        try runApplyResultSets(result_set, expect, user_type_names, anon_user_types);
    }

    test "generate select list#5 (nullable field)" {
        const result_set: []const ResultSetColumn = &.{
            .{.field_name = "id", .type_kind = .primitive, .field_type = "INTEGER", .nullable = false},
            .{.field_name = "kind", .type_kind = .primitive, .field_type = "INTEGER", .nullable = true},
            .{.field_name = "value", .type_kind = .primitive, .field_type = "VARCHAR", .nullable = true},
        };
        const anon_user_types = &.{};
        const user_type_names = &.{};

        const expect = 
            \\export type ResultSet = {
            \\  id: number;
            \\  kind: number | null;
            \\  value: string | null;
            \\}
        ;

        try runApplyResultSets(result_set, expect, user_type_names, anon_user_types);
    }

    test "generate select list#6 (field without alias)" {
        const result_set: []const ResultSetColumn = &.{
            .{.field_name = "id", .type_kind = .primitive, .field_type = "INTEGER", .nullable = false},
            .{.field_name = "kind", .type_kind = .primitive, .field_type = "INTEGER", .nullable = true},
            .{.field_name = "CAST($val AS VARCHAR)", .type_kind = .primitive, .field_type = "VARCHAR", .nullable = true},
        };
        const anon_user_types = &.{};
        const user_type_names = &.{};

        const expect = 
            \\export type ResultSet = {
            \\  id: number;
            \\  kind: number | null;
            \\  "CAST($val AS VARCHAR)": string | null;
            \\}
        ;

        try runApplyResultSets(result_set, expect, user_type_names, anon_user_types);
    }

    test "generate select list#7 (with enum user type)" {
        const result_set: []const ResultSetColumn = &.{
            .{.field_name = "id", .type_kind = .primitive, .field_type = "INTEGER", .nullable = false},
            .{.field_name = "vis1", .type_kind = .@"enum", .field_type = "SelList::Enum#1", .nullable = true},
            .{.field_name = "vis2", .type_kind = .user, .field_type = "Visibility", .nullable = false},
        };
        const anon_user_types: []const UserTypeDef = &.{
            .{  
                .header = .{ .type_category = .anonymous, .kind = .@"enum", .name = "SelList::Enum#1" },
                .fields = &.{ .{.field_name = "hide"}, .{.field_name = "visible"} },
            }
        };
        const user_type_names = &.{"Visibility", "Status"};

        const expect = 
            \\export type ResultSet = {
            \\  id: number;
            \\  vis1: 'hide' | 'visible' | null;
            \\  vis2: Visibility;
            \\}
        ;

        try runApplyResultSets(result_set, expect, user_type_names, anon_user_types);
    }

    test "generate select list#8 (primitive list)" {
        const result_set: []const ResultSetColumn = &.{
            .{.field_name = "id", .type_kind = .primitive, .field_type = "INTEGER", .nullable = false},
            .{.field_name = "numbers", .type_kind = .array, .field_type = "SelList::Array#1", .nullable = true},
        };
        const anon_user_types: []const UserTypeDef = &.{
            .{  
                .header = .{ .type_category = .anonymous, .kind = .array, .name = "SelList::Array#1" },
                .fields = &.{ 
                    .{
                        .field_name = "Anon::Primitive#2", 
                        .field_type = .{
                            .header = .{.type_category = .none, .kind = .primitive, .name = "INTEGER"}, 
                            .fields = &.{} 
                        }
                    }
                },
            }
        };
        const user_type_names = &.{};

        const expect = 
            \\export type ResultSet = {
            \\  id: number;
            \\  numbers: number[] | null;
            \\}
        ;

        try runApplyResultSets(result_set, expect, user_type_names, anon_user_types);
    }

    test "generate select list#8 (predefined enum list)" {
        const result_set: []const ResultSetColumn = &.{
            .{.field_name = "id", .type_kind = .primitive, .field_type = "INTEGER", .nullable = false},
            .{.field_name = "vis2", .type_kind = .array, .field_type = "SelList::Array#1", .nullable = true},
        };
        const anon_user_types: []const UserTypeDef = &.{
            .{  
                .header = .{ .type_category = .anonymous, .kind = .array, .name = "SelList::Array#1" },
                .fields = &.{ 
                    .{
                        .field_name = "Anon::User#2", 
                        .field_type = .{
                            .header = .{.type_category = .user, .kind = .@"user", .name = "Visibility"}, 
                            .fields = &.{} 
                        }
                    } 
                },
            }
        };
        const user_type_names = &.{"Visibility"};

        const expect = 
            \\export type ResultSet = {
            \\  id: number;
            \\  vis2: Visibility[] | null;
            \\}
        ;

        try runApplyResultSets(result_set, expect, user_type_names, anon_user_types);
    }

    test "generate select list#9 (predefined struct)" {
        const result_set: []const ResultSetColumn = &.{
            .{.field_name = "id", .type_kind = .primitive, .field_type = "INTEGER", .nullable = false},
            .{.field_name = "user", .type_kind = .user, .field_type = "USER_PROFILE", .nullable = true},
        };
        const anon_user_types = &.{};
        const user_type_names = &.{"USER_PROFILE"};

        const expect = 
            \\export type ResultSet = {
            \\  id: number;
            \\  user: UserProfile | null;
            \\}
        ;

        try runApplyResultSets(result_set, expect, user_type_names, anon_user_types);
    }

    test "generate select list#9 (anonymous struct)" {
        const result_set: []const ResultSetColumn = &.{
            .{.field_name = "id", .type_kind = .primitive, .field_type = "INTEGER", .nullable = false},
            .{.field_name = "user", .type_kind = .user, .field_type = "SelList::Array#1", .nullable = true},
        };
        const anon_user_types: []const UserTypeDef = &.{
            .{
                .header = .{.type_category = .anonymous, .kind = .array, .name = "SelList::Array#1"},
                .fields = &.{ 
                    .{
                        .field_name = "Anon::Struct#2", 
                        .field_type = .{
                            .header = .{.type_category = .anonymous, .kind = .@"struct", .name = "Anon::Struct#2"}, 
                            .fields = &.{}
                        }
                    }
                }
            },
            .{
                .header = .{.type_category = .anonymous, .kind = .@"struct", .name = "Anon::Struct#2"},
                .fields = &.{ 
                    .{.field_name = "name", .field_type = .{.header = .{.type_category = .none, .kind = .@"primitive", .name = "varchar"}, .fields = &.{}}},
                    .{.field_name = "age", .field_type = .{.header = .{.type_category = .none, .kind = .@"primitive", .name = "int"}, .fields = &.{}}},
                    .{.field_name = "gender", .field_type = .{.header = .{.type_category = .anonymous, .kind = .@"enum", .name = "Anon::Enum#3"}, .fields = &.{}}},                
                }
            },
            .{
                .header = .{.type_category = .anonymous, .kind = .@"enum", .name = "Anon::Enum#3"},
                .fields = &.{ 
                    .{.field_name = "male", .field_type = null},
                    .{.field_name = "female", .field_type = null},
                }
            },
        };
        const user_type_names = &.{"USER_PROFILE"};

        const expect = 
            \\export type ResultSet = {
            \\  id: number;
            \\  user: {
            \\    name: string | null;
            \\    age: number | null;
            \\    gender: 'male' | 'female' | null;
            \\  }[] | null;
            \\}
        ;

        try runApplyResultSets(result_set, expect, user_type_names, anon_user_types);
    }

    fn runApplyUserType(enum_type: UserTypeDef, expect: Symbol, user_type_names: []const Symbol, anon_user_types: []const UserTypeDef) !void {
        const io = std.testing.io;

        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        var dir = std.testing.tmpDir(.{});
        defer dir.cleanup();
        const parent_path = try dir.dir.realPathFileAlloc(io, ".", allocator);
        defer allocator.free(parent_path);

        var builder = CodeBuilder.init(allocator);
        defer builder.deinit();

        try builder.applyBoundUserType(user_type_names);
        try builder.applyAnonymousUserType(anon_user_types);
        try builder.applyUserType(allocator, enum_type);

        const apply_result = builder.entries.get(.user_type);
        try std.testing.expect(apply_result != null);
        try std.testing.expectEqualStrings(expect, apply_result.?);
    }

    test "generate enum user type#1 (PascalCase type name)" {
        const enum_type: UserTypeDef = .{
            .header = .{
                .type_category = .user, .kind = .@"enum", .name = "Visibility",
            },
            .fields = &.{
                .{.field_name = "hide", .field_type = null}, 
                .{.field_name = "visible", .field_type = null}, 
            },
        };
        const user_type_names = &.{};

        const expect = 
            \\export type Visibility = ('hide' | 'visible') & {_brand: 'Visibility'}
        ;

        try runApplyUserType(enum_type, expect, user_type_names, &.{});
    }

    test "generate enum user type#2 (lowercase type name)" {
        const enum_type: UserTypeDef = .{
            .header = .{
                .type_category = .user, .kind = .@"enum", .name = "visibility",
            },
            .fields = &.{
                .{.field_name = "hide", .field_type = null}, 
                .{.field_name = "visible", .field_type = null}, 
            },
        };
        const user_type_names = &.{"Visibility"};

        const expect = 
            \\export type Visibility = ('hide' | 'visible') & {_brand: 'Visibility'}
        ;

        try runApplyUserType(enum_type, expect, user_type_names, &.{});
    }

    test "generate enum user type#3 (UPPER CASE type name)" {
        const enum_type: UserTypeDef = .{
            .header = .{
                .type_category = .user, .kind = .@"enum", .name = "VISIBILITY",
            },
            .fields = &.{
                .{.field_name = "hide", .field_type = null}, 
                .{.field_name = "visible", .field_type = null},
            },
        };
        const user_type_names = &.{"VISIBILITY"};

        const expect = 
            \\export type Visibility = ('hide' | 'visible') & {_brand: 'Visibility'}
        ;

        try runApplyUserType(enum_type, expect, user_type_names, &.{});
    }

    test "generate enum user type#4 (snake_case type name)" {
        const enum_type: UserTypeDef = .{
            .header = .{
                .type_category = .user, .kind = .@"enum", .name = "USER_PROFILE_KIND",
            },
            .fields = &.{
                .{.field_name = "admin", .field_type = null}, 
                .{.field_name = "general", .field_type = null},
            },
        };
        const user_type_names = &.{"USER_PROFILE_KIND"};

        const expect = 
            \\export type UserProfileKind = ('admin' | 'general') & {_brand: 'UserProfileKind'}
        ;

        try runApplyUserType(enum_type, expect, user_type_names, &.{});
    }

    test "generate struct user type#1 (with predefined user type field)" {
        const user_type: UserTypeDef = .{
            .header = .{
                .type_category = .user, .kind = .@"struct", .name = "USER_PROFILE",
            },
            .fields = &.{
                .{.field_name = "user_id", .field_type = .{.header = .{.type_category = .none, .kind = .primitive, .name = "bigint"}, .fields = &.{}}}, 
                .{.field_name = "name", .field_type = .{.header = .{.type_category = .none, .kind = .primitive, .name = "varchar"}, .fields = &.{}}},
                .{.field_name = "gender", .field_type = .{.header = .{.type_category = .none, .kind = .primitive, .name = "Gender"}, .fields = &.{}}},
            },
        };
        const anon_user_types = &.{};
        const user_type_names = &.{"Gender"};

        const expect = 
            \\export type UserProfile = {
            \\  userId: number | null;
            \\  name: string | null;
            \\  gender: Gender | null;
            \\} & {_brand: 'UserProfile'}
        ;

        try runApplyUserType(user_type, expect, user_type_names, anon_user_types);
    }

    test "generate struct user type#2 (with anonymous struct type field)" {
        const user_type: UserTypeDef = .{
            .header = .{
                .type_category = .user, .kind = .@"struct", .name = "USER_PROFILE",
            },
            .fields = &.{
                .{.field_name = "user_id", .field_type = .{.header = .{.type_category = .user, .kind = .primitive, .name = "bigint"}, .fields = &.{}}}, 
                .{.field_name = "name", .field_type = .{.header = .{.type_category = .none, .kind = .primitive, .name = "varchar"}, .fields = &.{}}},
                .{.field_name = "children", .field_type = .{.header = .{.type_category = .anonymous, .kind = .array, .name = "Anon::Array#1"}, .fields = &.{}}},
            },
        };
        const anon_user_types: []const UserTypeDef = &.{
            .{
                .header = .{.type_category = .anonymous, .kind = .array, .name = "Anon::Array#1"},
                .fields = &.{ 
                    .{
                        .field_name = "Anon::Struct#2", 
                        .field_type = .{
                            .header = .{.type_category = .anonymous, .kind = .@"struct", .name = "Anon::Struct#2"}, 
                            .fields = &.{}
                        }
                    }
                }
            },
            .{
                .header = .{.type_category = .anonymous, .kind = .@"struct", .name = "Anon::Struct#2"},
                .fields = &.{ 
                    .{.field_name = "name", .field_type = .{.header = .{.type_category = .none, .kind = .@"primitive", .name = "varchar"}, .fields = &.{}}},
                    .{.field_name = "age", .field_type = .{.header = .{.type_category = .none, .kind = .@"primitive", .name = "int"}, .fields = &.{}}},
                    .{.field_name = "gender", .field_type = .{.header = .{.type_category = .anonymous, .kind = .@"enum", .name = "Anon::Enum#3"}, .fields = &.{}}},                
                }
            },
            .{
                .header = .{.type_category = .anonymous, .kind = .@"enum", .name = "Anon::Enum#3"},
                .fields = &.{ 
                    .{.field_name = "male", .field_type = null},
                    .{.field_name = "female", .field_type = null},
                }
            },
        };
        const user_type_names = &.{"Gender"};

        const expect = 
            \\export type UserProfile = {
            \\  userId: number | null;
            \\  name: string | null;
            \\  children: {
            \\    name: string | null;
            \\    age: number | null;
            \\    gender: 'male' | 'female' | null;
            \\  }[] | null;
            \\} & {_brand: 'UserProfile'}
        ;

        try runApplyUserType(user_type, expect, user_type_names, anon_user_types);
    }

    test "generate alias user type#1 (primitive type)" {
        const alias_type: UserTypeDef = .{
            .header = .{
                .type_category = .user, .kind = .alias, .name = "Description",
            },
            .fields = &.{
                .{
                    .field_name = "Anon::primitive#1", 
                    .field_type = .{
                        .header = .{.type_category = .none, .kind = .primitive, .name = "VARCHAR"}, 
                        .fields = &.{}
                    }
                }, 
            }
        };
        const expect = 
            \\export type Description = string & {_brand: 'Description'}
        ;

        const anon_user_types = &.{};

        try runApplyUserType(alias_type, expect, &.{}, anon_user_types);
    }

    test "Output build result#1" {
        const io = std.testing.io;

        var arena = std.heap.ArenaAllocator.init((std.testing.allocator));
        defer arena.deinit();
        const allocator = arena.allocator();

        var output_dir = std.testing.tmpDir(.{});
        defer output_dir.cleanup();

        var builder = CodeBuilder.init(allocator);
        defer builder.deinit();

        builder.entries.put(.query, try allocator.dupe(u8, "select $1::id, $2::name from foo where value = $3::value"));
        builder.entries.put(.parameter, try allocator.dupe(u8,"export type P = { id:number|null, name:string|null, value:string|null}"));
        builder.entries.put(.parameter_order, try allocator.dupe(u8,"export const O: (keyof P)[] = ['id', 'name', 'vis', 'status']"));
        builder.entries.put(.result_set, try allocator.dupe(u8, "export type R = { id:number, name:string|null }"));
        _ = try SourceGenerator.build(&builder, io, allocator, output_dir.dir, "Foo");

        query: {
            var file = try output_dir.dir.openFile(io, "Foo/query.sql", .{.mode = .read_only});
            defer file.close(io);

            var buffer: [1024]u8 = undefined;
            var reader = file.reader(io, &buffer);

            const content = try reader.interface.allocRemaining(allocator, .unlimited);
            defer allocator.free(content);

            try std.testing.expectEqualStrings(builder.entries.get(.query).?, content);

            break:query;
        }
        placeholder: {
            var file = try output_dir.dir.openFile(io, "Foo/types.ts", .{});
            defer file.close(io);

            var buffer: [1024]u8 = undefined;
            var reader = file.reader(io, &buffer);

            expect_placeholder: {
                const line = try reader.interface.takeDelimiterExclusive('\n');
                defer allocator.free(line);
                _ = try reader.interface.discardDelimiterInclusive('\n');
                try std.testing.expectEqualStrings(builder.entries.get(.parameter).?, line);
                break:expect_placeholder;
            }
            expect_blank: {
                const line = try reader.interface.takeDelimiterExclusive('\n');
                defer allocator.free(line);
                _ = try reader.interface.discardDelimiterInclusive('\n');
                try std.testing.expectEqualStrings("", line);
                break:expect_blank;
            }
            expect_placeholder_order: {
                const line = try reader.interface.takeDelimiterExclusive('\n');
                defer allocator.free(line);
                _ = try reader.interface.discardDelimiterInclusive('\n');
                try std.testing.expectEqualStrings(builder.entries.get(.parameter_order).?, line);
                break:expect_placeholder_order;
            }
            expect_blank: {
                const line = try reader.interface.takeDelimiterExclusive('\n');
                defer allocator.free(line);
                _ = try reader.interface.discardDelimiterInclusive('\n');
                try std.testing.expectEqualStrings("", line);
                break:expect_blank;
            }
            expect_result_set: {
                const line = try reader.interface.takeDelimiterExclusive('\n');
                defer allocator.free(line);
                _ = try reader.interface.discardDelimiterInclusive('\n');
                try std.testing.expectEqualStrings(builder.entries.get(.result_set).?, line);
                break:expect_result_set;
            }
            expect_eof: {
                try std.testing.expectError(error.EndOfStream, reader.interface.peekByte());
                break:expect_eof;
            }
            break:placeholder;
        }
    }

    test "Output build result#2 (with predefined user type)" {
        const io = std.testing.io;

        var arena = std.heap.ArenaAllocator.init((std.testing.allocator));
        defer arena.deinit();
        const allocator = arena.allocator();

        var output_dir = std.testing.tmpDir(.{});
        defer output_dir.cleanup();

        var builder = CodeBuilder.init(allocator);
        defer builder.deinit();

        builder.entries.put(.parameter, "export type P = { id:number|null, name:string|null, vis:Visibility|null, status: UIStatus|null}");
        builder.entries.put(.parameter_order, "export const O: (keyof P)[] = ['id', 'name', 'vis', 'status']");
        builder.entries.put(.result_set, "export type R = { id:number, name:string|null }");
        try builder.user_type_names.insert("Visibility");
        try builder.user_type_names.insert("ui_status");

        _ = try SourceGenerator.build(&builder, io, allocator, output_dir.dir, "Foo");

        placeholder: {
            var file = try output_dir.dir.openFile(io, "Foo/types.ts", .{});
            defer file.close(io);

            var buffer: [1024]u8 = undefined;
            var reader = file.reader(io, &buffer);

            expect_import: {
                const line = try reader.interface.takeDelimiterExclusive('\n');
                defer allocator.free(line);
                _ = try reader.interface.discardDelimiterInclusive('\n');
                try std.testing.expectEqualStrings("import { type UiStatus } from '../user-types/UiStatus'", line);
                break:expect_import;
            }
            expect_import: {
                const line = try reader.interface.takeDelimiterExclusive('\n');
                defer allocator.free(line);
                _ = try reader.interface.discardDelimiterInclusive('\n');
                try std.testing.expectEqualStrings("import { type Visibility } from '../user-types/Visibility'", line);
                break:expect_import;
            }
            expect_blank: {
                const line = try reader.interface.takeDelimiterExclusive('\n');
                defer allocator.free(line);
                _ = try reader.interface.discardDelimiterInclusive('\n');
                try std.testing.expectEqualStrings("", line);
                break:expect_blank;
            }
            expect_placeholder: {
                const line = try reader.interface.takeDelimiterExclusive('\n');
                defer allocator.free(line);
                _ = try reader.interface.discardDelimiterInclusive('\n');
                try std.testing.expectEqualStrings(builder.entries.get(.parameter).?, line);
                break:expect_placeholder;
            }
            expect_blank: {
                const line = try reader.interface.takeDelimiterExclusive('\n');
                defer allocator.free(line);
                _ = try reader.interface.discardDelimiterInclusive('\n');
                try std.testing.expectEqualStrings("", line);
                break:expect_blank;
            }
            expect_placeholder_order: {
                const line = try reader.interface.takeDelimiterExclusive('\n');
                defer allocator.free(line);
                _ = try reader.interface.discardDelimiterInclusive('\n');
                try std.testing.expectEqualStrings(builder.entries.get(.parameter_order).?, line);
                break:expect_placeholder_order;
            }
            expect_blank: {
                const line = try reader.interface.takeDelimiterExclusive('\n');
                defer allocator.free(line);
                _ = try reader.interface.discardDelimiterInclusive('\n');
                try std.testing.expectEqualStrings("", line);
                break:expect_blank;
            }
            expect_result_set: {
                const line = try reader.interface.takeDelimiterExclusive('\n');
                defer allocator.free(line);
                _ = try reader.interface.discardDelimiterInclusive('\n');
                try std.testing.expectEqualStrings(builder.entries.get(.result_set).?, line);
                break:expect_result_set;
            }
            expect_eof: {
                try std.testing.expectError(error.EndOfStream, reader.interface.peekByte());
                break:expect_eof;
            }
            break:placeholder;
        }
    }

    test "Output build enum user type" {
        const io = std.testing.io;

        var arena = std.heap.ArenaAllocator.init((std.testing.allocator));
        defer arena.deinit();
        const allocator = arena.allocator();

        var output_dir = std.testing.tmpDir(.{});
        defer output_dir.cleanup();

        var builder = CodeBuilder.init(allocator);
        defer builder.deinit();

        builder.entries.put(.user_type, "export type Visibility = ('hide' | 'visible') & {_brand = 'Visibility'}");

        _ = try UserTypeGenerator.build(&builder, io, allocator, output_dir.dir, "Foo");

        user_type: {
            var file = try output_dir.dir.openFile(io, "user-types/Foo.ts", .{});
            defer file.close(io);

            var buffer: [1024]u8 = undefined;
            var reader = file.reader(io, &buffer);

            const content = try reader.interface.allocRemaining(allocator, .unlimited);
            defer allocator.free(content);

            try std.testing.expect(
                std.mem.containsAtLeast(u8, content, 1, builder.entries.get(.user_type).?)
            );

            break:user_type;
        }
    }
};