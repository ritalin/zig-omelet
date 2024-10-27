const std = @import("std");

const c = @cImport({
    @cInclude("cbor/cbor.h");
});

const RAW_BUFFER_MAX = 9;

pub const Writer = struct {
    raw_buffer: std.ArrayList(u8),
    buffer: std.ArrayList(u8),
    raw_writer: c.cbor_writer_t,
    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) !Self {
        var writer: c.cbor_writer_t = undefined;

        var raw_buffer = try std.ArrayList(u8).initCapacity(allocator, RAW_BUFFER_MAX);
        try raw_buffer.resize(RAW_BUFFER_MAX);

        c.cbor_writer_init(&writer, raw_buffer.items.ptr, raw_buffer.items.len);
        
        return .{
            .raw_buffer = raw_buffer,
            .buffer = std.ArrayList(u8).init(allocator),
            .raw_writer = writer,
        };
    }

    pub fn deinit(self: *Self) void {
        self.raw_buffer.deinit();
        self.buffer.deinit();
    }

    pub fn writeInt(self: *Self, comptime T: type, value: T) !usize {
        comptime std.debug.assert(@typeInfo(T).Int.signedness == .signed);
        const info = @typeInfo(T).Int;
        const uint_type = @Type(.{
            .Int = .{
                .signedness = .unsigned,
                .bits = info.bits,
            }
        });

        if (value >= 0) {
            return self.writeUInt(uint_type, @as(uint_type, @intCast(value)));
        }

        self.raw_writer.bufidx = 0;
        try handleErrUnion(c.cbor_encode_negative_integer(&self.raw_writer, value));
        
        const write_len = self.raw_writer.bufidx;
        
        return self.flushInternal(self.raw_writer.buf[0..write_len]);
    }

    pub fn writeUInt(self: *Self, comptime T: type, value: T) !usize {
        self.raw_writer.bufidx = 0;
        try handleErrUnion(c.cbor_encode_unsigned_integer(&self.raw_writer, value));
        
        const write_len = self.raw_writer.bufidx;
        
        return self.flushInternal(self.raw_writer.buf[0..write_len]);
    }

    pub fn writeBool(self: *Self, value: bool) !usize {
        self.raw_writer.bufidx = 0;
        try handleErrUnion(c.cbor_encode_bool(&self.raw_writer, value));
        
        const write_len = self.raw_writer.bufidx;
        
        return self.flushInternal(self.raw_writer.buf[0..write_len]);
    }

    pub fn writeNull(self: *Self) !usize {
        self.raw_writer.bufidx = 0;
        try handleErrUnion(c.cbor_encode_null(&self.raw_writer));
            
        const write_len = self.raw_writer.bufidx;
        
        return self.flushInternal(self.raw_writer.buf[0..write_len]);
    }

    pub fn writeEnum(self: *Self, comptime T: type, value: T) !usize {
        comptime std.debug.assert(@typeInfo(T) == .Enum);

        const tag_type = @typeInfo(T).Enum.tag_type;
        
        if (@typeInfo(tag_type).Int.signedness == .signed) {
            return self.writeInt(tag_type, @intFromEnum(value));
        }
        else {
            return self.writeUInt(tag_type, @intFromEnum(value));
        }
    }

    pub fn writeBytes(self: *Self, value: []const u8) !usize {
        const write_size = try self.writeLength(2, value.len);
        
        var writer = self.buffer.writer();
        try writer.writeAll(value);

        return write_size + value.len;
    }

    pub fn writeString(self: *Self, value: []const u8) !usize {
        const write_size = try self.writeLength(3, value.len);
        
        var writer = self.buffer.writer();
        try writer.writeAll(value);

        return write_size + value.len;
    }

    pub fn writeCString(self: *Self, value: [:0]const u8) !usize {
        const write_size = try self.writeLength(3, value.len+1);
        
        var writer = self.buffer.writer();
        try writer.writeAll(value);
        try writer.writeByte(0);
        return write_size + value.len + 1;
    }

    pub fn writeSlice(self: *Self, comptime T: type, values: []const T) !usize {
        var write_size = try self.writeSliceHeader(values.len);

        for (values) |value| {
            write_size += try self.writeContainerField(T, value);
        }

        return write_size;
    }

    pub inline fn writeSliceHeader(self: *Self, len: usize) !usize {
        return self.writeLength(4, len);
    }

    pub fn writeTuple(self: *Self, comptime T: type, values: T) !usize {
        comptime std.debug.assert(@typeInfo(T).Struct.is_tuple);
        
        const fields = @typeInfo(T).Struct.fields;
        var write_size = try self.writeLength(4, fields.len);

        inline for (fields) |field| {
            write_size += try self.writeContainerField(field.type, @field(values, field.name));
        }

        return write_size;
    }

    fn writeContainerField(self: *Self, comptime T: type, value: T) !usize {
        switch (@typeInfo(T)) {
            .Int => |t| {
                if (t.signedness == .signed) {
                    return self.writeInt(T, value);
                }
                else {
                    return self.writeUInt(T, value);
                }
            },
            .Bool => {
                return self.writeBool(value);
            },
            .Enum => {
                return self.writeEnum(T, value);
            },
            .Pointer => |t| {
                std.debug.assert(t.size == .Slice);

                if (t.child == u8) {
                    if (t.sentinel == null) {
                        return self.writeString(value);
                    }
                    else {
                        return self.writeCString(value);
                    }
                }
                else {
                    return self.writeSlice(t.child, value);
                }
            },
            .Struct => |t| {
                if (t.is_tuple) {
                    return self.writeTuple(T, value);
                }
                else {
                    unreachable;
                }
            },
            .Optional => |payload| {
                if (value) |v| {
                    return try self.writeContainerField(payload.child, v);
                }
                else {
                    return try self.writeNull();
                }
            },
            else => unreachable,
        }
    }

    fn writeLength(self: *Self, main_type: u8, value: usize) !usize {
        self.raw_writer.bufidx = 0;
        try handleErrUnion(c.cbor_encode_unsigned_integer(&self.raw_writer, value));
        
        self.raw_writer.buf[0] |= (main_type << 5);
        const write_len = self.raw_writer.bufidx;
        
        return self.flushInternal(self.raw_writer.buf[0..write_len]);
    }

    fn flushInternal(self: *Self, data: []const u8) !usize {
        var writer = self.buffer.writer();
        try writer.writeAll(data);

        return data.len;
    }
};

pub const Reader = struct {
    raw_reader: c.cbor_reader_t,
    data: []const u8,
    offset: usize,
    
    const Self = @This();

    pub fn init(data: []const u8) Self {
        return .{
            .raw_reader = undefined,
            .data = data,
            .offset = 0,
        };
    }

    pub fn readInt(self: *Self, comptime T: type) !T {
        comptime std.debug.assert(@typeInfo(T).Int.signedness == .signed);

        const result = try parseOne(&self.raw_reader, self.data[self.offset..]);
        std.debug.assert(result.type == c.CBOR_ITEM_INTEGER);

        return self.readIntInternal(T, result);
    }

    pub fn readUInt(self: *Self, comptime T: type) !T {
        comptime std.debug.assert(@typeInfo(T).Int.signedness == .unsigned);

        const result = try parseOne(&self.raw_reader, self.data[self.offset..]);
        std.debug.assert(result.type == c.CBOR_ITEM_INTEGER);

        return self.readIntInternal(T, result);
    }

    pub fn readBool(self: *Self) !bool {
        const result = try parseOne(&self.raw_reader, self.data[self.offset..]);

        var value: bool = undefined;
        try handleErrUnion(c.cbor_decode(&self.raw_reader, &result, &value, @sizeOf(bool)));

        self.offset += self.raw_reader.msgidx;
        
        return value;
    }

    pub fn readEnum(self: *Self, comptime T: type) !T {
        comptime std.debug.assert(@typeInfo(T) == .Enum);

        const tag_type = @typeInfo(T).Enum.tag_type;

        if (@typeInfo(tag_type).Int.signedness == .signed) {
            return @enumFromInt(try self.readInt(tag_type));
        }
        else {
            return @enumFromInt(try self.readUInt(tag_type));
        }
    }

    fn parseOne(reader: *c.cbor_reader_t, data: []const u8) !c.cbor_item_t {
        var result: c.cbor_item_t = undefined;
        reader.items = &result;
        reader.maxitems = 1;

        try parseInternal(reader, data);

        return result;
    }

    fn parseInternal(reader: *c.cbor_reader_t, data: []const u8) !void {
        try handleErrUnion(c.cbor_parse(reader, data.ptr, data.len, null));
    }

    fn readIntInternal(self: *Self, comptime T: type, result: c.cbor_item_t) !T {     
        var value: T = undefined;
        try handleErrUnion(c.cbor_decode(&self.raw_reader, &result, &value, @sizeOf(T)));

        self.offset += self.raw_reader.msgidx;
        
        return value;
    }

    pub fn readNull(self: *Self, comptime T: type) !?T {
        const result = try parseOne(&self.raw_reader, self.data[self.offset..]);
        std.debug.assert(result.type == c.CBOR_ITEM_SIMPLE_VALUE);

        self.offset += self.raw_reader.msgidx;
        
        return null;
    }

    pub fn nextNull(self: *Self) !bool {
        const result = try parseOne(&self.raw_reader, self.data[self.offset..]);
        return self.isNullResult(result);
    }

    fn isNullResult(self: *Self, item: c.cbor_item_t) !bool {
        if ((item.type == c.CBOR_ITEM_SIMPLE_VALUE) and (item.size == 0)) {
            var v: u8 = undefined;
            try handleErrUnion(c.cbor_decode(&self.raw_reader, &item, &v, @sizeOf(u8)));
            
            if (v == 0) return true;
        }

        return false;
    }

    pub fn readBytes(self: *Self) ![]const u8 {
        const result = try parseOne(&self.raw_reader, self.data[self.offset..]);
        std.debug.assert(result.type == c.CBOR_ITEM_STRING);

        const value = self.data[self.offset..][result.offset..][0..result.size];
        self.offset += self.raw_reader.msgidx;

        return value;
    }

    pub fn readString(self: *Self) ![]const u8 {
        const result = try parseOne(&self.raw_reader, self.data[self.offset..]);
        std.debug.assert(result.type == c.CBOR_ITEM_STRING);

        const value = self.data[self.offset..][result.offset..][0..result.size];
        self.offset += self.raw_reader.msgidx;

        return value;
    }

    pub fn readCString(self: *Self) ![:0]const u8 {
        const result = try parseOne(&self.raw_reader, self.data[self.offset..]);
        std.debug.assert(result.type == c.CBOR_ITEM_STRING);

        const value = self.data[self.offset..][result.offset..][0..result.size-1 :0];
        self.offset += self.raw_reader.msgidx;

        return value;
    }

    pub fn readSliceHeader(self: *Self) !usize {
        const item = try parseOne(&self.raw_reader, self.data[self.offset..]);
        std.debug.assert(item.type == c.CBOR_ITEM_ARRAY);

        self.offset += self.raw_reader.msgidx;
        return item.size;
    }

    pub fn readSlice(self: *Self, allocator: std.mem.Allocator, comptime T: type) ![]const T {
        return self.readSliceWithAllocatorInternal(allocator, T, null);
    }

    pub fn readSliceWithAllocator(self: *Self, allocator: std.mem.Allocator, comptime T: type) ![]const T {
        return self.readSliceWithAllocatorInternal(allocator, T, allocator);
    }
    
    fn readSliceWithAllocatorInternal(self: *Self, allocator: std.mem.Allocator, comptime T: type, member_allocator: ?std.mem.Allocator) ![]const T {
        const prev_items = self.raw_reader.items;
        defer self.raw_reader.items = prev_items;
        
        const item = try parseOne(&self.raw_reader, self.data[self.offset..]);
        return try self.readSliceRecursive(allocator, T, item, member_allocator);
    }

    fn readSliceRecursive(self: *Self, allocator: std.mem.Allocator, comptime T: type, item: c.cbor_item_t, member_allocator: ?std.mem.Allocator) ![]const T {
        self.offset += item.offset;

        const values = try allocator.alloc(T, item.size);

        const child_info = @typeInfo(T);
        
        const cbor_member = comptime CborMember.matchType(T);

        for (values) |*v| {
            const child_item = try parseOne(&self.raw_reader, self.data[self.offset..]);

            if (cbor_member.option.nullable) {
                if (try self.isNullResult(child_item)) {
                    v.* = null;
                    self.offset += self.raw_reader.msgidx;
                    continue;
                }
            }
            
            switch (cbor_member.type) {
                inline .Int, .Bool, .Enum, .String, .CString, .Tuple => {
                    const item_ty = type_item: {
                        if (cbor_member.option.nullable) {
                            break:type_item @typeInfo(T).Optional.child;
                        }
                        else {
                            break:type_item T;
                        }   
                    };
                    
                    v.* = try self.readContainerField(item_ty, cbor_member.type, child_item, member_allocator);
                },
                inline .Slice => {
                    const item_ty = type_item: {
                        if (cbor_member.option.nullable) {
                            break:type_item @typeInfo(child_info.Pointer.child).Optional.child;
                        }
                        else {
                            break:type_item child_info.Pointer.child;
                        }   
                    };
                    
                    v.* = try self.readSliceRecursive(allocator, item_ty, child_item, member_allocator);
                },
            }
        }

        return values;
    }

    pub fn readTuple(self: *Self, comptime T: type) !T {
        return self.readTupleInternal(T, null);
    }
    
    pub fn readTupleWithAllocator(self: *Self, allocator: std.mem.Allocator, comptime T: type) !T {
        return self.readTupleInternal(T, allocator);
    }

    fn readTupleInternal(self: *Self, comptime T: type, member_allocator: ?std.mem.Allocator) !T {
        comptime std.debug.assert(@typeInfo(T).Struct.is_tuple);

        const prev_items = self.raw_reader.items;
        defer self.raw_reader.items = prev_items;
        
        const item = try parseOne(&self.raw_reader, self.data[self.offset..]);
        return try self.readTupleRecursive(T, item, member_allocator);
    }

    fn readTupleRecursive(self: *Self, comptime T: type, item: c.cbor_item_t, member_allocator: ?std.mem.Allocator) !T {
        self.offset += item.offset;

        const fields = @typeInfo(T).Struct.fields;
        var value: T = undefined;

        inline for (fields) |field| {
            const child_item = try parseOne(&self.raw_reader, self.data[self.offset..]);
            const cbor_member = comptime CborMember.matchType(field.type);

            if (cbor_member.option.nullable and try self.isNullResult(child_item)) {
                @field(value, field.name) = null;
                self.offset += self.raw_reader.msgidx;
            }
            else {
                switch(cbor_member.type) {
                    inline .Int, .Bool, .Enum, .String, .CString, .Tuple => {
                        @field(value, field.name) = try self.readContainerField(field.type, cbor_member.type, child_item, member_allocator);
                    },
                    inline .Slice => {
                        if (member_allocator) |a| {
                            const slice_member_type = tuple_member: {
                                if (cbor_member.option.nullable) {
                                    const type_opt = @typeInfo(field.type).Optional;
                                    break:tuple_member @typeInfo(type_opt.child).Pointer;
                                }
                                else {
                                    break:tuple_member @typeInfo(field.type).Pointer;
                                }
                            };
                            @field(value, field.name) = try self.readSliceRecursive(a, slice_member_type.child, child_item, member_allocator);
                        }
                        else {
                            @panic("A tupe containing the slice needs `member_allocator`\n");
                        }
                    }
                }
            }
        }

        return value;
    }

    pub fn readOptional(self: *Self, comptime T: type) !?T {
        return self.readOptionalInternal(T, null);
    }

    pub fn readOptionalWithAllocator(self: *Self, allocator: std.mem.Allocator, comptime T: type) !?T {
        return self.readOptionalInternal(T, allocator);
    }

    pub fn readOptionalInternal(self: *Self, comptime T: type, allocator: ?std.mem.Allocator, ) !?T {
        const result = try parseOne(&self.raw_reader, self.data[self.offset..]);

        if ((result.type == c.CBOR_ITEM_SIMPLE_VALUE) and (result.size == 0)) {
            var v: u8 = undefined;
            try handleErrUnion(c.cbor_decode(&self.raw_reader, &result, &v, @sizeOf(u8)));
            if (v == 0) {
                self.offset += self.raw_reader.msgidx;
                return null;
            }
        }

        const cbor_member = comptime CborMember.matchType(T);

        switch (cbor_member.type) {
            .Slice => { 
                if (allocator) |a| {
                    return try self.readSliceWithAllocator(a, @typeInfo(T).Pointer.child);
                }
                @panic("Optional slice needs `member_allocator`. Call `readOptionalWithAllocator`\n");  
            },          
            .Tuple => {
                return try self.readTupleInternal(T, allocator);
            },
            else => {
                return try self.readContainerField(T, cbor_member.type, result, allocator);
            }
        }
    }

    fn readContainerField(self: *Self, comptime T: type, comptime member_type: CborMember.Type, item: c.cbor_item_t, member_allocator: ?std.mem.Allocator) anyerror!T {
        switch (member_type) {
            .Int => {
                return try self.readIntInternal(T, item);
            },
            .Bool => {
                return try self.readBool();
            },
            .Enum => {
                return try self.readEnum(T);
            },
            .String => {
                const v = try self.readString();
                return if (member_allocator) |a| try a.dupe(u8, v) else v;
            },
            .CString => {
                const v = try self.readCString();
                return if (member_allocator) |a| try a.dupeZ(u8, v) else v;
            },
            .Tuple => {
                return self.readTupleInternal(T, member_allocator);
            },
            else => {
                unreachable;
            }
        }
    }
};

fn handleErrUnion(err: c.cbor_error_t) !void {
    switch (err) {
        c.CBOR_SUCCESS, c.CBOR_OVERRUN => return,
        c.CBOR_ILLEGAL => return error.CborIllegalValue,
        c.CBOR_INVALID => return error.CborInvalidValue,
        c.CBOR_BREAK => return error.CborBreak,
        c.CBOR_EXCESSIVE => return error.CborExcessive,
        else => return error.CborUnexpectedError,
    }
}

pub const CborMember = struct {
    pub const Type = enum {
        Int,
        Bool,
        Enum,
        String,
        CString,
        Slice,
        Tuple,
    };

    type: Type,
    option: struct { nullable: bool = false, },

    pub fn matchType(comptime T: type) CborMember {
        switch (@typeInfo(T)) {
            .Int => return .{.type = .Int, .option = .{}},
            .Bool => return .{.type = .Bool, .option = .{}},
            .Enum => return .{.type = .Enum, .option = .{}},
            .Pointer => |t| {
                if (t.size == .Slice) {
                    if (t.child == u8) {
                        if (t.sentinel) |_| {
                            return .{.type = .CString, .option = .{}};
                        }
                        else {
                            return .{.type = .String, .option = .{}};
                        }
                    }
                    else {
                        return .{.type = .Slice, .option = .{}};
                    }
                }
                @compileError(std.fmt.comptimePrint("Unexpected pointer type: {s}\n", .{@typeName(T)}));
            },
            .Struct => |t| {
                if (t.is_tuple) {
                    return .{.type = .Tuple, .option = .{}};
                }
            },
            .Optional => |payload| {
                const child = matchType(payload.child);
                return .{.type = child.type, .option = .{.nullable = true}};
            },
            else => {}
        }
        @compileError(std.fmt.comptimePrint("Unsupported type: {s}\n", .{@typeName(T)}));
    }
};

test "ReadMemberType mattcher" {
    try std.testing.expectEqualDeep(CborMember{ .type = .Int, .option = .{.nullable = false} }, CborMember.matchType(i8));
    try std.testing.expectEqualDeep(CborMember{ .type = .Int, .option = .{.nullable = false} }, CborMember.matchType(u16));
    try std.testing.expectEqualDeep(CborMember{ .type = .Enum, .option = .{.nullable = false} }, CborMember.matchType(CborMember.Type));
    try std.testing.expectEqualDeep(CborMember{ .type = .String, .option = .{.nullable = false} }, CborMember.matchType([]const u8));
    try std.testing.expectEqualDeep(CborMember{ .type = .String, .option = .{.nullable = false} }, CborMember.matchType([]u8));
    try std.testing.expectEqualDeep(CborMember{ .type = .Slice, .option = .{.nullable = false} }, CborMember.matchType([]const i32));
    try std.testing.expectEqualDeep(CborMember{ .type = .Slice, .option = .{.nullable = false} }, CborMember.matchType([]i32));
    try std.testing.expectEqualDeep(CborMember{ .type = .Tuple, .option = .{.nullable = false} }, CborMember.matchType(struct {i32, u8, []const u8}));
}

test "ReadMemberType mattcher (optional)" {
    try std.testing.expectEqualDeep(CborMember{ .type = .Int, .option = .{.nullable = true} }, CborMember.matchType(?i8));
    try std.testing.expectEqualDeep(CborMember{ .type = .Int, .option = .{.nullable = true} }, CborMember.matchType(?u16));
    try std.testing.expectEqualDeep(CborMember{ .type = .Enum, .option = .{.nullable = true} }, CborMember.matchType(?CborMember.Type));
    try std.testing.expectEqualDeep(CborMember{ .type = .String, .option = .{.nullable = true} }, CborMember.matchType(?[]const u8));
    try std.testing.expectEqualDeep(CborMember{ .type = .String, .option = .{.nullable = true} }, CborMember.matchType(?[]u8));
    try std.testing.expectEqualDeep(CborMember{ .type = .Slice, .option = .{.nullable = true} }, CborMember.matchType(?[]const i32));
    try std.testing.expectEqualDeep(CborMember{ .type = .Slice, .option = .{.nullable = true} }, CborMember.matchType(?[]i32));
    try std.testing.expectEqualDeep(CborMember{ .type = .Tuple, .option = .{.nullable = true} }, CborMember.matchType(?(struct {i32, u8, []const u8})));
}

pub const Lexer = struct {
    pub const Token = union(enum) {
        Header: HeaderTag,
        Short: usize,
        Extend: usize,
        Value: []const u8,
        Invalid: c.cbor_error_t,
    };

    pub fn tokenize(allocator: std.mem.Allocator, source: []const u8) ![]const Token {
        var result = std.ArrayList(Token).init(allocator);

        var reader: c.cbor_reader_t = undefined;
        var item: c.cbor_item_t = undefined;
        c.cbor_reader_init(&reader, &item, 1);

        var offset: usize = 0;

        while (offset < source.len) {
            const err = c.cbor_parse(&reader, source[offset..].ptr, source.len - offset, null);
            defer offset += reader.msgidx;

            if ((err != c.CBOR_SUCCESS) and (err != c.CBOR_OVERRUN)) {
                try result.append(.{.Invalid = err});
            }
            else {
                try result.append(.{.Header = headerTag(item.type)});
                try result.append(.{.Short = item.size});

	            const additional_info: u8 = @intCast(c.get_cbor_additional_info(source[offset]));
                const following_bytes = c.cbor_get_following_bytes(additional_info);

                switch (item.type) {
                    c.CBOR_ITEM_INTEGER, c.CBOR_ITEM_SIMPLE_VALUE => {

                        if (following_bytes > 0) {
                            var ext_size: usize = undefined;
                            _ = c.cbor_decode(&reader, &item, &ext_size, @sizeOf(usize));
                            try result.append(.{.Extend = ext_size});
                        }
                    },
                    c.CBOR_ITEM_STRING => {
                        if (following_bytes == 0) {
                            try result.append(.{.Value = source[offset..][item.offset..][0..item.size]});
                        }
                        else {
                            var ext_size: usize = undefined;
                            _ = c.cbor_decode(&reader, &item, &ext_size, @sizeOf(usize));
                            try result.append(.{.Extend = ext_size});
                            try result.append(.{.Value = source[offset..][item.offset..][0..ext_size]});
                        }
                    },
                    c.CBOR_ITEM_ARRAY => {
                        if (following_bytes > 0) {
                            var ext_size: usize = undefined;
                            _ = c.cbor_decode(&reader, &item, &ext_size, @sizeOf(usize));
                            try result.append(.{.Extend = ext_size});
                        }
                    },
                    else => {},
                }
            }
        }
        return result.toOwnedSlice();
    }

    pub const HeaderTag = enum(u8) {
        CBOR_ITEM_UNKNOWN = c.CBOR_ITEM_UNKNOWN,
        CBOR_ITEM_INTEGER = c.CBOR_ITEM_INTEGER,
        CBOR_ITEM_STRING = c.CBOR_ITEM_STRING,
        CBOR_ITEM_ARRAY = c.CBOR_ITEM_ARRAY,
        CBOR_ITEM_MAP = c.CBOR_ITEM_MAP,
        CBOR_ITEM_FLOAT = c.CBOR_ITEM_FLOAT,
        CBOR_ITEM_SIMPLE_VALUE = c.CBOR_ITEM_SIMPLE_VALUE,        
    };

    fn headerTag(header: c.cbor_item_data_t) HeaderTag {
        return switch (header) {
            c.CBOR_ITEM_INTEGER => .CBOR_ITEM_INTEGER,
            c.CBOR_ITEM_STRING => .CBOR_ITEM_STRING,
            c.CBOR_ITEM_ARRAY => .CBOR_ITEM_ARRAY,
            c.CBOR_ITEM_MAP => .CBOR_ITEM_MAP,
            c.CBOR_ITEM_FLOAT => .CBOR_ITEM_FLOAT,
            c.CBOR_ITEM_SIMPLE_VALUE => .CBOR_ITEM_SIMPLE_VALUE,
            else => .CBOR_ITEM_UNKNOWN,
        };
    }
};

test "Read/Write unsigned int as cbor - Tyny" {
    const allocator = std.testing.allocator;
    
    const expected = 15;
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeUInt(u8, expected);

    var reader = Reader.init(writer.buffer.items);

    const v = try reader.readUInt(u8);

    try std.testing.expectEqual(expected, v);
}

test "Read/Write unsigned int as cbor (nullable#1) - Tyny" {
    const allocator = std.testing.allocator;
    
    const expected: ?u8 = 15;
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeUInt(u8, expected.?);

    var reader = Reader.init(writer.buffer.items);

    const v = try reader.readOptional(u8);

    try std.testing.expectEqual(expected, v);
}

test "Read/Write unsigned int as cbor (nullable#2) - Tyny" {
    const allocator = std.testing.allocator;
    
    const expected: ?u8 = null;
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeNull();

    var reader = Reader.init(writer.buffer.items);

    const v = try reader.readOptional(u8);

    try std.testing.expectEqual(expected, v);
}

test "Read/Write int as cbor - Tyny" {
    const allocator = std.testing.allocator;
    
    const expected = 21;
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeInt(i8, expected);

    var reader = Reader.init(writer.buffer.items);

    const v = try reader.readInt(i8);

    try std.testing.expectEqual(expected, v);
}

test "Read/Write int as cbor (nullable#1) - Tyny" {
    const allocator = std.testing.allocator;
    
    const expected: ?i8 = 15;
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeUInt(u8, expected.?);

    var reader = Reader.init(writer.buffer.items);

    const v = try reader.readOptional(i8);

    try std.testing.expectEqual(expected, v);
}

test "Read/Write int as cbor - Short" {
    const allocator = std.testing.allocator;
    
    const expected = -127;
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeInt(i8, expected);

    var reader = Reader.init(writer.buffer.items);

    const v = try reader.readInt(i8);

    try std.testing.expectEqual(expected, v);
}

test "Read/Write unsigned int as cbor - Short" {
    const allocator = std.testing.allocator;
    
    const expected = 12345;
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeUInt(u16, expected);

    var reader = Reader.init(writer.buffer.items);

    const v = try reader.readUInt(u16);

    try std.testing.expectEqual(expected, v);
}

test "Read/Write boolean: true as cbor - Tyny" {
    const allocator = std.testing.allocator;

    const expected = true;
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeBool(expected);

    var reader = Reader.init(writer.buffer.items);

    const v = try reader.readBool();

    try std.testing.expectEqual(expected, v);
}

test "Read/Write boolean: false as cbor - Tyny" {
    const allocator = std.testing.allocator;

    const expected = false;
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeBool(expected);

    var reader = Reader.init(writer.buffer.items);

    const v = try reader.readBool();

    try std.testing.expectEqual(expected, v);
}

test "Read/Write boolean: true as cbor (nullable#1) - Tyny" {
    const allocator = std.testing.allocator;

    const expected: ?bool = true;
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeBool(expected.?);

    var reader = Reader.init(writer.buffer.items);

    const v = try reader.readOptional(bool);

    try std.testing.expectEqual(expected, v);
}

test "Read/Write enum as cbor - Tyny" {
    const E = enum (u8) { e1 = 1, e2, e3, };

    const allocator = std.testing.allocator;

    const expected: E = .e2;
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeEnum(E, expected);

    var reader = Reader.init(writer.buffer.items);

    const v = try reader.readEnum(E);

    try std.testing.expectEqual(expected, v);
}

test "Read/Write tagless enum as cbor - Tyny" {
    const E = enum { e1, e2, e3, };

    const allocator = std.testing.allocator;

    const expected: E = .e3;
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeEnum(E, expected);

    var reader = Reader.init(writer.buffer.items);

    const v = try reader.readEnum(E);

    try std.testing.expectEqual(expected, v);
}

test "Read/Write negative enum as cbor - Tyny" {
    const E = enum (i8) { e1 = -1, e2, e3, };

    const allocator = std.testing.allocator;
    const expected: E = .e1;
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeEnum(E, expected);

    var reader = Reader.init(writer.buffer.items);

    const v = try reader.readEnum(E);

    try std.testing.expectEqual(expected, v);
}

test "Read/Write tagless enum as cbor (nullable#1) - Tyny" {
    const E = enum { e1, e2, e3, };

    const allocator = std.testing.allocator;

    const expected: ?E = .e3;
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeEnum(E, expected.?);

    var reader = Reader.init(writer.buffer.items);

    const v = try reader.readOptional(E);

    try std.testing.expectEqual(expected, v);
}

test "Read/Write enum as cbor - Small" {
    const E = enum (u16) { e1 = 100, e2, e3, };

    const allocator = std.testing.allocator;
    const expected: E = .e2;
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeEnum(E, expected);

    var reader = Reader.init(writer.buffer.items);

    const v = try reader.readEnum(E);

    try std.testing.expectEqual(expected, v);
}

test "Read/Write binary as cbor - Short" {
    const allocator = std.testing.allocator;
    
    const expected: []const u8 = "\x03\x05" ** 10;
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeBytes(expected);

    var reader = Reader.init(writer.buffer.items);

    const v = try reader.readBytes();

    try std.testing.expectEqualSlices(u8, expected, v);
}

test "Read/Write binary as cbor - Long" {
    const allocator = std.testing.allocator;
    
    const expected: []const u8 = "\x03\x05" ** 100;
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeBytes(expected);

    var reader = Reader.init(writer.buffer.items);

    const v = try reader.readBytes();

    try std.testing.expectEqualSlices(u8, expected, v);
}

test "Read/Write string as cbor - Short" {
    const allocator = std.testing.allocator;
    
    const expected: []const u8 = "19" ** 10;
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeString(expected);

    var reader = Reader.init(writer.buffer.items);

    const v = try reader.readString();

    try std.testing.expectEqualStrings(expected, v);
}

test "Read/Write string as cbor - Long" {
    const allocator = std.testing.allocator;
    
    const expected: []const u8 = "19" ** 128;
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeString(expected);

    var reader = Reader.init(writer.buffer.items);

    const v = try reader.readString();

    try std.testing.expectEqualStrings(expected, v);
}

test "Read/Write string as cbor (nullable#1) - Long" {
    const allocator = std.testing.allocator;
    
    const expected: ?[]const u8 = "19" ** 128;
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeString(expected.?);

    var reader = Reader.init(writer.buffer.items);

    const v = try reader.readOptional([]const u8);

    try std.testing.expectEqualDeep(expected, v);
}

test "Read/Write string as cbor (nullable#2) - Long" {
    const allocator = std.testing.allocator;
    
    const expected: ?[]const u8 = null;
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeNull();

    var reader = Reader.init(writer.buffer.items);

    const v = try reader.readOptional([]const u8);

    try std.testing.expectEqualDeep(expected, v);
}

test "Read/Write c-style string as cbor - Short" {
    const allocator = std.testing.allocator;
    
    const expected: [:0]const u8 = "19" ** 10;
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeCString(expected);

    var reader = Reader.init(writer.buffer.items);

    const v = try reader.readCString();

    try std.testing.expectEqualStrings(expected, v);
}

test "Read/Write c-style string as cbor - Long" {
    const allocator = std.testing.allocator;
    
    const expected: [:0]const u8 = "19" ** 168;
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeCString(expected);

    var reader = Reader.init(writer.buffer.items);

    const v = try reader.readCString();

    try std.testing.expectEqualStrings(expected, v);
}

test "Read/Write string following unsigned integer as cbor" {
    const allocator = std.testing.allocator;
    
    const expected_s: []const u8 = "19" ** 10;
    const expected_u16: u16 = 1024;

    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeString(expected_s);
    _ = try writer.writeUInt(u16, expected_u16);

    var reader = Reader.init(writer.buffer.items);

    const v_s = try reader.readString();
    const v_u16 = try reader.readUInt(u16);

    try std.testing.expectEqualStrings(expected_s, v_s);
    try std.testing.expectEqual(expected_u16, v_u16);
}

test "Read/Write c-style string following unsigned integer as cbor" {
    const allocator = std.testing.allocator;
    
    const expected_s: [:0]const u8 = "19" ** 10;
    const expected_u16: u16 = 1024;

    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeCString(expected_s);
    _ = try writer.writeUInt(u16, expected_u16);

    var reader = Reader.init(writer.buffer.items);

    const v_s = try reader.readCString();
    const v_u16 = try reader.readUInt(u16);

    try std.testing.expectEqualStrings(expected_s, v_s);
    try std.testing.expectEqual(expected_u16, v_u16);
}

fn sliceReadWriteTest(comptime T: type, expected: []const T) !void {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeSlice(T, expected);

    var reader = Reader.init(writer.buffer.items);

    const values = try reader.readSlice(arena.allocator(), T);

    try std.testing.expectEqualDeep(expected, values);    
}

test "Read/Write integer slice as cbor - Tiny" {
    const expected: []const i16 = &.{ 1, 3, 5, 7, 11, 13, 17 };
    try sliceReadWriteTest(i16, expected);
}

test "Read/Write integer slice as cbor (nullable#1) - Tiny" {
    const allocator = std.testing.allocator;

    var writer = try Writer.init(allocator);
    defer writer.deinit();

    const expected: ?[]i16 = @constCast(&[_]i16{ 1, 3, 5, 7, 11, 13, 17 });
    _ = try writer.writeSlice(i16, expected.?);

    var reader = Reader.init(writer.buffer.items);

    const values = try reader.readOptionalWithAllocator(allocator, []const i16);
    defer allocator.free(values.?);

    try std.testing.expectEqualDeep(expected, values);    
}

test "Read/Write integer slice as cbor (nullable#2) - Tiny" {
    const allocator = std.testing.allocator;

    var writer = try Writer.init(allocator);
    defer writer.deinit();

    const expected: ?[]i16 = null;
    _ = try writer.writeNull();

    var reader = Reader.init(writer.buffer.items);

    const values = try reader.readOptionalWithAllocator(allocator, []const i16);

    try std.testing.expectEqualDeep(expected, values);    
}

test "Read/Write integer slice as cbor - Short" {
    const expected: []const i16 = &.{ 1, 3, 555, 7, 1111, 13, 17 };
    try sliceReadWriteTest(i16, expected);
}

test "Read/Write boolean slice as cbor - Tiny" {
    const expected: []const bool = &.{ false, true, true, false };
    try sliceReadWriteTest(bool, expected);
}

test "Read/Write enum slice as cbor - Tiny" {
    const TestEnum = enum { T1, T2, T3, T4, T5};
    const expected: []const TestEnum = &.{ .T1, .T3, .T4 };
    try sliceReadWriteTest(TestEnum, expected);
}

test "Read/Write 2D integer array as cbor - Tiny" {
    const expected: []const []const i16 = &.{ &.{1, 3, 5, 7}, &.{11, 13, 17} };
    try sliceReadWriteTest([]const i16, expected);
}

test "Read/Write string array as cbor - Short" {
    const expected: []const []const u8 = &.{ "'1'", "'3'", "55", "7", "11", "13", "17" };
    try sliceReadWriteTest([]const u8, expected);
}

test "Read/Write 2D string array as cbor - Short" {
    const expected: []const []const []const u8 = &.{ &.{"'1'", "'3'", "55"}, &.{"7", "11", "13", "17"} };
    try sliceReadWriteTest([]const []const u8, expected);
}

fn allocSliceReadWriteTest(comptime T: type, expected: []const T) !void {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeSlice(T, expected);

    var reader = Reader.init(writer.buffer.items);

    const values = try reader.readSliceWithAllocator(arena.allocator(), T);

    try std.testing.expectEqualDeep(expected, values);    
}

test "Read/Write 2D string array as cbor; cloning with allocator - Short" {
    const expected: []const []const []const u8 = &.{ &.{"'1'", "'3'", "55"}, &.{"7", "11", "13", "17"} };
    try allocSliceReadWriteTest([]const []const u8, expected);
}

fn tupleReadWriteTest(comptime T: type, expected: T) !void {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeTuple(T, expected);

    var reader = Reader.init(writer.buffer.items);

    const values = try reader.readTuple(T);

    try std.testing.expectEqualDeep(expected, values);
}

test "Read/Write int tuple - Short" {
    const TestTuple = struct {u16, i16, i32};
    const expected: TestTuple = .{ 42, -108, 1024 };
    try tupleReadWriteTest(TestTuple, expected);
}

test "Read/Write string tuple - Short" {
    const TestTuple = struct {[]const u8, []const u8, []const u8};
    const expected: TestTuple = .{ "item_1", "item_2", "item_3" };
    try tupleReadWriteTest(TestTuple, expected);
}

test "Read/Write string and int tuple - Short" {
    const TestTuple = struct {[]const u8, u16};
    const expected: TestTuple = .{ "item_1", 888 };
    try tupleReadWriteTest(TestTuple, expected);
}

test "Read/Write bool and string tuple - Short" {
    const TestTuple = struct {[]const u8, bool};
    const expected: TestTuple = .{ "item_1", false };
    try tupleReadWriteTest(TestTuple, expected);    
}

test "Read/Write nested int tuple - Short" {
    const TestTupleItem = struct {CborMember.Type, []const u8};
    const TestTuple = struct {u32, TestTupleItem};
    const expected: TestTuple = .{ 655535, .{ .String, "item_1" } };
    try tupleReadWriteTest(TestTuple, expected);
}

test "Read/Write tuple containing slice - Short" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    
    const TestTuple = struct {[]const u32, []const []const u8};
    const expected: TestTuple = .{ &.{42, 101}, &.{ "item_1", "qwerty" } };

    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeTuple(TestTuple, expected);

    var reader = Reader.init(writer.buffer.items);

    const values = try reader.readTupleWithAllocator(arena.allocator(), TestTuple);

    try std.testing.expectEqualDeep(expected, values);
}

test "Read/Write tuple slice slice - Short" {
    const TestTuple = struct {u32, []const u8};
    const expected: [] const TestTuple = &.{ .{42, "item_1"}, .{101 , "qwerty" } };
    try allocSliceReadWriteTest(TestTuple, expected);
}

fn optionalTupleReadWriteTest(comptime T: type, expected: ?T) !void {
    const allocator = std.testing.allocator;
    
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    if (expected) |v| {
        _ = try writer.writeTuple(T, v);
    }
    else {
        _ = try writer.writeNull();
    }

    var reader = Reader.init(writer.buffer.items);

    const values = try reader.readOptional(T);

    try std.testing.expectEqualDeep(expected, values);
}

test "Read/Write tuple (nullable#1) - Short" {
    try optionalTupleReadWriteTest(struct {u8, i8}, .{42, -42});
}

test "Read/Write tuple (nullable#2) - Short" {
    try optionalTupleReadWriteTest(struct {u8, i8}, null);
}

test "Read/Write tuple (nullable member#1) - Short" {
    try optionalTupleReadWriteTest(struct {u8, ?i8}, .{42, null});
}

test "Read/Write tuple (nullable member#2) - Short" {
    try optionalTupleReadWriteTest(struct {u8, ?i8}, .{42, -42});
}

test "Read/Write tuple (nullable member#3) - Short" {
    try optionalTupleReadWriteTest(struct {u8, ?[]const u8}, .{42, "qwerty"});
}

test "Read/Write tuple (nullable member#4) - Short" {
    const T = struct {u8, ?[]const i32};
    const expected: ?T = .{42, &.{1, -2, 4, -8, 16}};
    const allocator = std.testing.allocator;
    
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    if (expected) |v| {
        _ = try writer.writeTuple(T, v);
    }
    else {
        _ = try writer.writeNull();
    }

    var reader = Reader.init(writer.buffer.items);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const values = try reader.readOptionalWithAllocator(arena.allocator(), T);

    try std.testing.expectEqualDeep(expected, values);
}

test "Read/Write tuple (nullable member#5) - Short" {
    const T = struct {u8, ?[]const ?i32};
    const expected: ?T = .{42, &.{null, -2, 4, -8, 16}};
    const allocator = std.testing.allocator;
    
    var writer = try Writer.init(allocator);
    defer writer.deinit();

    if (expected) |v| {
        _ = try writer.writeTuple(T, v);
    }
    else {
        _ = try writer.writeNull();
    }

    var reader = Reader.init(writer.buffer.items);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const values = try reader.readOptionalWithAllocator(arena.allocator(), T);

    try std.testing.expectEqualDeep(expected, values);
}
