const std = @import("std");
const core = @import("core");

const log = core.Logger.SystemDirect(@import("build_options").app_context);

pub const DufaultArg = union (enum) {
    default: void,
    values: []const core.types.Symbol,
    enabled: bool,

    pub fn tag(self: DufaultArg) std.meta.FieldEnum(DufaultArg) {
        return std.meta.activeTag(self);
    }
};

pub fn Defaults(comptime ArgId: type) type {
    return struct {
        map: Self.Map,

        const Self = @This();

        pub const Map = std.enums.EnumMap(ArgId, Arg);
        pub const Arg = DufaultArg;
        pub const Iterator = Self.Map.Iterator;

        pub const default: Self = .{ .map = Self.Map.initFull(.default) };

        pub fn loadFromFile(io: std.Io, allocator: std.mem.Allocator, file: std.Io.File, log_style: core.Logger.LogStyle, on_apply: ApplyDefaultHandler) !void {
            var buffer: [1024]u8 = undefined;
            var reader = file.reader(io, &buffer);
            const content = try reader.interface.allocRemainingAlignedSentinel(allocator, .unlimited, .@"1", 0);
            defer allocator.free(content);

            var self: Self = .{ .map = .{} };

            try self.loadFromSource(allocator, content, log_style);

            try (on_apply.handler)(on_apply.ptr, allocator, &self);
        }

        fn loadFromSource(self: *Self, allocator: std.mem.Allocator, contents: [:0]const u8, error_style: core.Logger.LogStyle) !void {
            var ast = try std.zig.Ast.parse(allocator, contents, .zon);
            defer ast.deinit(allocator);
            if (ast.errors.len > 0) {
                if (error_style == .stderr) {
                    for (ast.errors) |err| {
                        var buffer: [1024]u8 = undefined;
                        const t = std.debug.lockStderr(&buffer).terminal();
                        defer std.debug.unlockStderr();
                        try ast.renderError(err, t.writer);
                    }
                }
                return error.InvalidSettingFile;
            }

            var ir = try std.zig.ZonGen.generate(allocator, ast, .{});
            defer ir.deinit(allocator);

            const root_index: std.zig.Zoir.Node.Index = .root;
            const root_node = root_index.get(ir);
            switch (root_node) {
                .struct_literal => {},
                .empty_literal => return,
                else => return error.InvalidSettingFile,
            }

            const fields = std.meta.fields(ArgId);

            for (root_node.struct_literal.names, 0..) |ident_name, i| {
                const name = ident_name.get(ir);
                const node_index = root_node.struct_literal.vals.at(@intCast(i));

                apply: {
                    inline for (fields) |f| {
                        if (std.mem.eql(u8, f.name, name)) {
                            const key: ArgId = @enumFromInt(f.value);
                            const val = 
                                std.zon.parse.fromZoirNodeAlloc(Self.Arg, allocator, ast, ir, node_index, null, .{})
                                catch return error.InvalidSettingEntry
                            ;

                            self.map.put(key, val);
                            break:apply;
                        }
                    }
                    return error.InvalidSettingKey;
                }
            }
        }

        pub fn iterator(self: *Self) Self.Iterator {
            return self.map.iterator();
        }

        pub const ApplyDefaultHandler = struct {
            ptr: *anyopaque,
            handler: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, defaults: *Self) anyerror!void,
        };
    };
}

test "test default setting" {
    std.testing.refAllDecls(@This());
}

pub const tests = struct {
    const ArgId = @import("./commands/Generate.zig").ArgId(.{});

    test "Empty defaults" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const contents: [:0]const u8 = ".{}";

        var self: Defaults(ArgId) = .{ .map = .{} };
        try self.loadFromSource(allocator, contents, .discard);
        try std.testing.expectEqual(0, self.map.count());
    }

    test "All default tag (subcommand: generate)" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const contents: [:0]const u8 =
            \\.{
            \\    .source_dir_set = .default,
            \\    .schema_dir_set = .default,
            \\    .include_filter_set = .default,
            \\    .exclude_filter_set = .default,
            \\    .output_dir_path = .default,
            \\}
        ;
        const expect: Defaults(ArgId) = .default;

        var self: Defaults(ArgId) = .{ .map = .{} };
        try self.loadFromSource(allocator, contents, .discard);

        try std.testing.expectEqualDeep(expect.map, self.map);
    }

    test "All fixed args (subcommand: generate)" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const contents: [:0]const u8 =
            \\.{
            \\    .source_dir_set = .{.values = .{"queries"}},
            \\    .schema_dir_set = .{.values = .{"./schemas"}},
            \\    .include_filter_set = .{.values = .{"queries"}},
            \\    .exclude_filter_set = .{.values = .{"./table", "./indexes"}},
            \\    .output_dir_path = .{.values = .{"./output"}},
            \\}
        ;
        const expect: Defaults(ArgId) = .{ 
            .map = Defaults(ArgId).Map.init(.{
                .source_dir_set = .{.values = &.{"queries"}},
                .schema_dir_set = .{.values = &.{"./schemas"}},
                .include_filter_set = .{.values = &.{"queries"}},
                .exclude_filter_set = .{.values = &.{"./table", "./indexes"}},
                .output_dir_path = .{.values = &.{"./output"}},
            }),
        };

        var self: Defaults(ArgId) = .{ .map = .{} };
        try self.loadFromSource(allocator, contents, .discard);

        try std.testing.expectEqualDeep(expect.map, self.map);
    }
    
    test "Invalid default#1 (root node)" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const contents: [:0]const u8 = "[]";

        var self: Defaults(ArgId) = .{ .map = .{} };
        try std.testing.expectError(error.InvalidSettingFile, self.loadFromSource(allocator, contents, .discard));
    }

    test "Invalid default#2 (unknown key)" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const contents: [:0]const u8 = ".{.qwerty = .default}";

        var self: Defaults(ArgId) = .{ .map = .{} };
        try std.testing.expectError(error.InvalidSettingKey, self.loadFromSource(allocator, contents, .discard));
    }

    test "Invalid default#3 (unknown arg tag#1)" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const contents: [:0]const u8 = ".{.output_dir_path = .xyz}";

        var self: Defaults(ArgId) = .{ .map = .{} };
        try std.testing.expectError(error.InvalidSettingEntry, self.loadFromSource(allocator, contents, .discard));
    }
};
