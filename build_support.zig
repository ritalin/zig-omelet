const std = @import("std");
const clap = @import("clap");

const help = @import("runner/src/settings/help.zig");
const GeneralSetting = @import("runner/src/settings/commands/GeneralSetting.zig");
const CommandSetting = @import("runner/src/settings/commands/commands.zig").CommandSetting;

const findSubcommandTag = @import("runner/src/settings/commands/commands.zig").findTag;

pub const SettingInspection = struct {
    const Symbol = []const u8;

    pub fn inspectArgId(allocator: std.mem.Allocator, args_: ?[]const Symbol) !SettingInspection.Result {
        var map = std.StringHashMap(help.ArgHelpSetting.Tag).init(allocator);

        if (args_ == null) {
            return .{ .subcommand_tag = null, .args = map };
        }

        const args = args_.?;

        try map.ensureTotalCapacity(@intCast(args.len));
        
        var iter = clap.args.SliceIterator{ .args = args, .index = 0 };
        var diag = clap.Diagnostic{};
        // inspect general args
        try inspectArgIdInternal(GeneralSetting.ArgId(.{}), .cmd_general, &iter, &map, &diag);

        // inspect subbcommand args
        const subcommand: ?help.CommandArgId = findSubcommandTag(&diag) catch null; 
        if (subcommand) |tag| {
            switch (tag) {
                .generate => {
                    const Subcommand = std.meta.FieldType(CommandSetting, .generate);
                    try inspectArgIdInternal(Subcommand.ArgId(.{}), .cmd_generate, &iter, &map, null);
                }
            }
        }

        return .{
            .subcommand_tag = subcommand,
            .args = map,
        };
    }

    pub const Result = struct {
        subcommand_tag: ?help.CommandArgId,
        args: std.StringHashMap(help.ArgHelpSetting.Tag),

        pub fn deinit(self: *SettingInspection.Result) void {
            self.args.deinit();
        }

        pub fn subcommand(self: SettingInspection.Result) ?Symbol {
            return if (self.subcommand_tag) |sc| @tagName(sc) else null;
        }

        pub fn iterate(self: *SettingInspection.Result, allocator: std.mem.Allocator, id: help.ArgHelpSetting.Tag) !*Iterator {
            const iter = try allocator.create(Iterator);
            iter.* = .{
                .internal_iter = self.args.iterator(),
                .needle = id,
            };

            return iter;
        }

        pub fn iterateFromSubcommand(self: *SettingInspection.Result, allocator: std.mem.Allocator, subcommand_tag: help.CommandArgId) !*Iterator {
            return switch (subcommand_tag) {
                .generate => self.iterate(allocator, .cmd_generate),
            };
        }

        pub const Iterator = struct {
            internal_iter: std.StringHashMap(help.ArgHelpSetting.Tag).Iterator,
            needle: help.ArgHelpSetting.Tag,

            pub fn deinit(self: *Iterator, allocator: std.mem.Allocator) void {
                allocator.destroy(self);
            }

            pub fn next(self: *Iterator) ?Symbol {
                while (self.internal_iter.next()) |entry| {
                    const k = entry.key_ptr.*;
                    const v = entry.value_ptr.*;

                    if (v == self.needle) return k;
                }

                return null;
            }
        };
    };

    fn inspectArgIdInternal(comptime ArgId: type, help_id: help.ArgHelpSetting.Tag, args_iter: *clap.args.SliceIterator, map: *std.StringHashMap(help.ArgHelpSetting.Tag), diag: ?*clap.Diagnostic) !void {
        var parser = clap.streaming.Clap(ArgId, clap.args.SliceIterator){
            .params = ArgId.Decls,
            .iter = args_iter,
            .diagnostic = diag,
        };

        while (true) {
            const arg_ = parser.next() catch {
                if (diag != null) return else continue;
            };
            if (arg_ == null) break;

            try map.put(args_iter.args[args_iter.index-1], help_id);
        }
    }
};

test "Inspect command args" {
    const args = &.{
        "--reqrep-channel=ipc:///tmp/duckdb-ext-ph/default/req_c2s",
        "--pubsub-channel=ipc:///tmp/duckdb-ext-ph/default/cmd_s2c",
        "generate",
        "--source-dir=./_sql-examples",
        "--output-dir=./_dump/ts",
        "--schema-dir=./_schema-examples",
    };
    const expects: []const ?help.ArgHelpSetting.Tag = &.{
        .cmd_general,
        .cmd_general,
        null,
        .cmd_generate,
        .cmd_generate,
        .cmd_generate,
    };
    
    const allocator = std.testing.allocator;
    var result = try SettingInspection.inspectArgId(allocator, args);
    defer result.deinit();

    const subcommand = result.subcommand();
    try std.testing.expectEqualDeep("generate", subcommand.?);
    try std.testing.expectEqual(expects.len-1, result.args.count());

    try std.testing.expectEqualDeep(expects[0], result.args.get(args[0]));
    try std.testing.expectEqualDeep(expects[1], result.args.get(args[1]));
    try std.testing.expectEqualDeep(expects[2], result.args.get(args[2]));
    try std.testing.expectEqualDeep(expects[3], result.args.get(args[3]));
    try std.testing.expectEqualDeep(expects[4], result.args.get(args[4]));
    try std.testing.expectEqualDeep(expects[5], result.args.get(args[5]));

    subcmd_iter: {
        var set = std.BufSet.init(allocator);
        defer set.deinit();
        var iter = try result.iterate(allocator, .cmd_general);
        defer iter.deinit(allocator);
        while (iter.next()) |arg| { try set.insert(arg); }

        try std.testing.expectEqual(2, set.count());
        try std.testing.expect(set.contains("--reqrep-channel=ipc:///tmp/duckdb-ext-ph/default/req_c2s"));
        try std.testing.expect(set.contains("--pubsub-channel=ipc:///tmp/duckdb-ext-ph/default/cmd_s2c"));
        break:subcmd_iter;
    }
    subcmd_iter: {
        var set = std.BufSet.init(allocator);
        defer set.deinit();
        var iter = try result.iterate(allocator, .cmd_generate);
        defer iter.deinit(allocator);
        while (iter.next()) |arg| { try set.insert(arg); }

        try std.testing.expectEqual(3, set.count());
        try std.testing.expect(set.contains("--source-dir=./_sql-examples"));
        try std.testing.expect(set.contains("--output-dir=./_dump/ts"));
        try std.testing.expect(set.contains("--schema-dir=./_schema-examples"));
        break:subcmd_iter;    
    }
}