const std = @import("std");
const core = @import("core");

const Config = @import("../configs/Config.zig");
const Setting = @import("../settings/Setting.zig");
const ExtraArg = @import("../configs/types.zig").ExtraArg;
const DufaultArg = @import("../settings/default_args.zig").DufaultArg;

const GenerateSetting = @import("../settings/commands/Generate.zig");

processes: []?std.process.Child,

const Self = @This();

pub fn launch(io: std.Io, allocator: std.mem.Allocator, guest_configs: *const std.MultiArrayList(Config.Guest), setting: *const Setting) !Self {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    
    const processes = try allocator.alloc(?std.process.Child, guest_configs.len);

    const kinds = guest_configs.items(.kind);
    const modes = guest_configs.items(.mode);
    const exe_paths = guest_configs.items(.location);
    const extra_args = guest_configs.items(.extra_args);

    const managed_allocator = arena.allocator();

    for (kinds, modes, exe_paths, extra_args, 0..) |kind, mode, path, extra, i| {
        if (mode == .managed) {
            var args: std.ArrayListUnmanaged(core.types.Symbol) = .empty;
            defer args.deinit(managed_allocator);

            try writeBaseArgs(managed_allocator, path, &setting.base, &args);
            try stitchGuestArgs(managed_allocator, kind, &extra, &setting.command, &args);

            processes[i] = try std.process.spawn(io, .{ .argv = args.items });
        }
        else {
            processes[i] = null;
        }
    }

    return .{
        .processes = processes,
    };
}

pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
    allocator.free(self.processes);
}

pub fn wait(self: *const Self, io: std.Io) !void {
    var tasks: std.Io.Group = .init;
    for (self.processes) |process| {
        if (process) |p| {
            tasks.async(io, waitProcess, .{ io, p });
        }
    }
    try tasks.await(io);
}

fn waitProcess(io: std.Io, p: std.process.Child) void {
    var process = p;
    _ = process.wait(io) catch {};
}

const LogStyleTag = std.meta.FieldEnum(core.Logger.LogStyle); 

fn writeBaseArgs(
    allocator: std.mem.Allocator,
    path: core.types.FilePath,
    base_setting: *const Setting.BaseSetting,
    args: *std.ArrayListUnmanaged(core.types.Symbol)) !void 
{
    executable_path: {
        try args.append(allocator, path);
        break:executable_path;
    }
    base_setting: {
        const log_level: core.types.Symbol = @tagName(base_setting.log_level);
        const log_style: core.types.Symbol = if (base_setting.log_quiet) @tagName(LogStyleTag.discard) else @tagName(LogStyleTag.integrated);

        const base_setting_args: ExtraArg(BaseSettingArgId) = .{
            .log_level = .{.values = &.{ log_level }},
            .log_style = .{.values = &.{ log_style }},
            .no_color = .{.enabled = base_setting.no_color },
            .req_rep = .{.values = &.{ base_setting.endpoints.req_rep }},
            .pub_sub = .{.values = &.{ base_setting.endpoints.pub_sub }},
            .push_pull = .{.values = &.{ base_setting.endpoints.push_pull }},
        };
        try writeArgs(allocator, BaseSettingArgId, &base_setting_args, &.{}, args);
        break:base_setting;
    }
}

const BaseSettingArgId = core.configs.guests.GuestBaseConfiig.ArgId(.{});
const GuestWatchArgId = core.configs.guests.GuestWatch.ArgId(.{});
const GuestExtractArgId = core.configs.guests.GuestExtract.ArgId(.{});
const GuestGenerateArgId = core.configs.guests.GuestGenerate.ArgId(.{});

fn stitchGuestArgs(
    allocator: std.mem.Allocator,
    kind: core.configs.types.StageKind, 
    extra_set: *const Config.Guest.ExtraArgSet,
    command_setting: *const Setting.SubcommandSetting,
    args: *std.ArrayListUnmanaged(core.types.Symbol)) !void
{
    switch (kind) {
        .watch => {
            const default_args: ExtraArg(GuestWatchArgId) = .{
                .source_dir_set = .{.values = command_setting.generate.source_dir_set},
                .schema_dir_set = .{.values = command_setting.generate.schema_dir_set},
                .include_filter_set = .{.values = command_setting.generate.include_filter_set},
                .exclude_filter_set = .{.values = command_setting.generate.exclude_filter_set},
                .watch = .{.enabled = command_setting.generate.watch},
            };
            try writeArgs(allocator, GuestWatchArgId, &extra_set.watch, &default_args, args);
        },
        .extract => {
            const default_args: ExtraArg(GuestExtractArgId) = .{
                .schema_dir_set = .{.values = command_setting.generate.schema_dir_set},
            };
            try writeArgs(allocator, GuestExtractArgId, &extra_set.extract, &default_args, args);
        },
        .generate => {
            const default_args: ExtraArg(GuestGenerateArgId) = .{
                .output_dir_path = .{.values = &.{command_setting.generate.output_dir_path}},
            };
            try writeArgs(allocator, GuestGenerateArgId, &extra_set.generate, &default_args, args);
        },
        .init => {
            unreachable;
        },
    }
}

fn writeArgs(
    allocator: std.mem.Allocator,
    comptime GuestArgId: type, 
    extra_set: *const ExtraArg(GuestArgId),
    default_extra_args: *const ExtraArg(GuestArgId),
    args: *std.ArrayListUnmanaged(core.types.Symbol)) !void
{
    inline for (comptime std.meta.fields(GuestArgId), GuestArgId.Decls) |f, decl| {
        comptime {
            std.debug.assert(decl.id == @as(GuestArgId, @enumFromInt(f.value)));
        }
        const value = @field(extra_set, f.name);

        writeArgsInternal(allocator, decl.names.long, value, args)
        catch |err| switch (err) {
            error.NeedDefaultValue => {
                const default_value = @field(default_extra_args, f.name);
                try writeArgsInternal(allocator, decl.names.long, default_value, args);
            },
            else => return err,
        };
    }
}

fn writeArgsInternal(allocator: std.mem.Allocator, name: core.types.Symbol, value: DufaultArg, args: *std.ArrayListUnmanaged(core.types.Symbol)) !void {
    switch (value) {
        .default => {
            return error.NeedDefaultValue;
        },
        .values => |values| {
            for (values) |v| {
                const arg = try std.fmt.allocPrint(allocator, "--{s}={s}", .{name, v});
                try args.append(allocator,arg);
            }
        },
        .enabled => |enabled| {
            if (enabled) {
                const arg = try std.fmt.allocPrint(allocator, "--{s}", .{name });
                try args.append(allocator,arg);
            }
        }
    }
}

test "Guest launcher test" {
    std.testing.refAllDecls(@This());
}

pub const tests = struct {
    test "write base setting" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const setting: Setting.BaseSetting = .{
            .log_level = .info,
            .log_quiet = false,
            .no_color = true,
            .scope = "test",
            .endpoints = .{
                .req_rep = "ipc:///path/to/req-rep",
                .pub_sub = "ipc:///path/to/pub-sub",
                .push_pull = "ipc:///path/to/push-pull",
            },
            .ipc_config = .default,
        };

        var args: std.ArrayListUnmanaged(core.types.Symbol) = .empty;
        defer args.deinit(allocator);

        try writeBaseArgs(allocator, "/path/to/guest", &setting, &args);

        try std.testing.expectEqual(7, args.items.len);
        try std.testing.expectEqualStrings("/path/to/guest", args.items[0]);
        try std.testing.expectEqualStrings("--reqrep-channel=ipc:///path/to/req-rep", args.items[1]);
        try std.testing.expectEqualStrings("--pubsub-channel=ipc:///path/to/pub-sub", args.items[2]);
        try std.testing.expectEqualStrings("--pushpull-channel=ipc:///path/to/push-pull", args.items[3]);
        try std.testing.expectEqualStrings("--log-level=info", args.items[4]);
        try std.testing.expectEqualStrings("--log-style=integrated", args.items[5]);
        try std.testing.expectEqualStrings("--no-color", args.items[6]);
    }

    test "write guest-watch setting#1 (all default extra)" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const setting: GenerateSetting = .{
            .source_dir_set = &.{ "/path/to/setting-source-1", "/path/to/setting-source-2" },
            .schema_dir_set = &.{ "/path/to/setting-schema-2", "/path/to/setting-schema-1" },
            .include_filter_set = &.{ "foo", "baz" },
            .exclude_filter_set = &.{ "bar", "quax" },
            .output_dir_path = "/path/to/setting-out",
            .watch = false,
        };
        const extra_args: ExtraArg(GuestWatchArgId) = .{};

        var args: std.ArrayListUnmanaged(core.types.Symbol) = .empty;
        defer args.deinit(allocator);

        try stitchGuestArgs(allocator, .watch, &.{.watch = extra_args}, &.{.generate = setting}, &args);

        try std.testing.expectEqual(8, args.items.len);
        try std.testing.expectEqualStrings("--source-dir=/path/to/setting-source-1", args.items[0]);
        try std.testing.expectEqualStrings("--source-dir=/path/to/setting-source-2", args.items[1]);
        try std.testing.expectEqualStrings("--schema-dir=/path/to/setting-schema-2", args.items[2]);
        try std.testing.expectEqualStrings("--schema-dir=/path/to/setting-schema-1", args.items[3]);
        try std.testing.expectEqualStrings("--include-filter=foo", args.items[4]);
        try std.testing.expectEqualStrings("--include-filter=baz", args.items[5]);
        try std.testing.expectEqualStrings("--exclude-filter=bar", args.items[6]);
        try std.testing.expectEqualStrings("--exclude-filter=quax", args.items[7]);
    }

    test "write guest-watch setting#1 (all explicit extra)" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const setting: GenerateSetting = .{
            .source_dir_set = &.{ "/path/to/setting-source-1", "/path/to/setting-source-2" },
            .schema_dir_set = &.{ "/path/to/setting-schema-2", "/path/to/setting-schema-1" },
            .include_filter_set = &.{ "foo", "baz" },
            .exclude_filter_set = &.{ "bar", "quax" },
            .output_dir_path = "/path/to/setting-out",
            .watch = false,
        };
        const extra_args: ExtraArg(GuestWatchArgId) = .{
            .source_dir_set = .{ .values = &[_]core.types.FilePath{ "/path/to/explicit-source" } },
            .schema_dir_set = .{ .values = &[_]core.types.FilePath{ "/path/to/explicit-schema" } },
            .include_filter_set = .{ .values = &[_]core.types.FilePath{ "baz" } },
            .exclude_filter_set = .{ .values = &[_]core.types.FilePath{ "bar" } },
            .watch = .{ .enabled = true },
        };

        var args: std.ArrayListUnmanaged(core.types.Symbol) = .empty;
        defer args.deinit(allocator);

        try stitchGuestArgs(allocator, .watch, &.{.watch = extra_args}, &.{.generate = setting}, &args);

        try std.testing.expectEqual(5, args.items.len);
        try std.testing.expectEqualStrings("--source-dir=/path/to/explicit-source", args.items[0]);
        try std.testing.expectEqualStrings("--schema-dir=/path/to/explicit-schema", args.items[1]);
        try std.testing.expectEqualStrings("--include-filter=baz", args.items[2]);
        try std.testing.expectEqualStrings("--exclude-filter=bar", args.items[3]);
        try std.testing.expectEqualStrings("--watch", args.items[4]);
    }

    test "write guest-extract setting#1 (all default extra)" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const setting: GenerateSetting = .{
            .source_dir_set = &[_]core.types.FilePath{ "/path/to/setting-source" },
            .schema_dir_set = &[_]core.types.FilePath{ "/path/to/setting-schema" },
            .include_filter_set = &[_]core.types.FilePath{ "foo" },
            .exclude_filter_set = &[_]core.types.FilePath{ "bar" },
            .output_dir_path = "/path/to/setting-out",
            .watch = false,
        };
        const extra_args: ExtraArg(GuestExtractArgId) = .{};

        var args: std.ArrayListUnmanaged(core.types.Symbol) = .empty;
        defer args.deinit(allocator);

        try stitchGuestArgs(allocator, .extract, &.{.extract = extra_args}, &.{.generate = setting}, &args);

        try std.testing.expectEqual(1, args.items.len);
        try std.testing.expectEqualStrings("--schema-dir=/path/to/setting-schema", args.items[0]);
    }

    test "write guest-extract setting#1 (all explicit extra)" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const setting: GenerateSetting = .{
            .source_dir_set = &.{ "/path/to/setting-source" },
            .schema_dir_set = &.{ "/path/to/setting-schema" },
            .include_filter_set = &.{ "foo" },
            .exclude_filter_set = &.{ "bar" },
            .output_dir_path = "/path/to/setting-out",
            .watch = false,
        };
        const extra_args: ExtraArg(GuestExtractArgId) = .{
            .schema_dir_set = .{ .values = &[_]core.types.FilePath{ "/path/to/explicit-schema" } }
        };

        var args: std.ArrayListUnmanaged(core.types.Symbol) = .empty;
        defer args.deinit(allocator);

        try stitchGuestArgs(allocator, .extract, &.{.extract = extra_args}, &.{.generate = setting}, &args);

        try std.testing.expectEqual(1, args.items.len);
        try std.testing.expectEqualStrings("--schema-dir=/path/to/explicit-schema", args.items[0]);
    }

    test "write guest-generate setting#1 (all default extra)" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const setting: GenerateSetting = .{
            .source_dir_set = &.{ "/path/to/setting-source" },
            .schema_dir_set = &.{ "/path/to/setting-schema" },
            .include_filter_set = &.{ "foo" },
            .exclude_filter_set = &.{ "bar" },
            .output_dir_path = "/path/to/setting-out",
            .watch = false,
        };
        const extra_args: ExtraArg(GuestGenerateArgId) = .{};

        var args: std.ArrayListUnmanaged(core.types.Symbol) = .empty;
        defer args.deinit(allocator);

        try stitchGuestArgs(allocator, .generate, &.{.generate = extra_args}, &.{.generate = setting}, &args);

        try std.testing.expectEqual(1, args.items.len);
        try std.testing.expectEqualStrings("--output-dir=/path/to/setting-out", args.items[0]);
    }

    test "write guest-generate setting#1 (all explicit extra)" {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const setting: GenerateSetting = .{
            .source_dir_set = &.{ "/path/to/setting-source" },
            .schema_dir_set = &.{ "/path/to/setting-schema" },
            .include_filter_set = &.{ "foo" },
            .exclude_filter_set = &.{ "bar" },
            .output_dir_path = "/path/to/setting-out",
            .watch = false,
        };

        const extra_args: ExtraArg(GuestGenerateArgId) = .{
            .output_dir_path = .{ .values = &[_]core.types.FilePath{ "/path/to/explicit-out" } }
        };

        var args: std.ArrayListUnmanaged(core.types.Symbol) = .empty;
        defer args.deinit(allocator);

        try stitchGuestArgs(allocator, .generate, &.{.generate = extra_args}, &.{.generate = setting}, &args);

        try std.testing.expectEqual(1, args.items.len);
        try std.testing.expectEqualStrings("--output-dir=/path/to/explicit-out", args.items[0]);
    }
};