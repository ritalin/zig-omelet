const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const FilePath = core.types.FilePath;
const Symbol = core.types.Symbol;
const ArgScanner = core.settings.types.ArgScanner;
const ArgParserPair = core.settings.types.ArgParserPair;
const ConfigCategory = core.configs.types.ConfigCategory;

const default_init_scope = @import("build_options").default_init_scope;
const BaseSetting = @import("./BaseSetting.zig");

const BaseSettingArgId = BaseSetting.ArgId(.{});
const IntialzeArgId = ArgId(.{});
const Defaults = @import("../default_args.zig").Defaults(std.meta.FieldEnum(IntialzeArgId));

source_dir_path: FilePath,
output_dir_path: FilePath,
target_scope: Symbol,

const Setting = @This();

pub fn deinit(self: *Setting, allocator: std.mem.Allocator) void {
    _ = self;
    _ = allocator;
} 

pub fn ArgId(comptime descriptions: core.settings.types.DescriptionMap) type {
    return enum {
        target_scope,
        from_scope,
        global,

        pub const Decls: []const clap.Param(@This()) = &.{
            .{.id = .target_scope, .names = .{.long = "target-scope", .short = 's'}, .takes_value = .one},
            .{.id = .from_scope, .names = .{.long = "from-scope", .short = 's'}, .takes_value = .one},
            .{.id = .global, .names = .{.long = "global", .short = 'g'}, .takes_value = .none},
        };

        const arg_view = core.settings.types.ArgHelp(@This(), descriptions);
        pub const description = arg_view.description;
        pub const value = arg_view.value;
    };
}

pub const strategies = core.configs.types.StageStrategy.init(.{
    .init = .one,
});

pub fn Builder(comptime ArgIterator: type) type {
    return struct {
        build_log_style: core.Logger.LogStyle,
        category: ConfigCategory,
//  TODO:
//     category: core.ConfigCategory,
//     subcommand: ?Symbol = null,
//     from_scope_required: bool,
//     unsupported_set: std.enums.EnumSet(core.SubcommandArgId),
        target_scope: ?Symbol = null,
        from_scope: ?Symbol = null,
        global: ?bool = null,

    //     pub fn init(allocator: std.mem.Allocator, category: core.ConfigCategory, from_scope_required: bool, unsupported: UnsupportedCommands) Builder {
    //         return .{
    //             .allocator = allocator,
    //             .category = category,
    //             .scope_set = std.ArrayList(Symbol).init(allocator),
    //             .from_scope_required = from_scope_required,
    //             .unsupported_set = std.enums.EnumSet(core.SubcommandArgId).init(unsupported),
    //         };
    //     }


        pub fn fromArgs(
            allocator: std.mem.Allocator, 
            scanner: *ArgScanner(ArgIterator), 
            base_builder: *BaseSetting.Builder(ArgIterator), 
            category: ConfigCategory,
            log_style: core.Logger.LogStyle) !Builder(ArgIterator) 
        {
            var diag: clap.Diagnostic = .{}; 
            var parsers = ArgParserPair(IntialzeArgId, BaseSettingArgId, ArgIterator).init(scanner, &diag);

            var builder: Builder(ArgIterator) = .{ .build_log_style = log_style, .category = category };

            while (scanner.scan()) {
                const next_arg = parsers.next(scanner) catch |err| {
                    if (log_style == .stderr) {
                        try core.log_supports.reportClapError(&diag, err);
                    }
                    return err;
                };
                if (next_arg == null) break;

                switch (next_arg.?) {
                    .base => |arg| {
                        try builder.handleArg(allocator, arg);
                    },
                    .extra => |arg| {
                        base_builder.handleArg(allocator, arg) catch return error.ShowCommandHelp;
                    }
                }
            }

            return builder;

    // TODO:
    //         var diag: clap.Diagnostic = .{};
    //         var parser = clap.streaming.Clap(InitArgId(.{}), std.process.ArgIterator){
    //             .params = InitArgId(.{}).Decls,
    //             .iter = iter,
    //             .diagnostic = &diag,
    //         };

    //         while (true) {
    //             const arg_ = parser.next() catch |err| {
    //                 try diag.report(std.io.getStdErr().writer(), err);
    //                 return error.ShowCommandHelp;
    //             };
    //             if (arg_ == null) {
    //                 return try self.build();
    //             }

    //             if (arg_) |arg| {
    //                 switch (arg.param.id) {
    //                     .subcommand => self.subcommand = arg.value,
    //                     .global => self.global = true,
    //                     .new_scope => if (arg.value) |v| try self.scope_set.append(v),
    //                     .from_scope => self.from_scope = arg.value,
    //                 }
    //             }
    //         }
        }

        fn handleArg(self: *Builder(ArgIterator), allocator: std.mem.Allocator, arg: clap.streaming.Arg(IntialzeArgId)) !void {
            _ = allocator;

            switch (arg.param.id) {
                .target_scope => {
                    self.target_scope = arg.value;
                },
                .from_scope => {
                    self.from_scope = arg.value;
                },
                .global => {
                    self.global = true;
                },
            }
        }

        fn applyDefaults(ptr: *anyopaque, allocator: std.mem.Allocator, defaults: *Defaults) !void {
            _ = allocator;

            var self: *Builder(ArgIterator) = @ptrCast(@alignCast(ptr));
            var iter = defaults.iterator();

            while (iter.next()) |entry| {
                switch (entry.key) {
                    .target_scope => {
                        if (entry.value.tag() == .values) {
                            self.target_scope = entry.value.values[0];
                        }
                    },
                    .from_scope => {
                        if (entry.value.tag() == .values) {
                            self.from_scope = entry.value.values[0];
                        }
                    },
                    .global => {
                        if (entry.value.tag() == .enabled) {
                            self.global = entry.value.enabled;
                        }
                    },
                }
            }
        }

        pub fn build(self: Builder(ArgIterator), io: std.Io, allocator: std.mem.Allocator, env: *const std.process.Environ.Map, options: core.configs.supports.FileResolveOptions) !Setting {
            var arena = std.heap.ArenaAllocator.init(allocator);
            defer arena.deinit();
            
            var builder_default: Builder(ArgIterator) = .{ .build_log_style = self.build_log_style, .category = self.category, };            
            
            if (try core.configs.supports.resolveFileCandidate(io, arena.allocator(), env, options)) |file| {
                defer file.close(io);

                const callback: Defaults.ApplyDefaultHandler = .{ 
                    .ptr = &builder_default, 
                    .handler = Builder(ArgIterator).applyDefaults 
                };
                try Defaults.loadFromFile(io, arena.allocator(), file, self.build_log_style, callback);
            }

            var has_err = false;

            const from_scope = self.from_scope orelse builder_default.from_scope orelse default_init_scope;
            const source_dir_path = try core.configs.supports.resolveConfigDirPath(io, allocator, env, from_scope, self.category, options.root);
            if (source_dir_path == null) {
                has_err = true;
                if (self.build_log_style == .stderr) {
                    std.log.warn("Can not find setting / config file path.", .{});
                }
            }

            const output_dir_path = path: {
                const output_root_candidate = global_path: {
                    if (self.global orelse builder_default.global) |global| {
                        if (global) break:global_path core.configs.supports.RootPathCandidate{.home_dir = options.root.home_dir.?};
                    }
                    break:global_path null;
                };
                break:path try core.configs.supports.formatConfigRootDirPath(io, allocator, env, self.category, output_root_candidate orelse .{.current_dir = options.root.current_dir.?});
            };
            const target_scope = self.target_scope orelse builder_default.target_scope;
            if (target_scope == null) {
                has_err = true;
                if (self.build_log_style == .stderr) {
                    std.log.warn("Need to specify target scope.", .{});
                }
            }

            if (has_err) {
                return error.LoadSettingFailed;
            }

            return .{
                .source_dir_path = source_dir_path.?,
                .output_dir_path = output_dir_path,
                .target_scope = try allocator.dupe(u8, target_scope.?),
            };
        }
    };
}

test "Configuraton init" {
    std.testing.refAllDecls(@This());
}

pub const tests = struct {
    const ConfigFileCandidates = core.configs.types.ConfigFileCandidates;
    const FileResolveOptions = core.configs.supports.FileResolveOptions;
    const writeAssetFile = @import("../../supports/test_support.zig").writeAssetFile;
    const TetsArgsIterator = clap.args.SliceIterator;

    test "All explicit args for init-config" {
        const io = std.testing.io;
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        var tmp_dir = std.testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        const src_root_path = path: {
            const d = try tmp_dir.dir.createDirPathOpen(io, "src", .{});
            break:path try d.realPathFileAlloc(io, ".", allocator);
        };
        const home_path = path: {
            try tmp_dir.dir.createDirPath(io, "out/configs");
            const d = try tmp_dir.dir.createDirPathOpen(io, "out", .{});
            break:path try d.realPathFileAlloc(io, ".", allocator);
        };

        var args: std.ArrayListUnmanaged(core.types.Symbol) = .empty;
        try args.appendSlice(allocator, &.{
            "init-config",
            "--target-scope", "test",
            "--from-scope", "foo",
            "--global",
        });

        const defaults_source = ".{}";

        const file_candidates: ConfigFileCandidates = .{ .current_dir = src_root_path, .home_dir = home_path};
        const options: FileResolveOptions = .{
            .command = "intalize", .scope = "foo", .category = .configs, .root = file_candidates, .default_scope = default_init_scope
        };

        try writeAssetFile(&tmp_dir, options, defaults_source);

        const expect_source_dir_path = try tmp_dir.dir.realPathFileAlloc(io, "src/configs/foo", allocator);
        const expect_output_dir_path = try tmp_dir.dir.realPathFileAlloc(io, "out/configs", allocator);

        var iter: TetsArgsIterator = .{.args = args.items};
        var scanner = ArgScanner(TetsArgsIterator).init(&iter);

        var base_bulder_res = try BaseSetting.Builder(TetsArgsIterator).fromArgs(allocator, &scanner, .discard);
        var builder = try Builder(TetsArgsIterator).fromArgs(allocator, &scanner, &base_bulder_res.builder, .configs, .discard);

        const setting: Setting = try builder.build(io, allocator, options);

        try std.testing.expectEqualStrings(expect_source_dir_path, setting.source_dir_path);
        try std.testing.expectEqualStrings(expect_output_dir_path, setting.output_dir_path);
        try std.testing.expectEqualStrings("test", setting.target_scope);
    }

    test "All explicit args for init-default" {
        const io = std.testing.io;
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        var tmp_dir = std.testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        const src_root_path = path: {
            const d = try tmp_dir.dir.createDirPathOpen(io, "src", .{});
            break:path try d.realPathFileAlloc(io, ".", allocator);
        };
        const home_path = path: {
            try tmp_dir.dir.createDirPath(io, "out/defaults");
            const d = try tmp_dir.dir.createDirPathOpen(io, "out", .{});
            break:path try d.realPathFileAlloc(io, ".", allocator);
        };

        var args: std.ArrayListUnmanaged(core.types.Symbol) = .empty;
        try args.appendSlice(allocator, &.{
            "init-default",
            "--target-scope", "test",
            "--from-scope", "foo",
            "--global",
        });

        const defaults_source = ".{}";

        const file_candidates: ConfigFileCandidates = .{ .current_dir = src_root_path, .home_dir = home_path};
        const options: FileResolveOptions = .{
            .command = "intalize", .scope = "foo", .category = .defaults, .root = file_candidates, .default_scope = default_init_scope
        };

        try writeAssetFile(&tmp_dir, options, defaults_source);

        const expect_source_dir_path = try tmp_dir.dir.realPathFileAlloc(io, "src/defaults/foo", allocator);
        const expect_output_dir_path = try tmp_dir.dir.realPathFileAlloc(io, "out/defaults", allocator);

        var iter: TetsArgsIterator = .{.args = args.items};
        var scanner = ArgScanner(TetsArgsIterator).init(&iter);

        var base_bulder_res = try BaseSetting.Builder(TetsArgsIterator).fromArgs(allocator, &scanner, .discard);
        var builder = try Builder(TetsArgsIterator).fromArgs(allocator, &scanner, &base_bulder_res.builder, .defaults, .discard);

        const setting: Setting = try builder.build(io, allocator, options);

        try std.testing.expectEqualStrings(expect_source_dir_path, setting.source_dir_path);
        try std.testing.expectEqualStrings(expect_output_dir_path, setting.output_dir_path);
        try std.testing.expectEqualStrings("test", setting.target_scope);
    }

    test "All default args" {
        const io = std.testing.io;
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        var tmp_dir = std.testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        const src_root_path = path: {
            const d = try tmp_dir.dir.createDirPathOpen(io, "src", .{});
            break:path try d.realPathFileAlloc(io, ".", allocator);
        };
        const home_path = path: {
            try tmp_dir.dir.createDirPath(io, "out/defaults");
            const d = try tmp_dir.dir.createDirPathOpen(io, "out", .{});
            break:path try d.realPathFileAlloc(io, ".", allocator);
        };
        
        const defaults_source = 
            \\.{
            \\  .target_scope = .{.values = .{ "bar" }},
            \\  .from_scope = .{.values = .{ "foo" }},
            \\  .global = .{.enabled = true},
            \\}
        ;

        const file_candidates: ConfigFileCandidates = .{ .current_dir = src_root_path, .home_dir = home_path};
        const options: FileResolveOptions = .{
            .command = "intalize", .scope = "foo", .category = .defaults, .root = file_candidates, .default_scope = default_init_scope
        };
        
        try writeAssetFile(&tmp_dir, options, defaults_source);

        const expect_source_dir_path = try tmp_dir.dir.realPathFileAlloc(io, "src/defaults/foo", allocator);
        const expect_output_dir_path = try tmp_dir.dir.realPathFileAlloc(io, "out/defaults", allocator);
        
        var iter: TetsArgsIterator = .{.args = &.{"init-config"}};
        var scanner = ArgScanner(TetsArgsIterator).init(&iter);

        var base_bulder_res = try BaseSetting.Builder(TetsArgsIterator).fromArgs(allocator, &scanner, .discard);
        var builder = try Builder(TetsArgsIterator).fromArgs(allocator, &scanner, &base_bulder_res.builder, .defaults, .discard);

        const setting: Setting = try builder.build(io, allocator, options);

        try std.testing.expectEqualStrings(expect_source_dir_path, setting.source_dir_path);
        try std.testing.expectEqualStrings(expect_output_dir_path, setting.output_dir_path);
        try std.testing.expectEqualStrings("bar", setting.target_scope);
    }

    test "Explict + default args" {
        const io = std.testing.io;
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        var tmp_dir = std.testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        const src_root_path = path: {
            const d = try tmp_dir.dir.createDirPathOpen(io, "src", .{});
            break:path try d.realPathFileAlloc(io, ".", allocator);
        };
        const home_path = path: {
            try tmp_dir.dir.createDirPath(io, "out/defaults");
            const d = try tmp_dir.dir.createDirPathOpen(io, "out", .{});
            break:path try d.realPathFileAlloc(io, ".", allocator);
        };

        var args: std.ArrayListUnmanaged(core.types.Symbol) = .empty;
        try args.appendSlice(allocator, &.{
            "init-default",
            "--target-scope", "bar",
            "--from-scope", "foo",
        });
        
        const defaults_source = 
            \\.{
            \\  .target_scope = .{.values = .{ "baz" }},
            \\  .from_scope = .{.values = .{ "quax" }},
            \\  .global = .{.enabled = false},
            \\}
        ;

        const file_candidates: ConfigFileCandidates = .{ .current_dir = src_root_path, .home_dir = home_path};
        const options: FileResolveOptions = .{
            .command = "intalize", .scope = "foo", .category = .defaults, .root = file_candidates, .default_scope = default_init_scope,
        };
        
        try writeAssetFile(&tmp_dir, options, defaults_source);

        const expect_source_dir_path = try tmp_dir.dir.realPathFileAlloc(io, "src/defaults/foo", allocator);
        const expect_output_dir_path = try tmp_dir.dir.realPathFileAlloc(io, "src/defaults", allocator);
        
        var iter: TetsArgsIterator = .{.args = args.items};
        var scanner = ArgScanner(TetsArgsIterator).init(&iter);

        var base_bulder_res = try BaseSetting.Builder(TetsArgsIterator).fromArgs(allocator, &scanner, .discard);
        var builder = try Builder(TetsArgsIterator).fromArgs(allocator, &scanner, &base_bulder_res.builder, .defaults, .discard);

        const setting: Setting = try builder.build(io, allocator, options);

        try std.testing.expectEqualStrings(expect_source_dir_path, setting.source_dir_path);
        try std.testing.expectEqualStrings(expect_output_dir_path, setting.output_dir_path);
        try std.testing.expectEqualStrings("bar", setting.target_scope);
    }
};