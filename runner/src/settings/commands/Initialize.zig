const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const log = core.Logger.SystemDirect(@import("build_options").app_context);
const help = @import("../help.zig");

const FilePath = core.FilePath;
const Symbol = core.Symbol;

source_dir_path: FilePath,
output_dir_path: FilePath,
category: core.ConfigCategory,
command: core.SubcommandArgId,

const Self = @This();

pub fn ArgId(comptime descriptions: core.settings.DescriptionMap) type {
    return enum {
        subcommand,
        global,

        pub const Decls: []const clap.Param(@This()) = &.{
            .{.id = .subcommand, .names = .{.long = "command", .short = 'c'}, .takes_value = .one},
            .{.id = .global, .names = .{.long = "global", .short = 'g'}, .takes_value = .none},
        };
        pub usingnamespace core.settings.ArgHelp(@This(), descriptions);
        pub const options: core.settings.ArgHelpOption = .{.category_name = "init-default"};
    };
}

pub const strategy = core.configs.StageStrategy.init(.{
    .stage_generate = .one,
});

pub const UnsupportedCommands = std.enums.EnumFieldStruct(core.SubcommandArgId, bool, false);

pub const Builder = struct {
    allocator: std.mem.Allocator,
    category: core.ConfigCategory,
    subcommand: ?Symbol = null,
    global: bool = false,
    unsupported_set: std.enums.EnumSet(core.SubcommandArgId),

    pub fn init(allocator: std.mem.Allocator, category: core.ConfigCategory, unsupported: UnsupportedCommands) Builder {
        return .{
            .allocator = allocator,
            .category = category,
            .unsupported_set = std.enums.EnumSet(core.SubcommandArgId).init(unsupported),
        };
    }

    pub fn loadArgs(self: *Builder, comptime Iterator: type, iter: *Iterator) !Self {
        var diag: clap.Diagnostic = .{};
        var parser = clap.streaming.Clap(ArgId(.{}), std.process.ArgIterator){
            .params = ArgId(.{}).Decls,
            .iter = iter,
            .diagnostic = &diag,
        };

        while (true) {
            const arg_ = parser.next() catch |err| {
                try diag.report(std.io.getStdErr().writer(), err);
                return error.ShowCommandHelp;
            };
            if (arg_ == null) {
                return try self.build();
            }

            if (arg_) |arg| {
                switch (arg.param.id) {
                    .subcommand => self.subcommand = arg.value,
                    .global => self.global = true,
                }
            }
        }
    }

    const suffix_map = std.StaticStringMap(FilePath).initComptime(.{
        .{@tagName(.defaults), "default-templates"},
        .{@tagName(.configs), "configs"},
    });

    fn build(self: Builder) !Self {
        if (self.subcommand == null) {
            log.warn("Need to specify subcommand name", .{});
            return error.SettingLoadFailed;
        }

        const exe_dir_path = try std.fs.selfExeDirPathAlloc(self.allocator);
        defer self.allocator.free(exe_dir_path);
        const source_dir_path = try std.fs.path.join(self.allocator, &.{std.fs.path.dirname(exe_dir_path).?, suffix_map.get(@tagName(self.category)).?});

        const output_dir_path = try resolveOutputDir(self.allocator, self.category, self.global);

        const subcommand = std.meta.stringToEnum(core.SubcommandArgId, self.subcommand.?) orelse {
            log.warn("Unresolved subcommand name: `{?s}`", .{self.subcommand});
            return error.SettingLoadFailed;
        };
        if (self.unsupported_set.contains(subcommand)) {
            log.warn("Unsupported subcommand name: `{?s}`", .{self.subcommand});
            return error.SettingLoadFailed;
        }

        return .{
            .source_dir_path = source_dir_path,
            .output_dir_path = output_dir_path,
            .category = self.category,
            .command = subcommand,
        };
    }

    fn resolveOutputDir(allocator: std.mem.Allocator, id: core.ConfigCategory, global: bool) !FilePath {
        const known_folders = @import("known_folders");

        const output_root_path = path: {
            if (global) {
                break:path try known_folders.getPath(allocator, .home) orelse {
                    log.warn("Home directory is not exist", .{});
                    return error.SettingLoadFailed;
                };

            }
            else {
                break:path try std.fs.cwd().realpathAlloc(allocator, ".");
            }
        };
        defer allocator.free(output_root_path);

        return std.fs.path.join(allocator, &.{output_root_path, ".omelet", @tagName(id)});
    }
};