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
scope_set: []const Symbol,
from_scope: ?Symbol,

const Self = @This();

pub fn InitArgId(comptime descriptions: core.settings.DescriptionMap) type {
    return enum {
        subcommand,
        new_scope,
        from_scope,
        global,

        pub const Decls: []const clap.Param(@This()) = &.{
            .{.id = .subcommand, .names = .{.long = "command", .short = 'c'}, .takes_value = .one},
            .{.id = .new_scope, .names = .{.long = "scope", .short = 's'}, .takes_value = .many},
            .{.id = .from_scope, .names = .{.long = "from-scope", .short = 's'}, .takes_value = .one},
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
    scope_set: std.ArrayList(Symbol),
    from_scope: ?Symbol = null,
    from_scope_required: bool,
    global: bool = false,
    unsupported_set: std.enums.EnumSet(core.SubcommandArgId),

    pub fn init(allocator: std.mem.Allocator, category: core.ConfigCategory, from_scope_required: bool, unsupported: UnsupportedCommands) Builder {
        return .{
            .allocator = allocator,
            .category = category,
            .scope_set = std.ArrayList(Symbol).init(allocator),
            .from_scope_required = from_scope_required,
            .unsupported_set = std.enums.EnumSet(core.SubcommandArgId).init(unsupported),
        };
    }

    pub fn loadArgs(self: *Builder, comptime Iterator: type, iter: *Iterator) !Self {
        var diag: clap.Diagnostic = .{};
        var parser = clap.streaming.Clap(InitArgId(.{}), std.process.ArgIterator){
            .params = InitArgId(.{}).Decls,
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
                    .new_scope => if (arg.value) |v| try self.scope_set.append(v),
                    .from_scope => self.from_scope = arg.value,
                }
            }
        }
    }

    fn build(self: Builder) !Self {
        if (self.subcommand == null) {
            log.warn("Need to specify subcommand name", .{});
            return error.SettingLoadFailed;
        }

        const subcommand = std.meta.stringToEnum(core.SubcommandArgId, self.subcommand.?) orelse {
            log.warn("Unresolved subcommand name: `{?s}`", .{self.subcommand});
            return error.SettingLoadFailed;
        };
        if (self.unsupported_set.contains(subcommand)) {
            log.warn("Unsupported subcommand name: `{?s}`", .{self.subcommand});
            return error.SettingLoadFailed;
        }

        var scopes = std.ArrayList(Symbol).init(self.allocator);
        defer scopes.deinit();
        for (self.scope_set.items) |s| {
            try scopes.append(try self.allocator.dupe(u8, s));
        }
        
        if ((! self.from_scope_required) and (scopes.items.len == 0)) {
            try scopes.append(try self.allocator.dupe(u8, "default"));
        }

        const from_scope = scope: {
            if (self.from_scope) |scope| {
                break:scope try self.allocator.dupe(u8, scope);
            }
            else {
                break:scope null;
            }
        };
        
        const output_dir_path = try resolveOutputDir(self.allocator, self.global);

        const source_dir_path = path: {
            if (from_scope) |scope| {
                var dir = try std.fs.cwd().openDir(output_dir_path, .{});
                defer dir.close();

                var scope_dir = dir.openDir(scope, .{}) catch {
                    log.warn("Failed to access source scope: `{?s}`", .{scope});
                    return error.SettingLoadFailed;
                };
                defer scope_dir.close();

                break:path try self.allocator.dupe(u8, output_dir_path);
            }
            else {
                const exe_dir_path = try std.fs.selfExeDirPathAlloc(self.allocator);
                defer self.allocator.free(exe_dir_path);
                break:path try std.fs.path.join(self.allocator, &.{std.fs.path.dirname(exe_dir_path).?});
            }
        };

        return .{
            .source_dir_path = source_dir_path,
            .output_dir_path = output_dir_path,
            .category = self.category,
            .command = subcommand,
            .scope_set = try scopes.toOwnedSlice(),
            .from_scope = from_scope,
        };
    }

    fn resolveOutputDir(allocator: std.mem.Allocator, global: bool) !FilePath {
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

        return std.fs.path.join(allocator, &.{output_root_path, ".omelet"});
    }
};