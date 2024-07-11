const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const log = core.Logger.SystemDirect(@import("build_options").app_context);
const help = @import("../help.zig");

source_dir_path: []core.FilePath,
schema_dir_path: ?core.FilePath,
output_dir_path: core.FilePath,
watch: bool,

const Self = @This();

pub fn loadArgs(arena: *std.heap.ArenaAllocator, comptime Iterator: type, iter: *Iterator) !Self {
    var diag: clap.Diagnostic = .{};
    var parser = clap.streaming.Clap(ArgId(.{}), std.process.ArgIterator){
        .params = ArgId(.{}).Decls,
        .iter = iter,
        .diagnostic = &diag,
    };

    var builder = Builder.init(arena.child_allocator);
    defer builder.deinit();

    while (true) {
        const arg_ = parser.next() catch {
            return error.ShowCommandHelp;
        };
        if (arg_ == null) {
            return try builder.build(arena.allocator());
        }

        if (arg_) |arg| {
            switch (arg.param.id) {
                .source_dir_path => try builder.source_dir_path.append(arg.value),
                .schema_dir_path => builder.schema_dir_path = arg.value,
                .output_dir_path => builder.output_dir_path = arg.value,
                .watch => builder.watch = true,
            }
        }
    }
}

pub fn ArgId(comptime descriptions: core.settings.DescriptionMap) type {
    return enum {
        source_dir_path,
        schema_dir_path, 
        output_dir_path,
        watch,

        pub const Decls: []const clap.Param(@This()) = &.{
            .{.id = .source_dir_path, .names = .{.long = "source-dir", .short = 'i'}, .takes_value = .many},
            .{.id = .schema_dir_path, .names = .{.long = "schema-dir"}, .takes_value = .one},
            .{.id = .output_dir_path, .names = .{.long = "output-dir", .short = 'o'}, .takes_value = .one},
            .{.id = .watch, .names = .{.long = "watch"}, .takes_value = .none},
            // .{.id = ., .names = .{}, .takes_value = },
        };
        pub usingnamespace core.settings.ArgHelp(@This(), descriptions);
        pub const options: core.settings.ArgHelpOption = .{.category_name = "generate"};
    };
}

const Builder = struct {
    source_dir_path: std.ArrayList(?core.FilePath),
    schema_dir_path: ?core.FilePath = null,
    output_dir_path: ?core.FilePath = null,
    watch: bool = false,

    pub fn init(allocator: std.mem.Allocator) Builder {
        return .{
            .source_dir_path = std.ArrayList(?core.FilePath).init(allocator),
        };
    }

    pub fn deinit(self: *Builder) void {
        self.source_dir_path.deinit();
    }

    pub fn build(self: Builder, allocator: std.mem.Allocator) !Self {
        var base_dir = std.fs.cwd();

        var sources = std.ArrayList(core.FilePath).init(allocator);
        defer sources.deinit();
        
        if (self.source_dir_path.items.len == 0) {
            log.warn("Need to specify SQL source folder at least one", .{});
            return error.SettingLoadFailed;
        }
        else {
            for (self.source_dir_path.items) |path_| {
                if (path_) |path| {
                    _ = base_dir.statFile(path) catch {
                        log.warn("Cannot access source folder: {s}", .{path});
                        return error.SettingLoadFailed;
                    };

                    try sources.append(try base_dir.realpathAlloc(allocator, path));
                }
            }
        }

        const schema_dir_path: ?core.FilePath = path: {
            if (self.schema_dir_path) |path| {
                break:path try base_dir.realpathAlloc(allocator, path);
            }
            else {
                break:path null;
            }
        };

        const output_dir_path = path: {
            if (self.output_dir_path == null) {
                log.warn("Need to specify output folder", .{});
                return error.SettingLoadFailed;
            }
            else {
                try base_dir.makePath(self.output_dir_path.?);
                break :path try base_dir.realpathAlloc(allocator, self.output_dir_path.?);
            }
        };

        return .{
            .source_dir_path = try sources.toOwnedSlice(),
            .schema_dir_path = schema_dir_path,
            .output_dir_path = output_dir_path,
            .watch = self.watch,
        };
    }
};
