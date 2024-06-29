const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const Setting = @This();

arena: *std.heap.ArenaAllocator,
endpoints: core.Endpoints,
sources: []const SourceDir,
watch: bool,
standalone: bool,

pub const SourceDir = struct {
    dir_path: core.FilePath, 
    prefix: core.FilePath,
};

pub fn loadFromArgs(allocator: std.mem.Allocator) !Setting {
    var arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);
    const managed_allocator = arena.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();
    
    var builder = try loadInternal(managed_allocator, &args);
    defer builder.deinit();

    return builder.build(arena);
}

pub fn deinit(self: *Setting) void {
    self.arena.deinit();
    self.arena.child_allocator.destroy(self.arena);
    self.* = undefined;
}

pub fn showUsage(writer: anytype) !void {
    return clap.help(writer, ArgId, ParamDecls, .{});
}

const ArgUsages = std.StaticStringMap(struct { desc: []const u8, value: []const u8 }).initComptime(.{
    .{@tagName(.request_channel), .{.desc = "Comminicate Req/Rep endpoint for zmq", .value = "CHANNEL"}},
    .{@tagName(.subscribe_channel), .{.desc = "Comminicate Pub/Sub endpoint for zmq", .value = "CHANNEL"}},
    .{@tagName(.source_dir), .{.desc = "Source directores or files", .value = "PATH"}},
    .{@tagName(.watch), .{.desc = "Enter to watch-mode", .value = ""}},
});

const ArgId = enum {
    request_channel,
    subscribe_channel,
    source_dir,
    watch,
    standalone,

    pub fn description(self: ArgId) []const u8 {
        const usage = ArgUsages.get(@tagName(self)) orelse return "";
        return usage.desc;
    }
    pub fn value(self: ArgId) []const u8 {
        const usage = ArgUsages.get(@tagName(self)) orelse return "VALUE";
        return usage.value;
    }
};

const ParamDecls: []const clap.Param(ArgId) = &.{
    .{.id = .request_channel, .names = .{.long = "request-channel"}, .takes_value = .one},
    .{.id = .subscribe_channel, .names = .{.long = "subscribe-channel"}, .takes_value = .one},
    .{.id = .source_dir, .names = .{.long = "source-dir"}, .takes_value = .one},
    .{.id = .watch, .names = .{.long = "watch"}, .takes_value = .none},
    .{.id = .standalone, .names = .{.long = "standalone"}, .takes_value = .none},
    // .{.id = ., .names = , .takes_value = },
};

fn loadInternal(allocator: std.mem.Allocator, args_iter: *std.process.ArgIterator) !Builder {
    _ = args_iter.next();

    var parser = clap.streaming.Clap(ArgId, std.process.ArgIterator){
        .params = ParamDecls,
        .iter = args_iter,
    };

    var builder = Builder.init(allocator);

    while (try parser.next()) |arg| {
        switch (arg.param.id) {
            .request_channel => {
                if (arg.value) |v| builder.request_channel = v;
            },
            .subscribe_channel => {
                if (arg.value) |v| builder.subscribe_channel = v;
            },
            .source_dir => {
                if (arg.value) |v| try builder.addSourceDir(v);
            },
            .watch => {
                builder.watch = true;
            },
            .standalone => {
                builder.standalone = true;
            }
        }
    }

    return builder;
}

const Builder = struct {
    request_channel: ?core.Symbol,
    subscribe_channel: ?core.Symbol,
    sources: std.ArrayList(core.FilePath),
    watch: bool,
    standalone: bool,

    pub fn init(allocator: std.mem.Allocator) Builder {
        return .{
            .request_channel = null,
            .subscribe_channel = null,
            .sources = std.ArrayList(core.FilePath).init(allocator),
            .watch = false,
            .standalone = false,
        };
    }

    pub fn deinit(self: *Builder) void {
        self.sources.deinit();
    }

    pub fn addSourceDir(self: *Builder, path: core.FilePath) !void {
        return self.sources.append(path);
    }

    pub fn build (self: Builder, arena: *std.heap.ArenaAllocator) !Setting {
        const allocator = arena.allocator();

        if (self.request_channel == null) {
            std.debug.print("Need to specify a `request-channel` arg.\n\n", .{});
            return error.SettingLoadFailed;
        }
        if (self.subscribe_channel == null) {
            std.debug.print("Need to specify a `subscribe-channel` arg.\n\n", .{});
            return error.SettingLoadFailed;
        }
        if (self.sources.items.len == 0) {
            std.debug.print("Need to specify at least one `source-dir` arg.\n\n", .{});
            return error.SettingLoadFailed;
        }

        var sources = std.ArrayList(SourceDir).init(allocator);
        defer sources.deinit();

        for (self.sources.items) |path| {
            const path_abs = std.fs.cwd().realpathAlloc(allocator, path) catch {
                std.debug.print("can not resolve `source-dir` arg ({s}).\n\n", .{path});
                return error.SettingLoadFailed;
            };

            try sources.append(.{
                .dir_path = path_abs,
                .prefix = try allocator.dupe(u8, try resolvePrefix(path_abs)),
            });
        }

        return .{
            .arena = arena,
            .endpoints = .{
                .req_rep = try allocator.dupe(u8, self.request_channel.?),
                .pub_sub = try allocator.dupe(u8, self.subscribe_channel.?),
            },
            .sources = try sources.toOwnedSlice(),
            .watch = self.watch,
            .standalone = self.standalone,
        };
    }
};

fn resolvePrefix(path: core.FilePath) !core.FilePath {
    const state = try std.fs.cwd().statFile(path);
    if (state.kind == .file) {
        if (std.fs.path.dirname(path)) |prefix| return prefix;
    }
    
    return path;
}