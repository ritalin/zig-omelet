const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const log = core.Logger.SystemDirect("runner"); // TODO
const Setting = @This();

arena: *std.heap.ArenaAllocator,
general: GeneralSetting,
command: CommandSetting,

pub const GeneralSetting = struct {
    ipc_root_dir_path: ?core.FilePath,
    runner_endpoints: core.Endpoints,
    stage_endpoints: core.Endpoints,
};

pub const CommandSetting = union(SubcommandArgId) {
    generate: CommandSettings.Generate,

    pub fn watchModeEnabled(self: CommandSetting) bool {
        return switch (self) {
            .generate => |c| c.watch,
        };
    }
};

pub const CommandSettings = struct {
    pub const Generate = struct {
        source_dir_paths: []core.FilePath,
        output_dir_path: core.FilePath,
        watch: bool,
    };
};

pub fn deinit(self: *Setting) void {
    self.arena.deinit();
    self.arena.child_allocator.destroy(self.arena);
}

pub fn loadFromArgs(allocator: std.mem.Allocator) !LoadResult(Setting) {
    var args_iter = try std.process.argsWithAllocator(allocator);
    defer args_iter.deinit();

    _ = args_iter.next();

    var diag: clap.Diagnostic = .{};
    var parser = clap.streaming.Clap(GeneralArgId, std.process.ArgIterator){
        .params = GeneralArgId.Decls,
        .iter = &args_iter,
        .diagnostic = &diag,
    };

    const arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);
    errdefer allocator.destroy(arena);
    errdefer arena.deinit();

    var required_general = std.enums.EnumSet(GeneralArgId).init(.{});

    const general_setting = 
        loadGeneralArgs(arena, @TypeOf(parser), &parser, GeneralArgId, &required_general) 
        catch |err| switch (err) {
            error.SettingLoadFailed, error.ShowHelp => return .{ .help = GeneralHelpSetting },
            else => return err,
        }
    ;
    const subcommand_result = 
        loadSubcommand(arena, @TypeOf(parser), &parser) 
        catch |err| switch (err) {
            error.ShowGeneralHelp => return .{ .help = GeneralHelpSetting },
            else => return err,
        }
    ;
    

    const setting = .{
        .arena = arena,
        .general = general_setting,
        .command = switch (subcommand_result) {
            .success => |setting| setting,
            .help => |setting| return .{ .help = setting },
        }
    };

    return .{ .success = setting };
}

fn loadGeneralArgs(arena: *std.heap.ArenaAllocator, comptime Parser: type, parser: *Parser, comptime Id: type, required_set: *std.enums.EnumSet(Id)) !GeneralSetting {
    var builder = GeneralArgBuilder.init();
    
    while (true) {
        const arg_ = parser.next() catch {
            if (required_set.count() > 0) {
                return error.SettingLoadFailed;
            }
            else {
                return try builder.build(arena.allocator());
            }
        };
        if (arg_ == null) {
            return error.SettingLoadFailed;
        }

        if (arg_) |arg| switch (arg.param.id) {
            .help => return error.ShowHelp,
            .req_rep_channel => builder.request_channel = arg.value,
            .pub_sub_channel => builder.subscribe_channel = arg.value,
        };
    }
}

fn loadSubcommand(arena: *std.heap.ArenaAllocator, comptime Parser: type, parser: *Parser) !LoadResult(CommandSetting) {
    if (parser.diagnostic == null) return error.ShowGeneralHelp;
    const id = std.meta.stringToEnum(SubcommandArgId, parser.diagnostic.?.arg) orelse return error.ShowGeneralHelp;

    return switch (id) {
        .generate => loadGenerateArgs(arena, std.process.ArgIterator, parser.iter),
    };
}

fn loadGenerateArgs(arena: *std.heap.ArenaAllocator, comptime Iterator: type, iter: *Iterator) !LoadResult(CommandSetting) {
    var diag: clap.Diagnostic = .{};
    var parser = clap.streaming.Clap(GenerateCommandArgId, std.process.ArgIterator){
        .params = GenerateCommandArgId.Decls,
        .iter = iter,
        .diagnostic = &diag,
    };

    var builder = CommandBuilders.Generate.init(arena.child_allocator);
    defer builder.deinit();

    while (true) {
        const arg_ = parser.next() catch {
            return .{
                .help = .{.tags = &.{ .cmd_generate, .cmd_general }}
            };
        };
        if (arg_ == null) {
            return .{
                .success = .{ .generate = try builder.build(arena.allocator()) }
            };
        }

        if (arg_) |arg| {
            switch (arg.param.id) {
                .source_dir => try builder.source_dir_paths.append(arg.value),
                .output_dir => builder.output_dir_path = arg.value,
                .watch => builder.watch = true,
            }
        }
    }
}

pub fn LoadResult(comptime Result: type) type {
    return union(enum) {
        success: Result,
        help: ArgHelpSetting,
    };
}

pub const ArgHelpSetting = struct {
    pub const Tag = enum {
        general,
        subcommand,
        cmd_general,
        cmd_generate,
    };

    tags: []const Tag,

    pub fn help(self: ArgHelpSetting, writer: anytype) !void {
        for (self.tags) |tag| {
            switch (tag) {
                .general => {
                    try showHelp(writer, GeneralArgId, GeneralArgId.Decls);
                },
                .cmd_general => {
                    try showHelp(writer, CommandGeneralArgId, CommandGeneralArgId.Decls);
                },
                .subcommand => {
                    try showSubcommandAll(writer, SubcommandArgId);
                },
                .cmd_generate => {
                    try showHelp(writer, GenerateCommandArgId, GenerateCommandArgId.Decls);
                },
            }
        }
    }
};

const GeneralHelpSetting: ArgHelpSetting = .{ .tags = &.{.subcommand, .general} };

fn showCategory(writer: anytype, comptime Id: type) !void {
    if (Id.options.category_name) |category| {
        try writer.print("{s}:\n", .{category});
        try writer.writeByte('\n');
    }
}

fn showHelp(writer: anytype, comptime Id: type, params: []const clap.Param(Id)) !void {
    try showCategory(writer, Id);
    try clap.help(writer, Id, params, .{});
}

fn measureLineWidth(comptime Id: type) !usize {
    var max_width: usize = 0;

    inline for (std.meta.fields(Id)) |id| {
        var cw = std.io.countingWriter(std.io.null_writer);
        var writer = cw.writer();

        try writer.writeAll(id.name);
        max_width = @max(max_width, cw.bytes_written);
    }

    return max_width;
}

fn showSubcommandAll(writer: anytype, comptime Id: type) !void {
    const clap_opt: clap.HelpOptions = .{};
    const max_width = try measureLineWidth(Id);

    try showCategory(writer, Id);

    inline for (std.meta.fields(Id)) |id| {
        // write command
        try writer.writeByteNTimes(' ', clap_opt.indent);
        try writer.writeAll(id.name);
        try writer.writeByteNTimes(' ', clap_opt.indent + max_width - id.name.len);
        // write description
        try writer.writeAll(@as(Id, @enumFromInt(id.value)).description());
        try writer.writeByte('\n');
    }
    
    try writer.writeByte('\n');
}

const ArgUsages = UsageMap.initComptime(.{
    // General
    .{@tagName(.req_rep_channel), .{.desc = "Comminicate Req/Rep endpoint for zmq", .value = "CHANNEL",}},
    .{@tagName(.pub_sub_channel), .{.desc = "Comminicate Pub/Sub endpoint for zmq", .value = "CHANNEL",}},
    .{@tagName(.help), .{.desc = "Print command-specific usage", .value = "",}},
    // Commands
    .{@tagName(.generate), .{.desc = "Generate query parameters", .value = "",}},
    // Command/Generate
    .{@tagName(.source_dir), .{.desc = "Source SQL folder(s) or file(s)", .value = "PATH", .required = true}},
    .{@tagName(.output_dir), .{.desc = "Output filder", .value = "PATH", .required = true}},
});

const GeneralArgId = enum {
    req_rep_channel,
    pub_sub_channel,
    help,

    const Decls: []const clap.Param(@This()) = &.{
        .{.id = .req_rep_channel, .names = .{.long = "reqrep-channel"}, .takes_value = .one},
        .{.id = .pub_sub_channel, .names = .{.long = "pubsub-channel"}, .takes_value = .one},
        .{.id = .help, .names = .{.long = "help", .short = 'h'}, .takes_value = .none},
        // .{.id = ., .names = .{}, .takes_value = },
    };
    pub usingnamespace ArgHelp(@This(), ArgUsages);
    const options: ArgHelpOption = .{.category_name = "General Options"};
};


const SubcommandArgId = enum {
    generate,

    pub usingnamespace ArgHelp(@This(), ArgUsages);
    pub const options: ArgHelpOption = .{.category_name = "Subcommands"};
};

const CommandGeneralArgId = enum {
    help,

    const Decls: []const clap.Param(@This()) = &.{
        .{.id = .help, .names = .{.long = "help", .short = 'h'}, .takes_value = .none},
    };
    pub usingnamespace ArgHelp(@This(), ArgUsages);
    pub const options: ArgHelpOption = .{.category_name = "General Options"};
};

const GenerateCommandArgId = enum {
    source_dir,
    output_dir,
    watch,

    const Decls: []const clap.Param(@This()) = &.{
        .{.id = .source_dir, .names = .{.long = "source-dir", .short = 'i'}, .takes_value = .many},
        .{.id = .output_dir, .names = .{.long = "output-dir", .short = 'o'}, .takes_value = .one},
        .{.id = .watch, .names = .{.long = "watch"}, .takes_value = .none},
        // .{.id = ., .names = .{}, .takes_value = },
    };
    pub usingnamespace ArgHelp(@This(), ArgUsages);
    const options: ArgHelpOption = .{.category_name = "generate"};
};

pub const UsageMap = std.StaticStringMap(struct { desc: []const u8, value: []const u8, required: bool = false });

pub const ArgHelpOption = struct {
    category_name: ?[]const u8,
};

pub fn ArgHelp(comptime Id: type, comptime Usages: UsageMap) type {
    return struct {    
        pub fn description(self: Id) []const u8 {
            const usage = Usages.get(@tagName(self)) orelse return "";
            return usage.desc;
        }
        pub fn value(self: Id) []const u8 {
            const usage = Usages.get(@tagName(self)) orelse return "VALUE";
            return usage.value;
        }
    };
}

pub const GeneralArgBuilder = struct {
    request_channel: ?core.Symbol,
    subscribe_channel: ?core.Symbol,
    standalone: bool,

    pub fn init() GeneralArgBuilder {
        return .{
            .request_channel = null,
            .subscribe_channel = null,
            .standalone = false,
        };
    }

    pub fn deinit(self: *GeneralArgBuilder) void {
        _ = self;
    }

    pub fn build (self: GeneralArgBuilder, allocator: std.mem.Allocator) !GeneralSetting {
        const default_channel_folder = core.CHANNEL_ROOT;
        const default_channel_root = std.fmt.comptimePrint("ipc://{s}", .{default_channel_folder});
        
        const req_rep_channels = channel: {
            if (self.request_channel) |channel| {
                break :channel .{
                    try core.resolveConnectPort(allocator, channel), // REQ
                    try core.resolveBindPort(allocator, channel), // REP
                };
            }
            else {
                break :channel .{
                    try core.resolveIPCConnectPort(allocator, default_channel_root, core.REQ_PORT), // REQ
                    try core.resolveIPCBindPort(allocator, default_channel_root, core.REQ_PORT), // REP
                };
            }
        };
        const pub_sub_channels = channel: {
            if (self.subscribe_channel) |channel| {
                break :channel .{
                    try core.resolveBindPort(allocator, channel), // PUB
                    try core.resolveConnectPort(allocator, channel), // SUB
                };
            }
            else {
                break :channel .{
                    try core.resolveIPCBindPort(allocator, default_channel_root, core.PUBSUB_PORT), // PUB
                    try core.resolveIPCConnectPort(allocator, default_channel_root, core.PUBSUB_PORT), // SUB
                };
            }
        };

        return .{
            .ipc_root_dir_path = try allocator.dupe(u8, default_channel_folder),
            .runner_endpoints = .{
                .req_rep = req_rep_channels[1],
                .pub_sub = pub_sub_channels[0],
            },
            .stage_endpoints = .{
                .req_rep = req_rep_channels[0],
                .pub_sub = pub_sub_channels[1],
            },
        };
    }
};

const CommandBuilders = struct {
    const Generate = struct {
        source_dir_paths: std.ArrayList(?core.FilePath),
        output_dir_path: ?core.FilePath = null,
        watch: bool = false,

        pub fn init(allocator: std.mem.Allocator) Generate {
            return .{
                .source_dir_paths = std.ArrayList(?core.FilePath).init(allocator),
            };
        }

        pub fn deinit(self: *Generate) void {
            self.source_dir_paths.deinit();
        }

        pub fn build(self: Generate, allocator: std.mem.Allocator) !CommandSettings.Generate {
            var base_dir = std.fs.cwd();

            var sources = std.ArrayList(core.FilePath).init(allocator);
            defer sources.deinit();
            
            if (self.source_dir_paths.items.len == 0) {
                log.warn("Need to specify SQL source folder at least one", .{});
                return error.SettingLoadFailed;
            }
            else {
                for (self.source_dir_paths.items) |path_| {
                    if (path_) |path| {
                        _ = base_dir.statFile(path) catch {
                            log.warn("Cannot access source folder: {s}", .{path});
                            return error.SettingLoadFailed;
                        };

                        try sources.append(try base_dir.realpathAlloc(allocator, path));
                    }
                }
            }

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
                .source_dir_paths = try sources.toOwnedSlice(),
                .output_dir_path = output_dir_path,
                .watch = self.watch,
            };
        }
    };
};
