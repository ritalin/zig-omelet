const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const Symbol = core.types.Symbol;
const FilePath = core.types.FilePath;

const ArgScanner = core.settings.types.ArgScanner;
const ArgParserPair = core.settings.types.ArgParserPair;

const GuestBaseConfigArgId = core.configs.guests.GuestBaseConfig.ArgId(.{});
const GuestInitializeArgId = core.configs.guests.GuestInitialize.ArgId(.{});

const ArgHelp = @import("./help/ArgHelp.zig");
const Setting = @This();

log_level: core.events.LogLevel,
log_style: core.Logger.LogStyle,
no_color: bool,
endpoints: core.types.Endpoints,
source_dir_path: core.types.FilePath,
output_dir_path: core.types.FilePath,
target_scope: core.types.Symbol,

pub fn loadFromArgs(allocator: std.mem.Allocator, args: std.process.Args) core.settings.types.LoadResult(Setting, *const ArgHelp.Config) {
    var args_iter = try args.iterateAllocator(allocator);
    defer args_iter.deinit();

    _ = args_iter.next();
    
    var scanner: ArgScanner(std.process.Args.Iterator) = .init(&args_iter);
    var builder = Builder(std.process.Args.Iterator).fromArgs(&scanner, .stderr) catch return .{ .help = &ArgHelp.toplevel };
    const setting = builder.build(allocator) catch return .{ .help = &ArgHelp.toplevel };

    return .{ .success = setting };
}

pub fn deinit(self: *Setting, allocator: std.mem.Allocator) void {
    allocator.free(self.endpoints.req_rep);
    allocator.free(self.endpoints.pub_sub);
    allocator.free(self.endpoints.push_pull);
    allocator.free(self.source_dir_path);
    allocator.free(self.output_dir_path);
    allocator.free(self.target_scope);
}

// fn loadInternal(allocator: std.mem.Allocator, args_iter: *std.process.ArgIterator) !Builder {
//     _ = args_iter.next();

//     var diag = clap.Diagnostic{};
//     var parser = clap.streaming.Clap(ArgId, std.process.ArgIterator){
//         .params = ArgId.Decls,
//         .iter = args_iter,
//         .diagnostic = &diag,
//     };

//     var builder = Builder.init(allocator);

//     while (true) {
//         const arg_ = parser.next() catch |err| {
//             try diag.report(std.io.getStdErr().writer(), err);
//             return err;
//         };
//         const arg = arg_ orelse break;

//         switch (arg.param.id) {
//             .request_channel => builder.request_channel = arg.value,
//             .subscribe_channel => builder.subscribe_channel = arg.value,
//             .log_level => builder.log_level = arg.value,
//             .source_dir => builder.source_dir_path = arg.value,
//             .output_dir => builder.output_dir_path = arg.value,
//             .category => builder.category = arg.value,
//             .command => builder.command = arg.value,
//             .scope => {
//                 if (arg.value) |v| try builder.scope_set.append(v);
//             },
//             .from_scope => builder.from_scope = arg.value,
//             .standalone => builder.standalone = true,
//         }
//     }

//     return builder;
// }

fn Builder(comptime ArgIterator: type) type {
    return struct {
        builder_log_style: core.Logger.LogStyle,
        reqrep_channel: ?Symbol = null,
        pubsub_channel: ?Symbol = null,
        pushpull_channel: ?Symbol = null,
        log_level: ?Symbol = null,
        log_style: ?Symbol = null,
        no_color: bool = false,
        source_dir_path: ?core.types.FilePath = null,
        output_dir_path: ?core.types.FilePath = null,
        target_scope: ?core.types.Symbol = null,

        pub fn fromArgs(scanner: *ArgScanner(ArgIterator), log_style: core.Logger.LogStyle) !Builder(ArgIterator) {
            var diag: clap.Diagnostic = .{};
            var parsers = ArgParserPair(GuestBaseConfigArgId, GuestInitializeArgId, ArgIterator).init(scanner, &diag);

            var builder: Builder(ArgIterator) = .{ .builder_log_style = log_style };

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
                        builder.handleBaseArg(arg);
                    },
                    .extra => |arg| {
                        builder.handleExtraArg(arg);
                    }
                }
            }

            return builder;
        }

        fn handleBaseArg(self: *Builder(ArgIterator), arg: clap.streaming.Arg(GuestBaseConfigArgId)) void {
            switch (arg.param.id) {
                .req_rep => self.reqrep_channel = arg.value,
                .pub_sub => self.pubsub_channel = arg.value,
                .push_pull => self.pushpull_channel = arg.value,
                .log_level => self.log_level = arg.value,
                .log_style => self.log_style = arg.value,
                .no_color => self.no_color = true,
            }
        }

        fn handleExtraArg(self: *Builder(ArgIterator), arg: clap.streaming.Arg(GuestInitializeArgId)) void {
            switch (arg.param.id) {
                .source_dir_path => self.source_dir_path = arg.value,
                .output_dir_path => self.output_dir_path = arg.value,
                .target_scope => self.target_scope = arg.value,
            }
        }

        pub fn build(self: *Builder(ArgIterator), allocator: std.mem.Allocator) !Setting {
            var has_err = false;

            if (self.reqrep_channel == null) {
                has_err = true;
                if (self.builder_log_style == .stderr) {
                    std.log.warn("Need to specify a `request-channel` arg.", .{});
                }
            }
            if (self.pubsub_channel == null) {
                has_err = true;
                if (self.builder_log_style == .stderr) {
                    std.log.warn("Need to specify a `subscribe-channel` arg.", .{});
                }
            }
            if (self.pushpull_channel == null) {
                has_err = true;
                if (self.builder_log_style == .stderr) {
                    std.log.warn("Need to specify a `push-channel` arg.", .{});
                }
            }

            const log_level = core.events.LogLevel.resolveLogLevel(self.log_level) orelse log_level: {
                has_err = true;
                if (self.builder_log_style == .stderr) {
                    std.log.warn("Unresolved log level: {?s}", .{self.log_level});
                }
                break:log_level null;
            };
            const log_style = core.settings.supports.resolveGuestLogStyle(self.log_style) orelse log_style: {
                has_err = true;
                if (self.builder_log_style == .stderr) {
                    std.log.warn("Unresolved log style: {?s}", .{self.log_style});
                }                
                break:log_style null;
            };

            if (self.source_dir_path == null) {
                has_err = true;
                if (self.builder_log_style == .stderr) {
                    std.log.warn("Need to specify a `source-dir` arg.", .{});
                }                
            }
            if (self.output_dir_path == null) {
                has_err = true;
                if (self.builder_log_style == .stderr) {
                    std.log.warn("Need to specify a `output-dir` arg.", .{});
                }
            }
            if (self.target_scope == null) {
                has_err = true;
                if (self.builder_log_style == .stderr) {
                    std.log.warn("Need to specify a `target-scope` arg.", .{});
                }
            }

            if (has_err) {
                return error.SettingLoadFailed;
            }

            return .{
                .endpoints = .{
                    .req_rep = try allocator.dupe(u8, self.reqrep_channel.?),
                    .pub_sub = try allocator.dupe(u8, self.pubsub_channel.?),
                    .push_pull = try allocator.dupe(u8, self.pushpull_channel.?),
                },
                .log_level = log_level.?,
                .log_style = log_style.?,
                .no_color = self.no_color,
                .source_dir_path = try allocator.dupe(u8, self.source_dir_path.?),
                .output_dir_path = try allocator.dupe(u8, self.output_dir_path.?),
                .target_scope = try allocator.dupe(u8, self.target_scope.?),
            };
        }
    };
}