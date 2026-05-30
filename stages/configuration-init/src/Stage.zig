const std = @import("std");
const core = @import("core");
const app_context = @import("build_options").app_context;

const events = core.events;

const EventDispatcher = core.sockets.EventDispatcher;
const Logger = core.Logger.withAppContext(app_context);
const ReceiveEntry = core.sockets.ReceiveEntry;

const BootPhaseState = core.guest_phases.BootPhaseState(GuestStage, app_context);

const Setting = @import("./Setting.zig");
// const PathMatcher = @import("./PathMatcher.zig").PathMatcher(u21);

const GuestStage = @This();

allocator: std.mem.Allocator,
setting: *const Setting,
connection: *GuestStage.Connection,
dispatcher: EventDispatcher.Sized(1),
state: State,

// TODO:
// const Connection = core.sockets.Connection.Client(app_context, GenerateWorker);
pub const Connection = core.sockets.Connection.Client(app_context);

pub fn create(allocator: std.mem.Allocator, connection: *Connection, setting: *const Setting) !GuestStage {
    errdefer connection.deinit();

    try connection.subscribe(&.{
        .probe_launching,
        .request_topic,
        .ready_watch_path,
        .quit_all,
    });
    try connection.connect();

    const options: EventDispatcher.Options = .{ 
        .log_style = setting.log_style,
        .no_color = setting.no_color, 
    };
    const dispatcher = try connection.configureDispatcher(1, options);

    return .{
        .allocator = allocator,
        .setting = setting,
        .connection = connection,
        .dispatcher = dispatcher,
        .state = .{ .booting = BootPhaseState.init },
    };
}

pub fn deinit(self: *GuestStage) void {
    self.state.deinit();
    self.dispatcher.deinit();
}

pub fn run(self: *GuestStage) !void {
    self.dispatcher.run(app_context, GuestStage.onDispatch) catch |err| {
        // TODO: fatal error log
        // try self.connection.dispatcher.postFatal(@errorReturnTrace());
        return err;
    };
}

pub fn log(self: *GuestStage, comptime level: events.LogLevel, comptime fmt: []const u8, args: anytype) !void {
    if (! comptime std.log.logEnabled(level.toStdLevel(), .default)) return;
    try self.dispatcher.log(level, app_context, fmt, args);
}

pub fn transitPhase(self: *GuestStage, phase: EventDispatcher.Phase) !void {
    switch (phase) {
        .request, .ready => try self.doReadyPhase(),
        else => unreachable,
    }
}

pub fn defaultHandler(self: *GuestStage, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
    switch (entry.event) {
        .quit_all => {
            self.dispatcher.phase = .quitting;
        },
        else => {
            dirty.* = .unhandled;
        }
    }
}

fn doReadyPhase(self: *GuestStage) !void {
    self.state.deinit();
    self.dispatcher.phase = .ready;    
}

fn onDispatch(dispatcher: *EventDispatcher.Sized(1), entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) anyerror!void {
    const self: *GuestStage = @alignCast(@fieldParentPtr("dispatcher", dispatcher));

    switch (self.state) {
        .booting => |state| {
            try state.handle(self, entry, dirty);
        },
        // TODO:
        // .ready => |state| try state.handle(self, entry, dirty),
        else => {
            // TODO:
            // Invalid phase
            unreachable;
        }
    }
}

const State = union(EventDispatcher.Phase) {
    booting: BootPhaseState,
    request: void,
    ready: void,
    terminating: void,
    quitting: void,

    const deinit = deinitState;
};

fn deinitState(self: *State) void {
    switch (self.*) {
        .booting => |*state| state.deinit(),
        // TODO:
        // .ready => |*state| state.deinit(),
        else => unreachable,
    }
}
    // TODO:
    // try self.connection.dispatcher.state.ready();
    // .request_topic => {
    //     topics: {
    //         const topic = try core.Event.Payload.Topic.init(
    //             self.allocator, 
    //             .source,
    //             &.{},
    //             true,
    //         );
            
    //         try self.connection.dispatcher.post(.{.topic = topic});
    //         break :topics;
    //     }
    // },
    // .ready_watch_path => {
    //     try self.handleGenerate(setting);
    //     try self.connection.dispatcher.post(.finish_generate);
    // },


// fn handleGenerate(self: *GuestStage, setting: Setting) !void {
//     const source_dir_path = path: {
//         if (setting.from_scope) |scope| {
//             break:path try std.fs.path.join(self.allocator, &.{setting.output_dir_path, scope, setting.category.destPath()});
//         }
//         else {
//             break:path try std.fs.path.join(self.allocator, &.{setting.source_dir_path, setting.category.templateDir()});
//         }
//     };
//     defer self.allocator.free(source_dir_path);

//     var source_dir = std.fs.cwd().openDir(source_dir_path, .{}) 
//     catch {
//         try self.logger.log(.err, "Failed to access template root dir: `{s}`", .{setting.source_dir_path});
//         return;
//     };
//     defer source_dir.close();

//     const config_file_name = try std.fmt.allocPrint(self.allocator, "{s}.zon", .{@tagName(setting.command)});
//     defer self.allocator.free(config_file_name);

//     var file = source_dir.openFile(config_file_name, .{})
//     catch {
//         const full_path = try std.fs.path.join(self.allocator, &.{source_dir_path, config_file_name});
//         defer self.allocator.free(full_path);

//         try self.logger.log(.warn, "Failed to access template file: `{s}`", .{config_file_name});
//         return;
//     };
//     defer file.close();

//     for (setting.scope_set) |scope| {
//         try self.handleGenerateInternal(setting, source_dir, scope, config_file_name);
//     }
// }

// fn handleGenerateInternal(self: *GuestStage, setting: Setting, source_dir: std.fs.Dir, scope: core.Symbol, config_file_name: core.Symbol) !void {
//     const out_dir_path = try std.fs.path.join(self.allocator, &.{
//         setting.output_dir_path, scope, setting.category.destPath()
//     });
//     defer self.allocator.free(out_dir_path);

//     var out_dir = std.fs.cwd().makeOpenPath(out_dir_path, .{}) 
//     catch {
//         try self.logger.log(.err, "Failed to access destination dir: `{s}`", .{setting.output_dir_path});
//         return;
//     };
//     defer out_dir.close();

//     var file = out_dir.openFile(config_file_name, .{})
//     catch |err0| switch (err0) {
//         error.FileNotFound => {
//             return try std.fs.Dir.copyFile(source_dir, config_file_name, out_dir, config_file_name, .{});
//         },
//         else => return err0,
//     };
//     defer file.close();

//     const full_path = try std.fs.path.join(self.allocator, &.{out_dir_path, config_file_name});
//     defer self.allocator.free(full_path);

//     try self.logger.log(.warn, "Already exists: `{s}`", .{full_path});
// }