const std = @import("std");
const efsw = @import("efsw");
const core = @import("core");
const app_context = @import("build_options").app_context;

const events = core.events;

// TODO:
// const Connection = core.sockets.Connection.Client(app_context, GenerateWorker);
const EventDispatcher = core.sockets.EventDispatcher;
const Logger = core.Logger.withAppContext(app_context);
const ReceiveEntry = core.sockets.ReceiveEntry;

const BootPhaseState = core.guest_phases.BootPhaseState(GuestStage, app_context);

const Setting = @import("./Setting.zig");
// const PathMatcher = @import("./PathMatcher.zig").PathMatcher(u21);
// const worker_context = "worker/file-watching";

const Symbol = core.Symbol;

allocator: std.mem.Allocator,
setting: *const Setting,
connection: *GuestStage.Connection,
dispatcher: EventDispatcher.Sized(1),
state: State,

const GuestStage = @This();

pub const Connection = core.sockets.Connection.Client(app_context);

pub fn create(allocator: std.mem.Allocator, connection: *Connection, setting: *const Setting) !GuestStage {
    errdefer connection.deinit();

    try connection.subscribe(&.{
        .probe_launching,
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
        .request => try self.doRequestPhase(),
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

fn doRequestPhase(self: *GuestStage) !void {
    self.state.deinit();
    self.dispatcher.phase = .request;    
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

    // var watcher = try WatcherWrapper.init(self, setting);
    // defer watcher.deinit();

    // if (setting.watch) {
    //     watcher.instance.start();
    // }

// fn waitNextDispatch(self: *GuestStage, setting: Setting) !void {
//     const _item = self.connection.dispatcher.dispatch() catch |err| switch (err) {
//         error.InvalidResponse => {
//             try self.logger.log(.warn, "Unexpected data received", .{});
//             return;
//         },
//         else => return err,
//     };

//     if (_item) |item| {
//         defer item.deinit();
        
//         switch (item.event) {
//             .ready_watch_path => {
//                 try self.sendAllFiles(setting.sources, setting.filter);
//                 try self.connection.dispatcher.post(.finish_watch_path);
//             },
//             .worker_response => |res| {
//                 try self.handleWokerResponse(res, setting);
//             },
//             .quit => {
//                 try self.connection.dispatcher.quitAccept();
//             },
//             .quit_all => {
//                 try self.connection.dispatcher.quitAccept();
//                 try self.connection.pull_sink_socket.stop();
//             },
//             else => {
//                 try self.logger.log(.warn, "Discard command: {}", .{std.meta.activeTag(item.event)});
//             },
//         }
//     }  
// }

// fn sendAllFiles(self: *GuestStage, sources: []const Setting.SourceDir, filter: PathMatcher) !void {
//     for (sources) |src| {
//         const file_stat = try std.fs.cwd().statFile(src.dir_path);
//         if (file_stat.kind == .file) {
//             const name = std.fs.path.basename(src.dir_path);
//             try self.sendFile(src.category, std.fs.cwd(), src.dir_path, name, filter);
//         }
//         else if (file_stat.kind == .directory) {
//             try self.sendFiledOfDir(src.category, src.dir_path, filter);
//         }
//     }
// }

// fn sendFiledOfDir(self: *GuestStage, category: core.TopicCategory, dir_path: core.FilePath, filter: PathMatcher) !void {
//     var dir = try std.fs.cwd().openDir(dir_path, .{});
//     defer dir.close();

//     var iter = try dir.walk(self.allocator);
//     defer iter.deinit();

//     while (try iter.next()) |entry| {
//         if (entry.kind == .file) {
//             try self.sendFile(category, entry.dir, entry.basename, entry.path, filter);
//         }
//     }
// }

// const toUnicodeString = @import("./PathMatcher.zig").toUnicodeString;

// fn sendFile(self: *GuestStage, category: core.TopicCategory, base_dir: std.fs.Dir, file_path: core.FilePath, name: core.FilePath, filter: PathMatcher) !void {
//     const base_dir_path = try base_dir.realpathAlloc(self.allocator, ".");
//     defer self.allocator.free(base_dir_path);

//     const file_path_abs = try base_dir.realpathAlloc(self.allocator, file_path);
//     defer self.allocator.free(file_path_abs);

//     const path_u = try toUnicodeString(self.allocator, file_path_abs);
//     defer self.allocator.free(path_u);

//     if (filter.matchByExclude(path_u).exclude) {
//         return;
//     }
//     if (! filter.matchByInclude(path_u).include) {
//         return;
//     }

//     try self.logger.log(.debug, "Sending source file: `{s}`", .{file_path_abs});

//     var file = try base_dir.openFile(file_path, .{});
//     defer file.close();

//     const hash = try makeHash(self.allocator, name, file);
//     defer self.allocator.free(hash);

//     // Send path, content, hash
//     try self.connection.dispatcher.post(.{
//         .source_path = try core.Event.Payload.SourcePath.init(
//             self.allocator, .{category, std.fs.path.stem(name), file_path_abs, hash, 1}
//         ),
//     });
// }

// const Hasher = std.crypto.hash.sha2.Sha256;

// fn makeHash(allocator: std.mem.Allocator, file_path: []const u8, file: std.fs.File) !Symbol {
//     var hasher = Hasher.init(.{});

//     hasher.update(file_path);

//     var buf: [8192]u8 = undefined;

//     while (true) {
//         const read_size = try file.read(&buf);
//         if (read_size == 0) break;

//         hasher.update(buf[0..read_size]);
//     }

//     return core.bytesToHexAlloc(allocator, &hasher.finalResult());
// }

// fn handleWokerResponse(self: *GuestStage, res: core.Event.Payload.WorkerResponse, setting: Setting) !void {
//     var reader = core.CborStream.Reader.init(res.content);

//     const category = try reader.readEnum(core.TopicCategory);
//     const dir_path = try reader.readString();
//     const file_path = try reader.readString();

//     var dir = try std.fs.cwd().openDir(dir_path, .{});
//     defer dir.close();

//     try self.sendFile(category, dir, file_path, file_path, setting.filter);
// }

// const WatcherWrapper = struct {
//     allocator: std.mem.Allocator,
//     instance: efsw.Watcher,
//     socket: *zmq.ZSocket,
//     watch_contexts: std.ArrayListUnmanaged(*WatcherWrapper.Context),

//     fn init(stage: *GuestStage, setting: Setting) !WatcherWrapper {
//         const allocator = stage.allocator;

//         var wrapper: WatcherWrapper = .{
//             .allocator = allocator,
//             .instance = try efsw.Watcher.init(allocator, false),
//             .socket = try stage.connection.pull_sink_socket.workerSocket(),
//             .watch_contexts = std.ArrayListUnmanaged(*WatcherWrapper.Context){},
//         };
//         errdefer wrapper.deinit();

//         if (! setting.watch) return wrapper;

//         try wrapper.socket.connect(stage.connection.pull_sink_socket.endpoint);

//         try wrapper.watch_contexts.ensureTotalCapacity(allocator, setting.sources.len);

//         for (setting.sources, 1..) |source, id| {
//             const context = try allocator.create(WatcherWrapper.Context);
//             context.* = .{
//                 .id = id,
//                 .allocator = allocator,
//                 .category = source.category,
//                 .root_dir = source.dir_path,
//                 .socket = wrapper.socket,
//             };
//             try wrapper.watch_contexts.append(allocator, context);
//             _ = try wrapper.instance.addWatch(source.dir_path, .{
//                 .on_add = handleSourceFile,
//                 .on_modified = handleSourceFile,
//                 .recursive = true,
//                 .mac_modified_exclude_filter = .{.finder_info = true, .inode = true},
//                 .user_data = context,
//             });
//         }

//         return wrapper;
//     }

//     pub fn deinit(self: *WatcherWrapper) void {
//         self.instance.deinit();
//         self.socket.deinit();

//         for (self.watch_contexts.items) |ctx| {
//             self.allocator.destroy(ctx);
//         }
//         self.watch_contexts.deinit(self.allocator);
//     }

//     fn handleSourceFile(_: *efsw.Watcher, _: efsw.Watcher.WatchId, dir_path: core.FilePath, basename: Symbol, user_data: ?*anyopaque) !void {
//         if (user_data == null) return;

//         const context: *WatcherWrapper.Context = @ptrCast(@alignCast(user_data.?));

//         var dir = try std.fs.cwd().openDir(dir_path, .{});
//         defer dir.close();

//         const stat = try dir.statFile(basename);
//         if (stat.kind == .directory) return;

//         const event = try encodeWorkerResponse(context.allocator, context.category, context.root_dir, dir_path, basename);
//         defer event.deinit();
        
//         try core.sendEvent(context.allocator, context.socket, .{.kind = .post, .from = worker_context, .event = event});
//     }

//     fn encodeWorkerResponse(allocator: std.mem.Allocator, category: core.TopicCategory, root_dir_path: core.FilePath, dir_path: core.FilePath, basename: Symbol) !core.Event {
//         const relative_path = try std.fs.path.relative(allocator, root_dir_path, dir_path);
//         defer allocator.free(relative_path);
//         const file_path = try std.fs.path.join(allocator, &.{ relative_path, basename });
//         defer allocator.free(file_path);

//         var writer = try core.CborStream.Writer.init(allocator);
//         defer writer.deinit();

//         _ = try writer.writeEnum(core.TopicCategory, category);
//         _ = try writer.writeString(root_dir_path);
//         _ = try writer.writeString(file_path);

//         return .{
//             .worker_response = try core.Event.Payload.WorkerResponse.init(allocator, .{ writer.buffer.items })
//         };
//     }

//     const Context = struct {
//         id: usize,
//         allocator: std.mem.Allocator,
//         category: core.TopicCategory,
//         root_dir: core.FilePath,
//         socket: *zmq.ZSocket,
//     };
// };
