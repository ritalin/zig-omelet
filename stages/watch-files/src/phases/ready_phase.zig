const std = @import("std");
const core = @import("core");
const efsw = @import("efsw");

const ReceiveEntry = core.sockets.ReceiveEntry;
const EventDispatcher = core.sockets.EventDispatcher;
const StructView = core.events.StructView;
const Event = core.events.Event;

const PathMatcher = @import("../PathMatcher.zig").PathMatcher(u21);

const FileIterateWorker = @import("../watch_worker.zig").FileIterateWorker;

pub fn ReadyWatchFileState(comptime GuestStage: type) type {
    return struct {
        const Self = @This();

        pub const create: Self = .{};
        pub fn deinit(_: *Self) void {}

        pub fn handle(self: *Self, stage: *GuestStage, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
            _ = self;
            switch (entry.event) {
                .probe => |phase| {
                    if ((phase == .ready) and (std.meta.eql(stage.dispatcher.phase, .{.kind = .ready, .agreement = .pending}))) {
                        var channel = try stage.connection.requestChannel();
                        try channel.submit(stage.connection.context.io, .ready, .{});
                        try stage.transitPhase(.ready, .confirmed);

                        if (stage.setting.watch) {
                            // TODO: watch mode
                        }
                        return;
                    }
                },
                .ready_source_path => {
                    try FileIterateWorker(GuestStage).run(stage);
                    return;
                },
                else => {}
            }
            
            try stage.defaultHandler(entry, dirty);
        }
    };
}

test "test ready phase" {
    std.testing.refAllDecls(@This());
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
