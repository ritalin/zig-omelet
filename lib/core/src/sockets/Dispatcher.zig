const std = @import("std");
const nnng = @import("nnng");
const root = @import("../root.zig");

const types = root.types;
const events = root.events;
const sockets = root.sockets;

const EventDispatcher = @This();

const putConsoleLog = @import("../supports/log_support.zig").putConsoleLog;

pub fn Sized(comptime poller_size: comptime_int) type {
    return struct {
        allocator: std.mem.Allocator,
        queue: Queue,
        poller: ReceivePoller,
        phase: events.EventPhase,
        log_router: LogRouter,
        vtable: VTable,
        const Dispatcher = @This();
        const ReceivePoller = nnng.ReceivePoller(poller_size);

        pub fn create(context: nnng.Context, on_poll: Dispatcher.VTable.PollFn, options: Options) !Dispatcher {
            return .{
                .allocator = context.allocator,
                .queue = .{
                    .allocator = context.allocator,
                    .send_queue = .empty,
                    .receive_queue = .empty,
                },
                .poller = try ReceivePoller.create(context),
                .phase = .{ .kind = .preboot, .agreement = .pending },
                .log_router = .{ .style = options.log_style, .no_color = options.no_color },
                .vtable = .{
                    .on_poll = on_poll,
                },
            };
        }

        pub fn deinit(self: *Dispatcher) void {
            self.queue.send_queue.deinit(self.queue.allocator);
            self.queue.receive_queue.deinit(self.queue.allocator);
            self.poller.deinit();
        }

        pub fn run(self: *Dispatcher, stage_name: types.StageName, on_dispatch: VTable.DispatchFn) !void {
            var skip_entries: std.ArrayListUnmanaged(sockets.ReceiveEntry) = .empty;
            defer skip_entries.deinit(self.queue.allocator);

            var prev_status: IterationStatus = .awake;

            while (true) {
                const entry = self.queue.receive_queue.popFront();
                prev_status = try self.iterationInternal(stage_name, prev_status, entry, &skip_entries, on_dispatch);

                switch (prev_status) { 
                    .handled, .no_entry, .pending_quit, .pending_poll => continue, 
                    .terminated => break,
                    .awake => {
                        defer skip_entries.clearRetainingCapacity();
                        try self.queue.entrySkipped(skip_entries.items);
                    }
                }
            }
        }

        pub fn iteration(self: *Dispatcher, stage_name: types.StageName, options: IterationOptions, on_dispatch: VTable.DispatchFn) !IterationStatus {
            var skip_entries: std.ArrayListUnmanaged(sockets.ReceiveEntry) = .empty;
            defer skip_entries.deinit(self.queue.allocator);
            
            var prev_status: IterationStatus = options.prev_status;

            while (true) {
                prev_status = try self.iterationInternal(stage_name, prev_status, self.queue.receive_queue.popFront(), &skip_entries, on_dispatch);
                switch (prev_status) {
                    .handled => {
                        if (!options.handle_all) break;
                    },
                    .no_entry => {
                        if (options.handle_all) break;
                    },
                    .pending_quit => {
                        if (options.no_quit) break;
                    },
                    .terminated => break,
                    .pending_poll => {
                        if (options.no_poll) break;
                    },
                    .awake => {
                        if (!options.handle_all) break;
                    },
                }
            }

            if (skip_entries.items.len > 0) {
                try self.queue.entrySkipped(skip_entries.items);
            }
            return prev_status;
        }

        fn iterationInternal(
            self: *Dispatcher, 
            stage_name: types.StageName, 
            prev_status: IterationStatus, 
            entry_opt: ?sockets.ReceiveEntry, 
            skip_entries: *std.ArrayListUnmanaged(sockets.ReceiveEntry),
            on_dispatch: VTable.DispatchFn) !IterationStatus 
        {
            switch (prev_status) {
                .handled, .awake => {
                    if (entry_opt) |e| {
                        var entry = e;
                        var dirty: EventDispatcher.DirtyState = .none;

                        try self.log_router.log(self, .trace, stage_name, "Receive/pipe_id: {}, event: {s}, from-stage: {s}", .{ entry.pipe_id, @tagName(entry.event), entry.from_stage });
                        try on_dispatch(self, entry, &dirty);

                        switch (dirty) {
                            .none => {
                                entry.deinit(self.queue.allocator);
                            },
                            .delayed => {
                                try skip_entries.append(self.queue.allocator, entry);
                            },
                            .unhandled => {
                                defer entry.deinit(self.queue.allocator);
                                try self.log(.warn, stage_name, "Unhandled/event: {s}, phase: {}", .{ @tagName(entry.event), self.phase });
                            }
                        }

                        return .handled;
                    }
                    return .no_entry;
                },
                .no_entry => {
                    if (std.meta.eql(self.phase, .{ .kind = .quitting, .agreement = .confirmed})) {
                        return .pending_quit;
                    }
                    const batch_sender_log = self.log_router.as_sender();
                    const post_nonblocking = (! @import("builtin").is_test);

                    while (self.queue.send_queue.popFront()) |channel| {
                        try batch_sender_log.log(self, .trace, stage_name, "Send/pipe_id: {}", .{ channel.pipe_id });
                        channel.submit(.{ .flags = .{.nonblocking = post_nonblocking} }) catch |err| switch (err) {
                            error.WouldBlock => {
                                try self.queue.postPriority(channel);
                                break;
                            },
                            else => return err,
                        };
                    }
                    return .pending_poll;
                },
                .pending_quit => {
                    // TODO: about daemon boot or managed boot
                    if (self.vtable.on_quit) |q| {
                        try (q.handler)(q.ptr);
                    }
                    self.phase = .{.kind = .quit_done, .agreement = .confirmed};
                    return .terminated;
                },
                .terminated => {
                    return .terminated;
                },
                .pending_poll => {
                    _ = try self.poller.poll(Dispatcher.doPoll);

                    return .awake;
                }
            }
        }

        pub fn log(self: *Dispatcher, comptime level: events.LogLevel, stage_name: types.StageName, comptime fmt: []const u8, args: anytype) !void {
            if (! comptime std.log.logEnabled(level.toStdLevel(), .default)) return;
            try self.log_router.log(self, level, stage_name, fmt, args);
        }

        fn doPoll(poller: *ReceivePoller, results: []const nnng.PollEvent) !void {
            const self: *Dispatcher = @alignCast(@fieldParentPtr("poller", poller));
            try (self.vtable.on_poll)(self, results);
        }

        pub const LogForwarder = struct {
            ptr: *anyopaque,
            handler: *const fn (ptr: *anyopaque, dispatcher: *Dispatcher, log_event: events.Event.Payload.Log, mode: root.Logger.LogIntegratedMode) anyerror!void,
        };

        pub const VTable = struct {
            on_poll: PollFn,
            on_post: ?RawMessageForwarder = null,
            on_log: ?LogForwarder = null,
            on_quit: ?QuitHandler = null,

            pub const DispatchFn = *const fn (dispatcher: *Dispatcher, channel: sockets.ReceiveEntry, dirty: *DirtyState) anyerror!void;
            pub const PollFn = *const fn (dispatcher: *Dispatcher, results: []const nnng.PollEvent) anyerror!void;
            pub const RawMessageForwarder = struct {
                ptr: *anyopaque,
                handler: *const fn (ptr: *anyopaque, dispatcher: *Dispatcher, msg: nnng.Message) anyerror!void,
            };
            pub const QuitHandler = struct {
                ptr: *anyopaque,
                handler: *const fn (ptr: *anyopaque) anyerror!void,
            };
        };

        pub const LogRouter = struct {
            style: root.Logger.LogStyle,
            no_color: bool,

            pub fn log(self: *const LogRouter, dispatcher: *Dispatcher, comptime level: events.LogLevel, stage_name: types.StageName, comptime fmt: []const u8, args: anytype) !void {
                if (! comptime std.log.logEnabled(level.toStdLevel(), .default)) return;
                if (!root.Logger.accepted(level)) return;

                switch (self.style) {
                    .discard => {
                        // noop
                    },
                    .integrated => |mode| {
                        if (dispatcher.vtable.on_log) |on_log| {
                            const content = try std.fmt.allocPrint(dispatcher.allocator, fmt, args);
                            defer dispatcher.allocator.free(content);

                            const log_event = events.Event.Payload.Log.init(.{level, content});
                            return (on_log.handler)(on_log.ptr, dispatcher, log_event, mode);
                        }
                        unreachable;
                    },
                    .stderr => { 
                        try putConsoleLog(level, stage_name, fmt, args);
                    }
                }
            }

            pub fn handleLogEvent(self: *const LogRouter, from_stage: types.StageName, log_event: events.Event.Payload.Log) !void {
                _ = self;
                try putConsoleLog(log_event.level, from_stage, "{s}", .{log_event.content});
            }

            fn as_sender(self: *const LogRouter) LogRouter {
                return .{
                    .style = if (std.meta.activeTag(self.style) == .integrated) .{ .integrated = .direct } else self.style,
                    .no_color = self.no_color,
                };
            }
        };

        // TODO:
        // pub fn postFatal(self: *Self, stack_trace: ?*std.builtin.StackTrace) !void {
        //     const message = err_message: {
        //         if (stack_trace) |x| {
        //             var buf = std.ArrayList(u8).init(self.allocator);
        //             defer buf.deinit();

        //             var writer = buf.writer();
        //             try writer.print("{}", .{x});

        //             break:err_message try buf.toOwnedSlice();
        //         }
        //         else {
        //             break:err_message try self.allocator.dupe(u8, "Fatal eerror occured");
        //         }
        //     };
        //     defer self.allocator.free(message);

        pub const Queue = struct {
            allocator: std.mem.Allocator,
            send_queue: std.Deque(sockets.SendChannel),
            receive_queue: std.Deque(sockets.ReceiveEntry),

            pub fn post(self: *Queue, event: events.Event, channel: sockets.SendChannel) !void {
                if (std.meta.activeTag(event) != .log) {
                    const d: *Dispatcher = @alignCast(@fieldParentPtr("queue", self));
                    try d.log_router.log(d, .trace, channel.stage, "SendQueue/event: {s}", .{@tagName(std.meta.activeTag(event))});
                }

                var channel_mut = channel;
                try channel_mut.encode(event);
                try self.send_queue.pushBack(self.allocator, channel_mut);
            }

            pub fn pushReceiveQueue(self: *Queue, entry: sockets.ReceiveEntry) !void {
                try self.receive_queue.pushBack(self.allocator, entry);
            }

            pub fn postPriority(self: *Queue, channel: sockets.SendChannel) !void {
                try self.send_queue.pushFront(self.allocator, channel);
            }

            pub fn entrySkipped(self: *Queue, entries: []const sockets.ReceiveEntry) !void {
                try self.receive_queue.pushBackSlice(self.allocator, entries);
            }
        };

        pub const RawMessageForwarding = struct {
            pub fn post(dispatcher: *Dispatcher, msg: nnng.Message) !void {
                if (dispatcher.vtable.on_post) |on_post| {
                    try (on_post.handler)(on_post.ptr, dispatcher, msg);
                }
            }
        };
    };
}

pub const DirtyState = enum { none, delayed, unhandled };

pub const Options = struct {
    log_style: root.Logger.LogStyle = .stderr,
    no_color: bool = false,
    forward_worker: bool = true,
};

pub const IterationStatus = enum { 
    handled, no_entry, pending_quit, pending_poll, terminated, awake 
};
pub const IterationOptions = struct {
    prev_status: IterationStatus = .awake,
    handle_all: bool = false,
    no_poll: bool = false,
    no_quit: bool = false,
};

test "dispatcher test" {
    std.testing.refAllDecls(@This());
}

pub const tests = struct {
    const supports = @import("../supports/test_support.zig");
    const encodeToCbor = @import("../events/encoder.zig").encodeToCbor;

    const ReceiveEntry = root.sockets.ReceiveEntry;
    const ServerConnection = root.sockets.Connection.Server("runner");
    const ClientConnection = root.sockets.Connection.Client("stage");
    const Dispatcher = EventDispatcher.Sized(8);

    const WORKER_ENDPOINT = root.configs.Endpoint.WORKER_ENDPOINT;

    fn noopHandler(_: *Dispatcher, _: ReceiveEntry, _: *EventDispatcher.DirtyState) !void {}

    fn serverHandler(d: *Dispatcher, entry: ReceiveEntry, _: *EventDispatcher.DirtyState) !void {
        switch (entry.event) {
            .launching => {},
            .quit => {
                d.phase = .{.kind = .quitting, .agreement = .confirmed};
            },
            else => unreachable,
        }
    }

    fn clientHandler(d: *Dispatcher, entry: ReceiveEntry, _: *EventDispatcher.DirtyState) !void {
        switch (entry.event) {
            .launching, .ack => {},
            .probe => {
                d.phase = .{.kind = .quitting, .agreement = .confirmed};
            },
            else => unreachable,
        }
    }

    fn runConcurrent(host_dispatcher: *Dispatcher, guest_dispatcher: *Dispatcher) (std.Io.ConcurrentError || std.Io.Cancelable)!void {
        const Runnable = struct {
            d: *Dispatcher,
            callback: Dispatcher.VTable.DispatchFn,

            fn run(self: *const @This(), stage_name: types.StageName) void {
                self.d.run(stage_name, self.callback) catch |err| {
                    const msg = std.fmt.allocPrint(std.testing.allocator, "Unhandled dispatch error: {}", .{err}) catch @panic("OOM");
                    defer std.testing.allocator.free(msg);
                    @panic(msg);
                };
            }
        };

        var group: std.Io.Group = .init;
        try group.concurrent(std.testing.io, Runnable.run, .{ &Runnable{ .d = host_dispatcher, .callback = serverHandler }, "runner" });
        try group.concurrent(std.testing.io, Runnable.run, .{ &Runnable{ .d = guest_dispatcher, .callback = clientHandler }, "guest" });
        return group.await(std.testing.io);
    }

    test "pull event by PULL" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var tmp_dir = try supports.createTmpDir();
        defer tmp_dir.cleanup();

        var ep_config = try supports.testEndpointConfig(io, &tmp_dir, .{});
        var ep = try root.configs.Endpoint.runtimeIpc(allocator, ep_config);
        defer supports.releaseEndpoint(io, &ep, &ep_config);

        var conn = try ServerConnection.create(std.testing.io, std.testing.allocator, 4, ep);
        defer conn.deinit();
        try conn.bind();

        var dispatcher: Dispatcher = try conn.configureDispatcher(8, .{.log_style = .discard});
        defer dispatcher.deinit();

        var push_socket = socket: {
            const b = try nnng.Push.open(conn.context);
            break:socket try b.as_dialer(ep.push_pull);
        };
        try push_socket.transport.start(.{ .nonblocking = true });
        defer push_socket.close();

        const pipe = push_socket.pipe.item;
        send_event: {
            var msg = try nnng.Message.create();
            try encodeToCbor(&msg.writer, .{ .header = .quit, .stage_name = "test-stage", .event = .quit });
            try pipe.sender().submit(msg);
            break:send_event;
        }

        try std.testing.expectEqual(.preboot, dispatcher.phase.kind);

        try dispatcher.run("runner", serverHandler);

        try std.testing.expectEqual(.quit_done, dispatcher.phase.kind);
    }

    test "pull event by client worker PULL" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var tmp_dir = try supports.createTmpDir();
        defer tmp_dir.cleanup();

        var ep_config = try supports.testEndpointConfig(io, &tmp_dir, .{});
        var ep = try root.configs.Endpoint.runtimeIpc(allocator, ep_config);
        defer supports.releaseEndpoint(io, &ep, &ep_config);

        var conn = try ClientConnection.create(std.testing.io, std.testing.allocator, ep);
        defer conn.deinit();
        try conn.connect();

        var dispatcher: Dispatcher = try conn.configureDispatcher(8, .{.log_style = .discard});
        defer dispatcher.deinit();

        var worker_push_socket = socket: {
            const b = try nnng.Push.open(conn.context);
            break:socket try b.as_dialer(WORKER_ENDPOINT);
        };
        try worker_push_socket.transport.start(.{ .nonblocking = true });
        defer worker_push_socket.close();

        const worker_pipe = worker_push_socket.pipe.item;

        send_event: {
            var msg = try nnng.Message.create();
            try encodeToCbor(&msg.writer, .{ .header = .finish_source_path, .stage_name = "test-stage", .event = .finish_source_path });
            try worker_pipe.sender().submit(msg);
            break:send_event;
        }

        try std.testing.expectEqual(0, dispatcher.queue.send_queue.len);

        _ = try dispatcher.poller.poll(Dispatcher.doPoll);

        try std.testing.expectEqual(1, dispatcher.queue.send_queue.len);

        var channel: ?sockets.SendChannel = dispatcher.queue.send_queue.popFront();
        defer if (channel) |*c| c.deinit();

        try std.testing.expectEqual(conn.push_socket.pipe.item.id, channel.?.pipe_id);

        const decodeFromCbor = @import("../events/decoder.zig").decodeFromCbor;
        const packet = try decodeFromCbor(std.testing.allocator, channel.?.msg.bytes());
        try std.testing.expectEqual(.finish_source_path, packet.event);
    }

    test "receive event by REP" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var tmp_dir = try supports.createTmpDir();
        defer tmp_dir.cleanup();

        var ep_config = try supports.testEndpointConfig(io, &tmp_dir, .{});
        var ep = try root.configs.Endpoint.runtimeIpc(allocator, ep_config);
        defer supports.releaseEndpoint(io, &ep, &ep_config);

        var conn = try ServerConnection.create(std.testing.io, std.testing.allocator, 4, ep);
        defer conn.deinit();
        try conn.bind();

        var dispatcher: Dispatcher = try conn.configureDispatcher(8, .{.log_style = .discard});
        defer dispatcher.deinit();

        var worker_push_socket = socket: {
            const b = try nnng.Req.open(conn.context);
            break:socket try b.as_dialer(ep.req_rep);
        };
        try worker_push_socket.transport.start(.{ .nonblocking = true });
        defer worker_push_socket.close();

        const pipe = worker_push_socket.pipe.item;
        send_event: {
            var msg = try nnng.Message.create();
            try encodeToCbor(&msg.writer, .{ .header = .quit, .stage_name = "test-stage", .event = .quit });
            try pipe.sender().submit(msg);
            break:send_event;
        }

        try std.testing.expectEqual(.preboot, dispatcher.phase.kind);

        var task = try io.concurrent(Dispatcher.run, .{&dispatcher, "runner", serverHandler});
        try task.await(io);

        try std.testing.expectEqual(.quit_done, dispatcher.phase.kind);
    }

    test "receive event by SUB" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var tmp_dir = try supports.createTmpDir();
        defer tmp_dir.cleanup();

        var ep_config = try supports.testEndpointConfig(io, &tmp_dir, .{});
        var ep = try root.configs.Endpoint.runtimeIpc(allocator, ep_config);
        defer supports.releaseEndpoint(io, &ep, &ep_config);

        var host = try ServerConnection.create(std.testing.io, std.testing.allocator, 4, ep);
        defer host.deinit();

        var guest = try ClientConnection.create(std.testing.io, std.testing.allocator, ep);
        defer guest.deinit();
        try guest.subscribe(&.{ .probe });

        var subscriptions: std.ArrayListUnmanaged(types.Symbol) = .empty;
        defer subscriptions.deinit(std.testing.allocator);
        var view = guest.cmd_socket.subscriptionView();
        // try view.enableWildcard();
        try view.extractSubscriptions(std.testing.allocator, &subscriptions);

        try host.cmd_socket.transport.start(.{});
        try guest.cmd_socket.transport.start(.{});

        var dispatcher: Dispatcher = try guest.configureDispatcher(8, .{.log_style = .discard});
        defer dispatcher.deinit();
        dispatcher.vtable.on_quit = null;

        try io.sleep(.fromMilliseconds(20), .awake);

        var msg = try nnng.Message.create();
        send_event: {
            try encodeToCbor(&msg.writer, .{ .header = .probe, .stage_name = "test-stage", .event = .{.probe = .terminating} });
            try host.cmd_socket.pipe.item.sender().submit(msg);
            break:send_event;
        }

        try std.testing.expectEqual(.preboot, dispatcher.phase.kind);

        var task = try io.concurrent(Dispatcher.run, .{&dispatcher, "guest", clientHandler});
        try task.await(io);

        try std.testing.expectEqual(.quit_done, dispatcher.phase.kind);
    }

    const Coordinator = struct {
        allocator: std.mem.Allocator,
        host_dispatcher: Dispatcher,
        host_connection: ServerConnection,
        guest_dispatcher: Dispatcher,
        guest_connection: ClientConnection,

        pub fn handleQuit(ptr: *anyopaque) anyerror!void {
            var self: *Coordinator = @ptrCast(@alignCast(ptr));

            host: {
                const entry: ReceiveEntry = .{
                    .event = .quit,
                    .msg = try nnng.Message.withCapacity(0),
                    .pipe_id = 0,
                    .buffer = &.{},
                    .from_stage = ClientConnection.stage_name,
                    .features = .{.last_msg_owner = true},
                };
                try self.host_dispatcher.queue.pushReceiveQueue(entry);
                break:host;
            }
            guest: {
                const entry: ReceiveEntry = .{
                    .event = .ack,
                    .msg = try nnng.Message.withCapacity(0),
                    .pipe_id = 0,
                    .buffer = &.{},
                    .from_stage = ServerConnection.stage_name,
                    .features = .{.last_msg_owner = true},
                };
                try self.guest_dispatcher.queue.pushReceiveQueue(entry);
                break:guest;
            }
        }
    };

    test "Host/Guest rally" {
        // TODO: beacause blocked on GHA...
        // if (true) { 
        //     return error.SkipZigTest; 
        // }

        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var tmp_dir = try supports.createTmpDir();
        defer tmp_dir.cleanup();

        var ep_config = try supports.testEndpointConfig(io, &tmp_dir, .{});
        var ep = try root.configs.Endpoint.runtimeIpc(allocator, ep_config);
        defer supports.releaseEndpoint(io, &ep, &ep_config);

        var host = try ServerConnection.create(std.testing.io, std.testing.allocator, 4, ep);
        // var host = try ServerConnection.create(std.testing.io, std.testing.allocator, ep);
        defer host.deinit();

        var guest = try ClientConnection.create(std.testing.io, std.testing.allocator, ep);
        defer guest.deinit();
        try guest.subscribe(&.{ .probe });

        // listeners
        try host.pull_socket.transport.start(.{});
        try host.reply_socket.transport.start(.{});
        try host.cmd_socket.transport.start(.{});
        // dialers
        try guest.push_socket.transport.start(.{});
        try guest.req_socket.transport.start(.{});
        try guest.cmd_socket.transport.start(.{});

        var c: Coordinator = .{
            .allocator = allocator,
            .host_connection = host,
            .host_dispatcher = try host.configureDispatcher(8, .{ .log_style = .discard }),
            .guest_connection = guest,
            .guest_dispatcher = try guest.configureDispatcher(8, .{ .log_style = .discard }),
        };
        defer c.host_dispatcher.deinit();
        defer c.guest_dispatcher.deinit();

        c.guest_dispatcher.vtable.on_quit = .{
            .ptr = &c,
            .handler = Coordinator.handleQuit,
        };

        try std.testing.expectEqual(.preboot, c.host_dispatcher.phase.kind);
        try std.testing.expectEqual(.preboot, c.guest_dispatcher.phase.kind);

        try io.sleep(.fromMilliseconds(20), .awake);

        var guest_status: IterationStatus = .awake;

        host_iter: {
            try c.host_dispatcher.queue.post(.{.probe = .terminating}, try host.commandChannel());
            iter: {
                const host_status = try c.host_dispatcher.iteration("host", .{.no_poll = true}, serverHandler);
                try std.testing.expectEqual(.pending_poll, host_status);
                break:iter;
            }
            break:host_iter;
        }
        guest_iter: {
            try std.testing.expectEqual(0, c.guest_dispatcher.queue.receive_queue.len);
            iter: {
                guest_status = try c.guest_dispatcher.iteration("guest", .{.no_quit = true, .prev_status = guest_status}, clientHandler);
                try std.testing.expectEqual(.awake, guest_status);
                try std.testing.expectEqual(1, c.guest_dispatcher.queue.receive_queue.len);
                break:iter;
            }
            iter: {
                guest_status = try c.guest_dispatcher.iteration("guest", .{.no_quit = true, .prev_status = guest_status}, clientHandler);
                try std.testing.expectEqual(.handled, guest_status);
                try std.testing.expectEqual(0, c.guest_dispatcher.queue.receive_queue.len);
                try std.testing.expectEqual(.quitting, c.guest_dispatcher.phase.kind);
                break:iter;
            }
            iter: {
                guest_status = try c.guest_dispatcher.iteration("guest", .{.prev_status = guest_status}, clientHandler);
                try std.testing.expectEqual(.terminated, guest_status);
                try std.testing.expectEqual(0, c.guest_dispatcher.queue.send_queue.len);
                try std.testing.expectEqual(.quit_done, c.guest_dispatcher.phase.kind);
                break:iter;
            }
            break:guest_iter;
        }
        host_iter: {
            var host_status: IterationStatus = .awake;
            iter: {
                host_status = try c.host_dispatcher.iteration("host", .{.no_quit = true, .prev_status = host_status}, serverHandler);
                try std.testing.expectEqual(.handled, host_status);
                try std.testing.expectEqual(0, c.host_dispatcher.queue.receive_queue.len);
                try std.testing.expectEqual(.quitting, c.host_dispatcher.phase.kind);
                break:iter;
            }
            iter: {
                host_status = try c.host_dispatcher.iteration("host", .{.prev_status = host_status}, serverHandler);
                try std.testing.expectEqual(.terminated, host_status);
                try std.testing.expectEqual(0, c.host_dispatcher.queue.receive_queue.len);
                try std.testing.expectEqual(.quit_done, c.host_dispatcher.phase.kind);
                break:iter;
            }
            break:host_iter;
        }
    }

    test "single iteration" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var tmp_dir = try supports.createTmpDir();
        defer tmp_dir.cleanup();

        var ep_config = try supports.testEndpointConfig(io, &tmp_dir, .{});
        var ep = try root.configs.Endpoint.runtimeIpc(allocator, ep_config);
        defer supports.releaseEndpoint(io, &ep, &ep_config);

        var conn = try ServerConnection.create(std.testing.io, std.testing.allocator, 4, ep);
        defer conn.deinit();
        try conn.bind();

        var dispatcher: Dispatcher = try conn.configureDispatcher(8, .{.log_style = .discard});
        defer dispatcher.deinit();

        var worker_push_socket = socket: {
            const b = try nnng.Req.open(conn.context);
            break:socket try b.as_dialer(ep.req_rep);
        };
        try worker_push_socket.transport.start(.{ .nonblocking = true });
        defer worker_push_socket.close();

        const pipe = worker_push_socket.pipe.item;
        send_event: {
            var msg = try nnng.Message.create();
            try encodeToCbor(&msg.writer, .{ .header = .quit, .stage_name = "test-stage", .event = .quit });
            try pipe.sender().submit(msg);
            break:send_event;
        }

        var prev_status: IterationStatus = .awake;

        iteration: {
            prev_status = try dispatcher.iteration("runner", .{.no_poll = true, .prev_status = prev_status}, noopHandler);
            try std.testing.expectEqual(.pending_poll, prev_status);
            try std.testing.expectEqual(0, dispatcher.queue.receive_queue.len);
            break:iteration;
        }
        iteration: {
            prev_status = try dispatcher.iteration("runner", .{.prev_status = prev_status}, noopHandler);
            try std.testing.expectEqual(.awake, prev_status);
            try std.testing.expectEqual(1, dispatcher.queue.receive_queue.len);
            break:iteration;
        }
        iteration: {
            prev_status = try dispatcher.iteration("runner", .{.prev_status = prev_status}, noopHandler);
            try std.testing.expectEqual(.handled, prev_status);
            try std.testing.expectEqual(0, dispatcher.queue.receive_queue.len);
            break:iteration;
        }
    }
};
