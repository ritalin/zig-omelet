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
        queue: Queue,
        poller: ReceivePoller,
        phase: Phase,
        options: Options,
        on_poll: PollFn,
        on_quit: ?QuitHandler = null,

        const Self = @This();
        const ReceivePoller = nnng.ReceivePoller(poller_size);

        pub const DispatchFn = *const fn (dispatcher: *Self, channel: sockets.ReceiveEntry, dirty: *DirtyState) anyerror!void;
        pub const PollFn = *const fn (dispatcher: *Self, results: []const nnng.PollEvent) anyerror!void;

        // TODO:
        // pub const Phase = struct {
        //     level: std.enums.EnumFieldStruct(enum {booting, request, ready, terminating, quitting, done}, bool, false),

        //     pub inline fn ready(self: *Phase) !void {
        //         self.level.ready = true;
        //     }
        //     pub inline fn receiveTerminate(self: *Phase) !void {
        //         try self.ready();
        //         self.level.terminating = true;
        //     }
        //     pub inline fn readyQuit(self: *Phase) !void {
        //         try self.receiveTerminate();
        //         self.level.quitting = true;
        //     }
        //     pub inline fn done(self: *Phase) !void {
        //         try self.readyQuit();
        //         self.level.done = true;
        //     }
        // };

        pub fn create(context: nnng.Context, on_poll: Self.PollFn, options: Options) !Self {
            return .{
                .queue = .{
                    .allocator = context.allocator,
                    .send_queue = .empty,
                    .receive_queue = .empty,
                },
                .poller = try ReceivePoller.create(context),
                .phase = .booting,
                .options = options,
                .on_poll = on_poll,
            };
        }

        pub fn deinit(self: *Self) void {
            self.queue.send_queue.deinit(self.queue.allocator);
            self.queue.receive_queue.deinit(self.queue.allocator);
            self.poller.deinit();
        }

        pub fn run(self: *Self, stage_name: types.StageName, on_dispatch: DispatchFn) !void {
            while (true) {
                var skip_entries: std.ArrayListUnmanaged(sockets.ReceiveEntry) = .empty;
                defer skip_entries.deinit(self.queue.allocator);

                while (self.queue.receive_queue.popFront()) |e| {
                    try self.log(.trace, stage_name, "Receive/pipe_id: {}, event: {s}, from-stage: {s}", .{ e.pipe_id, @tagName(e.event), e.from_stage });

                    var entry = e;
                    var dirty: EventDispatcher.DirtyState = .none;

                    try on_dispatch(self, entry, &dirty);

                    switch (dirty) {
                        .none => {
                            entry.deinit(self.queue.allocator);
                        },
                        .skipped => {
                            try skip_entries.append(self.queue.allocator, entry);
                        },
                        .unhandled => {
                            defer entry.deinit(self.queue.allocator);
                            try self.log(.warn, stage_name, "Unhandled/event: {s}, phase: {}", .{ @tagName(e.event), self.phase });
                        }
                    }
                }
                if (self.phase == .quitting) {
                    if (self.on_quit) |q| {
                        try (q.handler)(q.ptr);
                    }
                    break;
                }

                while (self.queue.send_queue.popFront()) |channel| {
                    try self.log(.trace, stage_name, "Send/pipe_id: {}", .{ channel.inner.pipe_id,  });
                    try channel.submit(.{ .flags = .{.nonblocking = true} });
                }

                _ = try self.poller.poll(Self.doPoll);
                try self.queue.entrySkipped(skip_entries.items);
            }
        }

        pub fn log(self: *Self, comptime level: events.LogLevel, stage_name: types.StageName, comptime fmt: []const u8, args: anytype) !void {
            if (! comptime std.log.logEnabled(level.toStdLevel(), .default)) return;
            try self.logInternal(level, stage_name, fmt, args);
        }

        pub fn handleLogEvent(self: *Self, from_stage: types.StageName, log_event: events.Event.Payload.Log) !void {
            try self.logInternal(log_event.level, from_stage, "{s}", .{ log_event.content });
        }
        
        fn logInternal(self: *Self, level: events.LogLevel, stage_name: types.StageName, comptime fmt: []const u8, args: anytype) !void {
            if (self.options.log_style == .discard) return;
            if (!root.Logger.accepted(level)) return;

            switch (self.options.log_style) {
                .discard => {
                    // noop
                },
                .integrated => {
                    unreachable;
                },
                .stderr => { 
                    try putConsoleLog(level, stage_name, fmt, args);
                }
            }
        }

        fn doPoll(poller: *ReceivePoller, results: []const nnng.PollEvent) !void {
            const self: *Self = @alignCast(@fieldParentPtr("poller", poller));
            try (self.on_poll)(self, results);
        }

        fn doLog(ptr: *anyopaque, level: events.LogLevel, stage_name: types.StageName, comptime fmt: []const u8, args: anytype) anyerror!void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try self.logInternal(level, stage_name, fmt, args);
        }

        // TODO:
        // pub fn delay(self: *Self, socket: *zmq.ZSocket, from: types.Symbol, event: events.Event, routing_id: ?types.Symbol) !void {
        //     try self.receive_queue.prepend(.{
        //         .allocator = self.allocator,
        //         .kind = .response,
        //         .socket = socket,
        //         .from = try self.allocator.dupe(u8, from),
        //         .event = try event.clone(self.allocator),
        //         .routing_id = if (routing_id) |x| try self.allocator.dupe(u8, x) else null,
        //     });
        // }

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

        //     try self.send_queue.pushFront(.{
        //         .allocator = self.allocator,
        //         .kind = .post,
        //         .socket = self.send_socket,
        //         .from = try self.allocator.dupe(u8, stage_name),
        //         .event = .{.report_fatal = try events.Event.Payload.Log.init(self.allocator, .{.err, message})},
        //         .routing_id = null,
        //     });
        // }

        // pub fn revertFromPending(self: *Self) !void {
        //     if (self.receive_pending.dequeue()) |entry| {
        //         try self.send_queue.prepend(entry);
        //     }
        // }
    };
}

pub const Phase = enum { booting, request, ready, terminating, quitting };
pub const DirtyState = enum { none, skipped, unhandled };

pub const Options = struct {
    log_style: root.Logger.LogStyle = .stderr,
    no_color: bool = false,
};

pub const QuitHandler = struct {
    ptr: *anyopaque,
    handler: *const fn (ptr: *anyopaque) anyerror!void,
};

// TODO: will remove
// pub fn Logger(comptime Owner: type) type {
//     return struct {
//         owner: *Owner,

//         pub fn log(self: *Logger, comptime level: events.LogLevel, stage_name: types.StageName, comptime fmt: []const u8, args: anytype) !void {
//             if (! comptime std.log.logEnabled(level.toStdLevel(), .default)) return;
//             return self.owner.log(self.ptr, level, stage_name, fmt, args);
//         }
//     };
// }

pub const Queue = struct {
    allocator: std.mem.Allocator,
    send_queue: std.Deque(sockets.SendChannel),
    receive_queue: std.Deque(sockets.ReceiveEntry),

    pub fn post(self: *Queue, channel: sockets.SendChannel) !void {
        try self.send_queue.pushBack(self.allocator, channel);
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

    fn serverHandler(d: *Dispatcher, entry: ReceiveEntry, _: *EventDispatcher.DirtyState) !void {
        switch (entry.event) {
            .launching => {},
            .quit => {
                d.phase = .quitting;
            },
            else => unreachable,
        }
    }

    fn clientHandler(d: *Dispatcher, entry: ReceiveEntry, _: *EventDispatcher.DirtyState) !void {
        switch (entry.event) {
            .launching => {},
            .quit_all => {
                d.phase = .quitting;
            },
            else => unreachable,
        }
    }

    fn runConcurrent(host_dispatcher: *Dispatcher, guest_dispatcher: *Dispatcher) (std.Io.ConcurrentError || std.Io.Cancelable)!void {
        const Runnable = struct {
            d: *Dispatcher,
            callback: Dispatcher.DispatchFn,

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
        var tmp_dir = try supports.createTmpDir();
        defer tmp_dir.cleanup();

        const ep = try supports.createEndpoint(tmp_dir.dir);
        defer supports.releaseEndpoint(ep);

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
            try pipe.sender().submit(msg, .{});
            break:send_event;
        }

        try std.testing.expectEqual(.booting, dispatcher.phase);

        try dispatcher.run("runner", serverHandler);

        try std.testing.expectEqual(.quitting, dispatcher.phase);
    }

    test "receive event by REP" {
        var tmp_dir = try supports.createTmpDir();
        defer tmp_dir.cleanup();

        const ep = try supports.createEndpoint(tmp_dir.dir);
        defer supports.releaseEndpoint(ep);

        var conn = try ServerConnection.create(std.testing.io, std.testing.allocator, 4, ep);
        defer conn.deinit();
        try conn.bind();

        var dispatcher: Dispatcher = try conn.configureDispatcher(8, .{.log_style = .discard});
        defer dispatcher.deinit();

        var push_socket = socket: {
            const b = try nnng.Req.open(conn.context);
            break:socket try b.as_dialer(ep.req_rep);
        };
        try push_socket.transport.start(.{ .nonblocking = true });
        defer push_socket.close();

        const pipe = push_socket.pipe.item;
        send_event: {
            var msg = try nnng.Message.create();
            try encodeToCbor(&msg.writer, .{ .header = .quit, .stage_name = "test-stage", .event = .quit });
            try pipe.sender().submit(msg, .{});
            break:send_event;
        }

        try std.testing.expectEqual(.booting, dispatcher.phase);

        try dispatcher.run("runner", serverHandler);

        try std.testing.expectEqual(.quitting, dispatcher.phase);
    }

    test "receive event by SUB" {
        var tmp_dir = try supports.createTmpDir();
        defer tmp_dir.cleanup();

        const ep = try supports.createEndpoint(tmp_dir.dir);
        defer supports.releaseEndpoint(ep);

        var host = try ServerConnection.create(std.testing.io, std.testing.allocator, 4, ep);
        defer host.deinit();

        var guest = try ClientConnection.create(std.testing.io, std.testing.allocator, ep);
        defer guest.deinit();
        try guest.subscribe(&.{ .quit_all });

        var subscriptions: std.ArrayListUnmanaged(types.Symbol) = .empty;
        defer subscriptions.deinit(std.testing.allocator);
        var view = guest.cmd_socket.subscriptionView();
        // try view.enableWildcard();
        try view.extractSubscriptions(std.testing.allocator, &subscriptions);

        try host.cmd_socket.transport.start(.{});
        try guest.cmd_socket.transport.start(.{});

        var dispatcher: Dispatcher = try guest.configureDispatcher(8, .{.log_style = .discard});
        defer dispatcher.deinit();
        dispatcher.on_quit = null;

        var msg = try nnng.Message.create();
        send_event: {
            try encodeToCbor(&msg.writer, .{ .header = .quit_all, .stage_name = "test-stage", .event = .quit_all });
            try host.cmd_socket.pipe.item.sender().submit(msg, .{});
            break:send_event;
        }

        try std.testing.expectEqual(.booting, dispatcher.phase);

        try dispatcher.run("guest", clientHandler);

        try std.testing.expectEqual(.quitting, dispatcher.phase);
    }

    test "Host/Guest communication" {
        var tmp_dir = try supports.createTmpDir();
        defer tmp_dir.cleanup();

        const ep = try supports.createEndpoint(tmp_dir.dir);
        defer supports.releaseEndpoint(ep);

        var host = try root.sockets.Connection.Server("runner#2").create(std.testing.io, std.testing.allocator, 4, ep);
        // var host = try ServerConnection.create(std.testing.io, std.testing.allocator, ep);
        defer host.deinit();

        var guest = try ClientConnection.create(std.testing.io, std.testing.allocator, ep);
        defer guest.deinit();
        try guest.subscribe(&.{ .quit_all });

        // listeners
        try host.pull_socket.transport.start(.{});
        try host.reply_socket.transport.start(.{});
        try host.cmd_socket.transport.start(.{});
        // dialers
        try guest.push_socket.transport.start(.{});
        try guest.req_socket.transport.start(.{});
        try guest.cmd_socket.transport.start(.{});

        var host_dispatcher: Dispatcher = try host.configureDispatcher(8, .{ .log_style = .discard });
        defer host_dispatcher.deinit();

        var guest_dispatcher: Dispatcher = try guest.configureDispatcher(8, .{ .log_style = .discard });
        defer guest_dispatcher.deinit();

        publisg_event: {
            var cmd = try host.commandChannel();
            try cmd.encode(.quit_all);
            try host_dispatcher.queue.post(cmd);
            break:publisg_event;
        }

        try std.testing.expectEqual(.booting, host_dispatcher.phase);
        try std.testing.expectEqual(.booting, guest_dispatcher.phase);

        try runConcurrent(&host_dispatcher, &guest_dispatcher);

        try std.testing.expectEqual(.quitting, host_dispatcher.phase);
        try std.testing.expectEqual(.quitting, guest_dispatcher.phase);
    }
};
