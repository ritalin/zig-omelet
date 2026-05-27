const std = @import("std");
const nnng = @import("nnng");
const root = @import("../root.zig");

const types = root.types;
const sockets = root.sockets;

const EventDispatcher = @This();

pub fn Sized(comptime poller_size: comptime_int) type {
    return struct {
        queue: Queue,
        poller: ReceivePoller,
        phase: Phase,
        on_poll: PollFn,
        on_quit: ?QuitHandler = null,

        const Self = @This();
        const ReceivePoller = nnng.ReceivePoller(poller_size);

        pub const DispatchFn = *const fn (dispatcher: *Self, channel: sockets.ReceiveEntry, dirty: *DirtyState) anyerror!void;
        pub const PollFn = *const fn (queue: *EventDispatcher.Queue, results: []const nnng.PollEvent) anyerror!void;

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

        pub fn create(context: nnng.Context, on_poll: Self.PollFn) !Self {
            return .{
                .queue = .{
                    .allocator = context.allocator,
                    .send_queue = .empty,
                    .receive_queue = .empty,
                },
                .poller = try ReceivePoller.create(context),
                .phase = .booting,
                .on_poll = on_poll,
            };
        }

        pub fn deinit(self: *Self) void {
            self.queue.send_queue.deinit(self.queue.allocator);
            self.queue.receive_queue.deinit(self.queue.allocator);
            self.poller.deinit();
        }

        pub fn run(self: *Self, on_dispatch: DispatchFn, options: struct { force_concurrent: bool = false }) !void {
            while (true) {
                var skip_entries: std.ArrayListUnmanaged(sockets.ReceiveEntry) = .empty;
                defer skip_entries.deinit(self.queue.allocator);

                while (self.queue.receive_queue.popFront()) |e| {
                    var entry = e;
                    var dirty: EventDispatcher.DirtyState = .none;

                    // TODO:
                    // trace event log

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

                            // TODO:
                            // log
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
                    try channel.sender.submit(channel.msg, .{ .flags = .{.nonblocking = true} });
                }

                _ = try self.poller.poll(Self.doPoll, .{ .force_concurrent = options.force_concurrent });
                try self.queue.entrySkipped(skip_entries.items);
            }
        }

        fn doPoll(poller: *ReceivePoller, results: []const nnng.PollEvent) !void {
            const self: *Self = @alignCast(@fieldParentPtr("poller", poller));
            try (self.on_poll)(&self.queue, results);
        }

        // TODO:
        // pub fn reply(self: *Self, socket: *zmq.ZSocket, event: events.Event, routing_id: ?types.Symbol) !void {
        //     try self.send_queue.prepend(.{
        //         .allocator = self.allocator,
        //         .kind = .reply,
        //         .socket = socket,
        //         .from = try self.allocator.dupe(u8, stage_name),
        //         .event = event,
        //         .routing_id = if (routing_id) |x| try self.allocator.dupe(u8, x) else null,
        //     });
        // }

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

        // pub fn tryReadyQuit(self: *Self, event: events.Event) !void {
        //     if (event.tag() == .quit) {
        //         try self.approve();
        //         try self.state.readyQuit();
        //     }
        //     else if (event.tag() == .quit_all) {
        //         try self.state.readyQuit();
        //     }
        // }

        // pub fn quitAccept(self: *Self) !void {
        //     try self.send_queue.prepend(.{
        //         .allocator = self.allocator,
        //         .kind = .post,
        //         .socket = self.send_socket,
        //         .from = try self.allocator.dupe(u8, stage_name),
        //         .event = .quit_accept,
        //         .routing_id = null,
        //     });
        // }

        // pub fn approve(self: *Self) !void {
        //     if (self.receive_pending.dequeue()) |*prev| {
        //         defer prev.deinit();

        //         if (prev.event.tag() == .quit_accept) {
        //             try self.state.done();
        //         }
        //     }
        // }

        // pub fn revertFromPending(self: *Self) !void {
        //     if (self.receive_pending.dequeue()) |entry| {
        //         try self.send_queue.prepend(entry);
        //     }
        // }

        // pub fn isReady(self: *Self) bool {
        //     if (self.receive_queue.hasMore()) return true;
        //     if (self.send_queue.hasMore()) return true;

        //     return ! self.state.level.done;
        // }
    };
}

pub const Phase = enum { booting, request, ready, terminating, quitting };
pub const DirtyState = enum { none, skipped, unhandled };

pub const QuitHandler = struct {
    ptr: *anyopaque,
    handler: *const fn (ptr: *anyopaque) anyerror!void,
};

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

            fn run(self: *const @This()) void {
                self.d.run(self.callback, .{ .force_concurrent = true }) catch |err| {
                    const msg = std.fmt.allocPrint(std.testing.allocator, "Unhandled dispatch error: {}", .{err}) catch @panic("OOM");
                    defer std.testing.allocator.free(msg);
                    @panic(msg);
                };
            }
        };

        var group: std.Io.Group = .init;
        try group.concurrent(std.testing.io, Runnable.run, .{ &Runnable{ .d = host_dispatcher, .callback = serverHandler } });
        try group.concurrent(std.testing.io, Runnable.run, .{ &Runnable{ .d = guest_dispatcher, .callback = clientHandler } });
        return group.await(std.testing.io);
    }

    test "pull event by PULL" {
        var tmp_dir = try supports.createTmpDir();
        defer tmp_dir.cleanup();

        const ep = try supports.createEndpoint(tmp_dir.dir);
        defer supports.releaseEndpoint(std.testing.allocator, ep);

        var conn = try ServerConnection.create(std.testing.io, std.testing.allocator, 4, ep);
        defer conn.deinit();
        try conn.bind();

        var dispatcher: Dispatcher = try conn.configureDispatcher(8);
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

        try dispatcher.run(serverHandler, .{});

        try std.testing.expectEqual(.quitting, dispatcher.phase);
    }

    test "receive event by REP" {
        var tmp_dir = try supports.createTmpDir();
        defer tmp_dir.cleanup();

        const ep = try supports.createEndpoint(tmp_dir.dir);
        defer supports.releaseEndpoint(std.testing.allocator, ep);

        var conn = try ServerConnection.create(std.testing.io, std.testing.allocator, 4, ep);
        defer conn.deinit();
        try conn.bind();

        var dispatcher: Dispatcher = try conn.configureDispatcher(8);
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

        try dispatcher.run(serverHandler, .{});

        try std.testing.expectEqual(.quitting, dispatcher.phase);
    }

    test "receive event by SUB" {
        var tmp_dir = try supports.createTmpDir();
        defer tmp_dir.cleanup();

        const ep = try supports.createEndpoint(tmp_dir.dir);
        defer supports.releaseEndpoint(std.testing.allocator, ep);

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

        try guest.cmd_socket.transport.start(.{});
        try host.cmd_socket.transport.start(.{});

        var dispatcher: Dispatcher = try guest.configureDispatcher(8);
        defer dispatcher.deinit();
        dispatcher.on_quit = null;

        var msg = try nnng.Message.create();
        send_event: {
            try encodeToCbor(&msg.writer, .{ .header = .quit_all, .stage_name = "test-stage", .event = .quit_all });
            try host.cmd_socket.pipe.item.sender().submit(msg, .{});
            break:send_event;
        }

        try std.testing.expectEqual(.booting, dispatcher.phase);

        try dispatcher.run(clientHandler, .{});

        try std.testing.expectEqual(.quitting, dispatcher.phase);
    }

    test "Host/Guest communication" {
        var tmp_dir = try supports.createTmpDir();
        defer tmp_dir.cleanup();

        const ep = try supports.createEndpoint(tmp_dir.dir);
        defer supports.releaseEndpoint(std.testing.allocator, ep);

        var host = try root.sockets.Connection.Server("runner#2").create(std.testing.io, std.testing.allocator, 4, ep);
        // var host = try ServerConnection.create(std.testing.io, std.testing.allocator, ep);
        defer host.deinit();

        var guest = try ClientConnection.create(std.testing.io, std.testing.allocator, ep);
        defer guest.deinit();
        try guest.subscribe(&.{ .quit_all });

        // listeners
        try guest.cmd_socket.transport.start(.{});
        try host.pull_socket.transport.start(.{});
        try host.reply_socket.transport.start(.{});
        // dialers
        try guest.push_socket.transport.start(.{});
        try guest.req_socket.transport.start(.{});
        try host.cmd_socket.transport.start(.{});

        var host_dispatcher: Dispatcher = try host.configureDispatcher(8);
        defer host_dispatcher.deinit();

        var guest_dispatcher: Dispatcher = try guest.configureDispatcher(8);
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
