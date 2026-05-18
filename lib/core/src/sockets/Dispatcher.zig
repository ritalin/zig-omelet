const std = @import("std");
const nnng = @import("nnng");
const root = @import("../root.zig");

const types = root.types;
const sockets = root.sockets;

pub fn EventDispatcher(comptime poller_size: comptime_int) type {
    return struct {
        allocator: std.mem.Allocator,
        send_queue: std.Deque(sockets.SendChannel),
        receive_queue: std.Deque(sockets.ReceiveEntry),
        poller: ReceivePoller,
        phase: Phase,
        on_poll: PollFn,

        const Self = @This();

        pub const ReceivePoller = nnng.ReceivePoller(poller_size);
        pub const PollFn = *const fn (dispatcher: *Self, results: []const nnng.PollEvent) anyerror!void;
        pub const DispatchFn = *const fn (dispatcher: *Self, channel: sockets.ReceiveEntry, dirty: *DirtyState) anyerror!void;

        pub const Phase = enum { booting, request, ready, terminating, quitting };
        pub const DirtyState = enum { none, skipped };

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
            // const polling_sockets = try allocator.alloc(zmq.ZPolling.Item, receive_sockets.len);
            // defer allocator.free(polling_sockets);

            // for (receive_sockets, 0..) |socket, i| {
            //     polling_sockets[i] = zmq.ZPolling.Item.fromSocket(socket, .{ .PollIn = true });
            // }

            return .{
                .allocator = context.allocator,
                .send_queue = .empty,
                .receive_queue = .empty,
                .poller = try ReceivePoller.create(context),
                .phase = .booting,
                .on_poll = on_poll,
            };
        }

        pub fn deinit(self: *Self) void {
            self.send_queue.deinit(self.allocator);
            self.receive_queue.deinit(self.allocator);
            self.poller.deinit();
        }

        pub fn run(self: *Self, on_dispatch: DispatchFn) !void {
            while (true) {
                var skip_entries: std.ArrayListUnmanaged(sockets.ReceiveEntry) = .empty;
                defer skip_entries.deinit(self.allocator);

                while (self.receive_queue.popFront()) |e| {
                    var entry = e;
                    var dirty: Self.DirtyState = .none;
                    try on_dispatch(self, entry, &dirty);

                    switch (dirty) {
                        .none => {
                            entry.deinit(self.allocator);

                            if (entry.features.last_msg_owner) {
                            }
                        },
                        .skipped => {
                            try skip_entries.append(self.allocator, entry);
                        }
                    }
                }
                if (self.phase == .quitting) {
                    break;
                }

                // TODO: send event

                _ = try self.poller.poll(Self.doPoll);
                try self.receive_queue.pushBackSlice(self.allocator, skip_entries.items);
            }
        }

        fn doPoll(poller: *ReceivePoller, results: []const Self.ReceivePoller.WakeupResult) !void {
            const self: *Self = @fieldParentPtr("poller", poller);
            try (self.on_poll)(self, results);
        }

        pub fn post(self: *Self, channel: sockets.SendChannel) !void {
            try self.send_queue.pushBack(self.allocator, channel);
        }

        pub fn pushReceiveQueue(self: *Self, entry: sockets.ReceiveEntry) !void {
            try self.receive_queue.pushBack(self.allocator, entry);
        }

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

        pub fn postPriority(self: *Self, channel: sockets.SendChannel) !void {
            try self.send_queue.pushFront(self.allocator, channel);
        }

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

pub const Queue = struct {
    
};

test "dispatcher test" {
    std.testing.refAllDecls(@This());
}

pub const tests = struct {
    const supports = @import("../supports/test_support.zig");
    const encodeToCbor = @import("../events/encoder.zig").encodeToCbor;

    const ReceiveEntry = root.sockets.ReceiveEntry;
    const ServerConnection = root.sockets.Connection.Server("runner", 8);
    const Dispatcher = ServerConnection.EventDispatcher;

    fn serverHandler(d: *Dispatcher, entry: ReceiveEntry, _: *Dispatcher.DirtyState) !void {
        switch (entry.event) {
            .quit => {
                d.phase = .quitting;
            },
            else => unreachable,
        }
    }

    fn clientHandler(d: *Dispatcher, entry: ReceiveEntry, _: *Dispatcher.DirtyState) !void {
        switch (entry.event) {
            .request_quit => {
                d.phase = .quitting;
            },
            else => unreachable,
        }
    }

    test "pull event by PULL" {
        var tmp_dir = try supports.createTmpDir();
        defer tmp_dir.cleanup();

        const ep = try supports.createEndpoint(tmp_dir.dir);
        defer supports.releaseEndpoint(std.testing.allocator, ep);

        var conn = try ServerConnection.create(std.testing.io, std.testing.allocator, ep);
        defer conn.deinit();
        try conn.bind();

        var dispatcher = try conn.createEventDispatcher();
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

        try dispatcher.run(serverHandler);

        try std.testing.expectEqual(.quitting, dispatcher.phase);
    }

    test "receive event by REP" {
        var tmp_dir = try supports.createTmpDir();
        defer tmp_dir.cleanup();

        const ep = try supports.createEndpoint(tmp_dir.dir);
        defer supports.releaseEndpoint(std.testing.allocator, ep);

        var conn = try ServerConnection.create(std.testing.io, std.testing.allocator, ep);
        defer conn.deinit();
        try conn.bind();

        var dispatcher = try conn.createEventDispatcher();
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

        try dispatcher.run(serverHandler);

        try std.testing.expectEqual(.quitting, dispatcher.phase);
    }

    test "receive event by SUB" {
        var tmp_dir = try supports.createTmpDir();
        defer tmp_dir.cleanup();

        const ep = try supports.createEndpoint(tmp_dir.dir);
        defer supports.releaseEndpoint(std.testing.allocator, ep);

        var guest = try ClientConnection.create(std.testing.io, std.testing.allocator, ep);
        defer guest.deinit();
        guest.subscribe(&.{ .request_quit });
        try guest.bind();

        var dispatcher = try guest.createEventDispatcher();
        defer dispatcher.deinit();

        var pub_socket = socket: {
            const b = try nnng.Pub.open(conn.context);
            break:socket try b.as_dialer(ep.pub_sub);
        };
        try pub_socket.transport.start(.{ .nonblocking = true });
        defer pub_socket.close();

        var msg = try nnng.Message.create();
        send_event: {
            try encodeToCbor(&msg.writer, .{ .header = .quit, .stage_name = "test-stage", .event = .request_quit });
            try pipe.sender().submit(msg, .{});
            break:send_event;
        }

        try std.testing.expectEqual(.booting, dispatcher.phase);

        try dispatcher.run(clientHandler);

        try std.testing.expectEqual(.quitting, dispatcher.phase);
    }

    test "Host/Guest communication" {
        var tmp_dir = try supports.createTmpDir();
        defer tmp_dir.cleanup();

        const ep = try supports.createEndpoint(tmp_dir.dir);
        defer supports.releaseEndpoint(std.testing.allocator, ep);

        var host = try ServerConnection.create(std.testing.io, std.testing.allocator, ep);
        defer host.deinit();
        try host.bind();

        var host_dispatcher = try host.createEventDispatcher();
        defer host_dispatcher.deinit();

        var guest = try ClientConnection.create(std.testing.io, std.testing.allocator, ep);
        defer guest.deinit();
        guest.subscribe(&.{ .request_quit });
        try guest.bind();

        var guest_dispatcher = try guest.createEventDispatcher();
        defer guest_dispatcher.deinit();

        publisg_event: {
            var cmd = try host.createCommandChannel();
            cmd.encode(.request_quit);
            host_dispatcher.post(cmd);
            break:publisg_event;
        }

        var g: std.Io.Group = .init;
        g.concurrent(std.testing.io, Dispatcher.run, .{ host_dispatcher })
    }
};
