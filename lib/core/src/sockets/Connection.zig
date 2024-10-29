//! Client-side socket (send only)
//! 
//! const context = try ZContext.init(allocator);
//! conts socket = try ClientConnection.init(&context);
//! try socket.connect();
//! 
const std = @import("std");
const zmq = @import("zmq");

const types = @import("../types.zig");
const events = @import("../events/events.zig");
const SubscribeSocket = @import("./SubscribeSocket.zig");
const PullSinkSocket = @import("./PullSinkSocket.zig");
const Logger = @import("../Logger.zig");
const EventQueue = @import("../Queue.zig").Queue;

pub fn Client(comptime stage_name: types.Symbol, comptime WorkerType: type) type {
    const Trace = Logger.TraceDirect(stage_name);

    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        request_socket: *zmq.ZSocket,
        subscribe_socket: *SubscribeSocket,
        pull_sink_socket: *PullSinkSocket.Worker(WorkerType),
        dispatcher: *EventDispatcher(stage_name),

        pub fn init(allocator: std.mem.Allocator, context: *zmq.ZContext) !*Self {
            var request_socket = try zmq.ZSocket.init(zmq.ZSocketType.Dealer, context);
            // var request_socket = try zmq.ZSocket.init(zmq.ZSocketType.Req, context);
            request_socket_opt: {
                const opt:zmq.ZSocketOption = .{.RoutingId = @constCast(stage_name)}; 
                try request_socket.setSocketOption(opt);
                break:request_socket_opt;
            }

            const subscribe_socket = try SubscribeSocket.init(allocator, context);
            const pull_sink_socket = try PullSinkSocket.Worker(WorkerType).init(allocator, context);

            const self = try allocator.create(Self);
            self.* = .{
                .allocator = allocator,
                .request_socket = request_socket,
                .subscribe_socket = subscribe_socket,
                .pull_sink_socket = pull_sink_socket,
                .dispatcher = try EventDispatcher(stage_name).init(
                    allocator, request_socket, 
                    &.{request_socket, subscribe_socket.socket, pull_sink_socket.socket},
                    onDispatch
                ),
            };

            return self;
        }

        pub fn deinit(self: *Self) void {
            self.dispatcher.deinit();

            self.request_socket.deinit();
            self.subscribe_socket.deinit();
            
            self.pull_sink_socket.deinit();

            self.allocator.destroy(self);
        }

        /// Connect owned sockets
        pub fn connect(self: Self, endpoints: types.Endpoints) !void {
            try self.request_socket.connect(endpoints.req_rep);
            try self.subscribe_socket.connect(endpoints.pub_sub);
            try self.pull_sink_socket.connect();
        }

        fn onDispatch(dispatcher: *EventDispatcher(stage_name)) !?EventDispatcher(stage_name).Entry {
            while (true) {
                dispatch_loop: {
                    while (dispatcher.receive_queue.dequeue()) |*entry| {
                        if (dispatcher.state.level.done) {
                            continue;
                        }
                        else {
                            try dispatcher.tryReadyQuit(entry.event);
                        }

                        return .{ 
                            .allocator = entry.allocator,
                            .socket = entry.socket, 
                            .kind = .post,
                            .from = entry.from,
                            .event = entry.event,
                            .routing_id = entry.routing_id,
                        };
                    }

                    while (true) {
                        send_event: {
                        if (dispatcher.send_queue.dequeue()) |entry| {
                            const socket_state = try dispatcher.polling.socketState(entry.socket);
                            if (socket_state.PollIn) {
                                try dispatcher.send_queue.prepend(entry);
                            }
                            else {
                                if (dispatcher.state.level.done) {
                                    defer entry.deinit();
                                    continue;
                                }

                                Trace.debug("Sending: {} ({})", .{std.meta.activeTag(entry.event), dispatcher.send_queue.count()});
                                try dispatcher.receive_pending.enqueue(entry);

                                events.sendEvent(
                                    dispatcher.allocator, entry.socket, 
                                    .{ .kind = entry.kind, .from = stage_name, .event = entry.event }
                                ) 
                                catch |err| switch (err) {
                                    error.SocketStateInvalid => {
                                        Trace.debug("Cannot send REQ -> REQ", .{});
                                        try dispatcher.revertFromPending();
                                    },
                                    else => {
                                        Trace.debug("Unexpected error on sending: {}", .{err});
                                        return err;
                                    }
                                };
                            }
                        }
                        else if (dispatcher.receive_queue.hasMore()) {
                            break:dispatch_loop;
                        }
                        else if (dispatcher.state.level.done) {
                            return null;
                        }

                        while (true) {
                            var it = try dispatcher.polling.poll();
                            defer it.deinit();

                            while (it.next()) |item| {
                                // discard routing id
                                _ = try events.receiveRoutingId(dispatcher.allocator, item.socket);
                                
                                const packet = try events.receiveEventWithPayload(dispatcher.allocator, item.socket);

                                Trace.debug("Received command: {} ({})", .{packet.event.tag(), dispatcher.receive_queue.count()});

                                if (packet.kind == .reply) {
                                    switch (packet.event) {
                                        .nack => {
                                            try dispatcher.revertFromPending();
                                        },
                                        else => {
                                            try dispatcher.approve();
                                        }
                                    }
                                }

                                switch (packet.event) {
                                    .ack, .nack => {
                                        defer packet.deinit(dispatcher.allocator);
                                        continue;
                                    },
                                    else => {
                                        try dispatcher.receive_queue.enqueue(.{
                                            .allocator = dispatcher.allocator, 
                                            .kind = .response,
                                            .from = packet.from,
                                            .socket = item.socket, 
                                            .event = packet.event,
                                            .routing_id = null,
                                        });
                                    }
                                }
                            }

                            if (dispatcher.receive_pending.count() == 0) break:send_event;
                        }
                    }
                }
            }
            }

            return null;
        }
    };
}

pub fn Server(comptime stage_name: types.Symbol) type {
    const Trace = Logger.TraceDirect(stage_name);

    return struct {
        allocator: std.mem.Allocator,
        send_socket: *zmq.ZSocket,
        reply_socket: *zmq.ZSocket,
        dispatcher: *EventDispatcher(stage_name),

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator, context: *zmq.ZContext) !*Self {
            const send_socket = try zmq.ZSocket.init(zmq.ZSocketType.Pub, context);
            errdefer send_socket.deinit();
            const reply_socket = try zmq.ZSocket.init(zmq.ZSocketType.Router, context);
            // const reply_socket = try zmq.ZSocket.init(zmq.ZSocketType.Rep, context);
            errdefer reply_socket.deinit();

            const self = try allocator.create(Self);
            errdefer self.deinit();

            self.* = .{
                .allocator = allocator,
                .send_socket = send_socket,
                .reply_socket = reply_socket,
                .dispatcher = try EventDispatcher(stage_name).init(allocator, send_socket, &.{reply_socket}, onDispatch),
            };

            return self;
        }

        pub fn deinit(self: *Self) void {
            self.reply_socket.deinit();
            self.send_socket.deinit();
            self.dispatcher.deinit();     
            self.allocator.destroy(self);
        }

        pub fn bind(self: *Self, endpoints: types.Endpoints) !void {
            try self.send_socket.bind(endpoints.pub_sub);
            try self.reply_socket.bind(endpoints.req_rep);
        }

        fn onDispatch(dispatcher: *EventDispatcher(stage_name)) !?EventDispatcher(stage_name).Entry {
            while (true) {
                while (dispatcher.receive_queue.dequeue()) |entry| {
                    return entry;
                }

                send_event: {
                    while (dispatcher.send_queue.dequeue()) |entry| {
                        defer entry.deinit();

                        Trace.debug("{s}: {} from [{s}] to [{s}] ({}) ", .{
                            if (entry.kind == .reply) "Reply" else "Post",
                            std.meta.activeTag(entry.event), 
                            entry.from,
                            if (entry.routing_id) |routing_id| routing_id else "all",
                            dispatcher.send_queue.count()
                        });

                        if (entry.routing_id) |routing_id| {
                            try events.sendRoutingId(dispatcher.allocator, entry.socket, routing_id);
                        }

                        events.sendEvent(
                            dispatcher.allocator, entry.socket, 
                            .{ .kind = entry.kind, .from = stage_name, .event = entry.event }
                        ) 
                        catch |err| switch (err) {
                            else => {
                                // Logger.Server.traceLog.debug("Unexpected error on sending: {any}", .{err});
                                return err;
                            }
                        };
                    }
                    else if (dispatcher.state.level.done) {
                        return null;
                    }
                    break:send_event;
                }

                receive_event: {
                    while (true) {
                        var it = try dispatcher.polling.poll();
                        defer it.deinit();

                        while (it.next()) |item| {
                            const routing_id = try events.receiveRoutingId(dispatcher.allocator, item.socket);

                            const packet = events.receiveEventWithPayload(dispatcher.allocator, item.socket) catch |err| switch (err) {
                                // error.InvalidResponse => {
                                //     try events.sendEvent(dispatcher.allocator, item.socket, .nack);
                                //     continue;
                                // },
                                else => return err,
                            };

                            Trace.debug("Received command: {} from [{s}]", .{
                                packet.event.tag(), 
                                std.mem.sliceTo(packet.from, 0),
                            });

                            try dispatcher.receive_queue.enqueue(.{
                                .allocator = dispatcher.allocator,
                                .kind = .response,
                                .socket = item.socket, 
                                .from = packet.from,
                                .event = packet.event,
                                .routing_id = routing_id,
                            });
                        }

                        if (dispatcher.receive_queue.count() > 0) break:receive_event;
                    }
                }
            }

            return null;
        }
    };
}

pub fn EventDispatcher(comptime stage_name: types.Symbol) type {
    return struct {
        const DispatchFn = *const fn (dispatcher: *Self) anyerror!?Entry;
        const Self = @This();

        allocator: std.mem.Allocator,
        send_queue: EventQueue(Entry),
        receive_queue: EventQueue(Entry),
        receive_pending: EventQueue(Entry),
        polling: zmq.ZPolling,
        send_socket: *zmq.ZSocket,
        on_dispatch: DispatchFn,
        state: State,

        pub const State = struct {
            level: std.enums.EnumFieldStruct(enum {booting, ready, terminating, quitting, done}, bool, false),

            pub inline fn ready(self: *State) !void {
                self.level.ready = true;
            }
            pub inline fn receiveTerminate(self: *State) !void {
                try self.ready();
                self.level.terminating = true;
            }
            pub inline fn readyQuit(self: *State) !void {
                try self.receiveTerminate();
                self.level.quitting = true;
            }
            pub inline fn done(self: *State) !void {
                try self.readyQuit();
                self.level.done = true;
            }
        };

        pub fn init(allocator: std.mem.Allocator, send_socket: *zmq.ZSocket, receive_sockets: []const *zmq.ZSocket, on_dispatch: DispatchFn) !*Self {
            const polling_sockets = try allocator.alloc(zmq.ZPolling.Item, receive_sockets.len);
            defer allocator.free(polling_sockets);

            for (receive_sockets, 0..) |socket, i| {
                polling_sockets[i] = zmq.ZPolling.Item.fromSocket(socket, .{ .PollIn = true });
            }

            const self = try allocator.create(Self);
            errdefer self.deinit();
            
            self.* = .{
                .allocator = allocator,
                .send_queue = EventQueue(Entry).init(allocator),
                .receive_queue = EventQueue(Entry).init(allocator),
                .receive_pending = EventQueue(Entry).init(allocator),
                .polling = try zmq.ZPolling.init(allocator, polling_sockets, .{}), 
                .send_socket = send_socket, 
                .on_dispatch = on_dispatch,
                .state = .{ .level = .{.booting = true} },
            };

            return self;
        }

        pub fn deinit(self: *Self) void {
            self.send_queue.deinit();
            self.receive_queue.deinit();
            self.receive_pending.deinit();
            self.polling.deinit();
            self.allocator.destroy(self);
        }

        pub fn post(self: *Self, event: events.Event) !void {
            try self.send_queue.enqueue(.{ 
                .allocator = self.allocator,
                .kind = .post,
                .socket = self.send_socket, 
                .from = try self.allocator.dupe(u8, stage_name), 
                .event = event,
                .routing_id = null,
            });
        }

        pub fn reply(self: *Self, socket: *zmq.ZSocket, event: events.Event, routing_id: ?types.Symbol) !void {
            try self.send_queue.prepend(.{ 
                .allocator = self.allocator,
                .kind = .reply,
                .socket = socket, 
                .from = try self.allocator.dupe(u8, stage_name), 
                .event = event,
                .routing_id = if (routing_id) |x| try self.allocator.dupe(u8, x) else null,
            });
        }

        pub fn delay(self: *Self, socket: *zmq.ZSocket, from: types.Symbol, event: events.Event, routing_id: ?types.Symbol) !void {
            try self.receive_queue.prepend(.{
                .allocator = self.allocator,
                .kind = .response,
                .socket = socket, 
                .from = try self.allocator.dupe(u8, from), 
                .event = try event.clone(self.allocator),
                .routing_id = if (routing_id) |x| try self.allocator.dupe(u8, x) else null,
            });
        }

        pub fn postFatal(self: *Self, stack_trace: ?*std.builtin.StackTrace) !void {
            const message = err_message: {
                if (stack_trace) |x| {
                    var buf = std.ArrayList(u8).init(self.allocator);
                    defer buf.deinit();

                    var writer = buf.writer();
                    try writer.print("{}", .{x});
                    
                    break:err_message try buf.toOwnedSlice();
                }
                else {
                    break:err_message try self.allocator.dupe(u8, "Fatal eerror occured");
                }
            };
            defer self.allocator.free(message);

            try self.send_queue.prepend(.{
                .allocator = self.allocator,
                .kind = .post,
                .socket = self.send_socket, 
                .from = try self.allocator.dupe(u8, stage_name),
                .event = .{.report_fatal = try events.Event.Payload.Log.init(self.allocator, .{.err, message})},
                .routing_id = null,
            });
        }

        pub fn tryReadyQuit(self: *Self, event: events.Event) !void {
            if (event.tag() == .quit) {
                try self.approve();
                try self.state.readyQuit();
            }
            else if (event.tag() == .quit_all) {
                try self.state.readyQuit();
            }
        }

        pub fn quitAccept(self: *Self) !void {
            try self.send_queue.prepend(.{
                .allocator = self.allocator,
                .kind = .post,
                .socket = self.send_socket, 
                .from = try self.allocator.dupe(u8, stage_name),
                .event = .quit_accept,
                .routing_id = null,
            });
        }

        pub fn approve(self: *Self) !void {
            if (self.receive_pending.dequeue()) |*prev| {
                defer prev.deinit();

                if (prev.event.tag() == .quit_accept) {
                    try self.state.done();
                }
            }
        }

        pub fn revertFromPending(self: *Self) !void {
            if (self.receive_pending.dequeue()) |entry| {
                try self.send_queue.prepend(entry);
            }
        }

        pub fn isReady(self: *Self) bool {
            if (self.receive_queue.hasMore()) return true;
            if (self.send_queue.hasMore()) return true;

            return ! self.state.level.done;
        }

        pub fn dispatch(self: *Self) !?Entry {
            return self.on_dispatch(self);
        }

        pub const Entry = struct {
            allocator: std.mem.Allocator,
            socket: *zmq.ZSocket,
            kind: events.DataPacket.Kind,
            from: types.Symbol,
            event: events.Event,
            routing_id: ?types.Symbol,

            pub fn deinit(self: @This()) void {
                self.allocator.free(self.from);
                self.event.deinit();
                if (self.routing_id) |x| self.allocator.free(x);
            }
        };
    };
}