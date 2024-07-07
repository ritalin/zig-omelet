//! Client-side socket (send only)
//! 
//! const context = try ZContext.init(allocator);
//! conts socket = try ClientConnection.init(&context);
//! try socket.connect();
//! 
const std = @import("std");
const zmq = @import("zmq");

const types = @import("../types.zig");
const helpers = @import("../helpers.zig");
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
        dispatcher: *EventDispatcher,

        pub fn init(allocator: std.mem.Allocator, context: *zmq.ZContext) !*Self {
            const request_socket = try zmq.ZSocket.init(zmq.ZSocketType.Req, context);
            const subscribe_socket = try SubscribeSocket.init(allocator, context);
            const pull_sink_socket = try PullSinkSocket.Worker(WorkerType).init(allocator, context);

            const self = try allocator.create(Self);
            self.* = .{
                .allocator = allocator,
                .request_socket = request_socket,
                .subscribe_socket = subscribe_socket,
                .pull_sink_socket = pull_sink_socket,
                .dispatcher = try EventDispatcher.init(
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

        fn onDispatch(dispatcher: *EventDispatcher) !?EventDispatcher.Entry {
            while (true) {
                while (dispatcher.receive_queue.dequeue()) |*entry| {
                    switch (entry.event) {
                        .ack => {
                            defer entry.deinit();
                            Trace.debug("Received 'ack'", .{});
                            try dispatcher.approve();
                        },
                        .nack => {
                            defer entry.deinit();
                            try dispatcher.revertFromPending();
                        },
                        else => {
                            Trace.debug("Received command: {} ({})", .{std.meta.activeTag(entry.event), dispatcher.receive_queue.count()});

                            if (entry.event.tag() == .quit) {
                                try dispatcher.approve();
                                try dispatcher.state.readyQuit();
                            }
                            else if (entry.event.tag() == .quit_all) {
                                // TODO terminate worker thread
                                try dispatcher.state.readyQuit();
                            }

                            return .{ 
                                .socket = entry.socket, 
                                .kind = .post,
                                .event = entry.event
                            };
                        }
                    }
                }

                if (!dispatcher.receive_pending.hasMore()) {
                    if (dispatcher.send_queue.dequeue()) |entry| {
                        if (dispatcher.state.level.done) {
                            defer entry.deinit();
                            continue;
                        }

                        if (entry.event.tag() == .quit_accept) {
                            try dispatcher.state.done();
                        }
                        Trace.debug("Sending: {} ({})", .{std.meta.activeTag(entry.event), dispatcher.send_queue.count()});
                        try dispatcher.receive_pending.enqueue(entry);

                        helpers.sendEvent(dispatcher.allocator, entry.socket, entry.event) catch |err| switch (err) {
                            else => {
                                Trace.debug("Unexpected error on sending: {}", .{err});
                                return err;
                            }
                        };
                    }
                    else if (dispatcher.receive_queue.hasMore()) {
                        continue;
                    }
                    else if (dispatcher.state.level.done) {
                        break;
                    }
                }

                var it = try dispatcher.polling.poll();
                defer it.deinit();

                while (it.next()) |item| {
                    const event = try helpers.receiveEventWithPayload(dispatcher.allocator, item.socket);
                    defer event.deinit();

                    try dispatcher.receive_queue.enqueue(.{
                        .kind = .response,
                        .socket = item.socket, .event = try event.clone(dispatcher.allocator) 
                    });
                }
            }

            return null;
        }
    };
}

pub const Server = struct {
    allocator: std.mem.Allocator,
    send_socket: *zmq.ZSocket,
    reply_socket: *zmq.ZSocket,
    dispatcher: *EventDispatcher,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, context: *zmq.ZContext) !*Self {
        const send_socket = try zmq.ZSocket.init(zmq.ZSocketType.Pub, context);
        const reply_socket = try zmq.ZSocket.init(zmq.ZSocketType.Rep, context);

        const self = try allocator.create(Self);
        self.* = .{
            .allocator = allocator,
            .send_socket = send_socket,
            .reply_socket = reply_socket,
            .dispatcher = try EventDispatcher.init(allocator, send_socket, &.{reply_socket}, onDispatch),
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

    fn onDispatch(dispatcher: *EventDispatcher) !?EventDispatcher.Entry {
        while (true) {
            while (dispatcher.receive_queue.dequeue()) |entry| {
                return entry;
            }

            if (dispatcher.send_queue.dequeue()) |*entry| {
                defer entry.deinit();

                helpers.sendEvent(dispatcher.allocator, entry.socket, entry.event) catch |err| switch (err) {
                    else => {
                        // Logger.Server.traceLog.debug("Unexpected error on sending: {any}", .{err});
                        return err;
                    }
                };

                if (entry.kind == .reply) {
                    continue;
                }
            }
            else if (dispatcher.receive_queue.hasMore()) {
                continue;
            }
            else if (dispatcher.state.level.done) {
                break;
            }

            var it = try dispatcher.polling.poll();
            defer it.deinit();

            while (it.next()) |item| {
                const event = helpers.receiveEventWithPayload(dispatcher.allocator, item.socket) catch |err| switch (err) {
                    // error.InvalidResponse => {
                    //     try helpers.sendEvent(dispatcher.allocator, item.socket, .nack);
                    //     continue;
                    // },
                    else => return err,
                };
                defer event.deinit();

                try dispatcher.receive_queue.enqueue(.{
                    .kind = .response,
                    .socket = item.socket, .event = try event.clone(dispatcher.allocator) 
                });
            }
        }

        return null;     
    }
};

pub const EventDispatcher = struct {
    const DispatchFn = *const fn (dispatcher: *EventDispatcher) anyerror!?Entry;

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

        pub fn ready(self: *State) !void {
            self.level.ready = true;
        }
        pub fn receiveTerminate(self: *State) !void {
            self.level.terminating = true;
        }
        pub fn readyQuit(self: *State) !void {
            self.level.quitting = true;
        }
        pub fn done(self: *State) !void {
            self.level.done = true;
        }
    };

    pub fn init(allocator: std.mem.Allocator, send_socket: *zmq.ZSocket, receive_sockets: []const *zmq.ZSocket, on_dispatch: DispatchFn) !*EventDispatcher {
        const polling_sockets = try allocator.alloc(zmq.ZPolling.Item, receive_sockets.len);
        defer allocator.free(polling_sockets);

        for (receive_sockets, 0..) |socket, i| {
            polling_sockets[i] = zmq.ZPolling.Item.fromSocket(socket, .{ .PollIn = true });
        }

        const self = try allocator.create(EventDispatcher);
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

    pub fn deinit(self: *EventDispatcher) void {
        self.send_queue.deinit();
        self.receive_queue.deinit();
        self.receive_pending.deinit();
        self.polling.deinit();
        self.allocator.destroy(self);
    }

    pub fn post(self: *EventDispatcher, event: types.Event) !void {
        try self.send_queue.enqueue(.{ 
            .kind = .post,
            .socket = self.send_socket, .event = event
        });
    }

    pub fn reply(self: *EventDispatcher, socket: *zmq.ZSocket, event: types.Event) !void {
        try self.send_queue.enqueue(.{ 
            .kind = .reply,
            .socket = socket, .event = event
        });
    }

    pub fn delay(self: *EventDispatcher, socket: *zmq.ZSocket, event: types.Event) !void {
        try self.receive_queue.revert(.{
            .kind = .response,
            .socket = socket, .event = event
        });
    }

    pub fn approve(self: *EventDispatcher) !void {
        if (self.receive_pending.dequeue()) |*prev| {
            prev.deinit();
        }
    }

    pub fn revertFromPending(self: *EventDispatcher) !void {
        if (self.receive_pending.dequeue()) |entry| {
            try self.send_queue.revert(entry);
        }
    }

    pub fn isReady(self: *EventDispatcher) bool {
        if (self.receive_queue.hasMore()) return true;
        if (self.send_queue.hasMore()) return true;

        return ! self.state.level.done;
    }

    pub fn dispatch(self: *EventDispatcher) !?Entry {
        return self.on_dispatch(self);
    }

    pub const Entry = struct {
        socket: *zmq.ZSocket,
        kind: enum { post, reply, response},
        event: types.Event,

        pub fn deinit(self: @This()) void {
            self.event.deinit();
        }
    };
};