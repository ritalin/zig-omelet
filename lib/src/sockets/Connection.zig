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
const Logger = @import("../Logger.zig");

pub const Client = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    request_socket: *zmq.ZSocket,
    subscribe_socket: *SubscribeSocket,
    dispatcher: EventDispatcher,

    pub fn init(allocator: std.mem.Allocator, context: *zmq.ZContext) !*Self {
        const request_socket = try zmq.ZSocket.init(zmq.ZSocketType.Req, context);
        const subscribe_socket = try SubscribeSocket.init(allocator, context);

        const self = try allocator.create(Self);
        self.* = .{
            .allocator = allocator,
            .request_socket = request_socket,
            .subscribe_socket = subscribe_socket,
            .dispatcher = try EventDispatcher.init(
                allocator, request_socket, 
                &.{request_socket, subscribe_socket.socket},
                onDispatch
            ),
        };

        return self;
    }

    pub fn deinit(self: *Self) void {
        self.dispatcher.deinit();
        self.request_socket.deinit();
        self.subscribe_socket.deinit();
        self.allocator.destroy(self);
        self.* = undefined;
    }

    /// Connect owned sockets
    pub fn connect(self: Self) !void {
        try self.request_socket.connect(types.REQ_C2S_END_POINT);
        try self.subscribe_socket.connect();
    }

    fn onDispatch(dispatcher: *EventDispatcher) !?EventDispatcher.Entry {
        while (true) {
            while (dispatcher.receive_queue.dequeue()) |*entry| {
                defer entry.deinit();

                switch (entry.event) {
                    .ack => {
                        Logger.Server.traceLog.debug("Received 'ack'", .{});
                        try dispatcher.approve();
                    },
                    .nack => {
                        try dispatcher.revertFromPending();
                    },
                    else => {
                        Logger.Server.traceLog.debug("Received command: {} ({})", .{std.meta.activeTag(entry.event), dispatcher.receive_queue.count()});

                        return .{ 
                            .allocator = dispatcher.allocator,
                            .socket = entry.socket, 
                            .kind = .post,
                            .event = try entry.event.clone(dispatcher.allocator) 
                        };
                    }
                }
            }

            if (!dispatcher.receive_pending.hasMore()) {
                if (dispatcher.send_queue.dequeue()) |entry| {
                    Logger.Server.traceLog.debug("Sending: {} ({})", .{std.meta.activeTag(entry.event), dispatcher.send_queue.count()});
                    try dispatcher.receive_pending.enqueue(entry);

                    helpers.sendEvent(dispatcher.allocator, entry.socket, entry.event) catch |err| switch (err) {
                        else => {
                            Logger.Server.traceLog.debug("Unexpected error on sending: {}", .{err});
                            return err;
                        }
                    };
                }
                else if (dispatcher.receive_queue.hasMore()) {
                    continue;
                }
                else if (dispatcher.state == .done) {
                    break;
                }
            }

            var it = try dispatcher.polling.poll();
            defer it.deinit();

            while (it.next()) |item| {
                const event = try helpers.receiveEventWithPayload(dispatcher.allocator, item.socket);
                defer event.deinit(dispatcher.allocator);

                try dispatcher.receive_queue.enqueue(.{
                    .allocator = dispatcher.allocator, 
                    .kind = .response,
                    .socket = item.socket, .event = try event.clone(dispatcher.allocator) 
                });
            }
        }

        return null;
    }
};

pub const Server = struct {
    allocator: std.mem.Allocator,
    send_socket: *zmq.ZSocket,
    reply_socket: *zmq.ZSocket,
    dispatcher: EventDispatcher,

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
        self.allocator.destroy(self);
        self.* = undefined;
    }

    pub fn bind(self: *Self) !void {
        try self.send_socket.bind(types.CMD_S2C_END_POINT);
        try self.reply_socket.bind(types.REQ_C2S_END_POINT);
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
                        Logger.Server.traceLog.debug("Unexpected error on sending: {}", .{err});
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
            else if (dispatcher.state == .done) {
                break;
            }

            var it = try dispatcher.polling.poll();
            defer it.deinit();

            while (it.next()) |item| {
                const event = helpers.receiveEventWithPayload(dispatcher.allocator, item.socket) catch |err| switch (err) {
                    error.InvalidResponse => {
                        try helpers.sendEvent(dispatcher.allocator, item.socket, .nack);
                        continue;
                    },
                    else => return err,
                };
                defer event.deinit(dispatcher.allocator);

                try dispatcher.receive_queue.enqueue(.{
                    .allocator = dispatcher.allocator, 
                    .kind = .response,
                    .socket = item.socket, .event = try event.clone(dispatcher.allocator) 
                });
            }
        }

        return null;     
    }
};

pub fn EventQueue(comptime Entry: type) type {
    return struct {
        allocator: std.mem.Allocator,
        queue: std.TailQueue(Entry),

        const Queue = @This();

        pub fn init(allocator: std.mem.Allocator) Queue {
            return .{
                .allocator = allocator,
                .queue = std.TailQueue(Entry){},
            };
        }

        pub fn deinit(self: *Queue, _: std.mem.Allocator) void {
            while (self.dequeue()) |*entry| {
                entry.deinit();
            }
            self.* = undefined;
        }

        pub fn enqueue(self: *Queue, entry: Entry) !void {
            const node = try self.allocator.create(std.TailQueue(Entry).Node);

            node.data = entry;
            self.queue.append(node);
        }

        pub fn dequeue(self: *Queue) ?Entry {
            if (self.queue.popFirst()) |node| {
                defer self.allocator.destroy(node);
                return node.data;
            }

            return null;
        }

        pub fn peek(self: *Queue) ?Entry {
            return if (self.queue.first) |node| node.data else null;
        }

        pub fn revert(self: *Queue, entry: Entry) !void {
            const node = try self.allocator.create(std.TailQueue(Entry).Node);

            node.data = entry;
            self.queue.prepend(node);
        }

        pub fn hasMore(self: Queue) bool {
            return self.queue.first != null;
        }

        pub fn count(self: Queue) usize {
            return self.queue.len;
        }
    };
}

pub const EventDispatcher = struct {
    const DispatchFn = *const fn (dispatcher: *EventDispatcher) anyerror!?Entry;

    allocator: std.mem.Allocator,
    send_queue: EventQueue(Entry),
    receive_queue: EventQueue(Entry),
    receive_pending: EventQueue(Entry),
    polling: zmq.ZPolling,
    send_socket: *zmq.ZSocket,
    on_dispatch: DispatchFn,
    state: enum { ready, done},

    pub fn init(allocator: std.mem.Allocator, send_socket: *zmq.ZSocket, receive_sockets: []const *zmq.ZSocket, on_dispatch: DispatchFn) !EventDispatcher {
        const polling_sockets = try allocator.alloc(zmq.ZPolling.Item, receive_sockets.len);
        defer allocator.free(polling_sockets);

        for (receive_sockets, 0..) |socket, i| {
            polling_sockets[i] = zmq.ZPolling.Item.fromSocket(socket, .{ .PollIn = true });
        }
        
        return .{
            .allocator = allocator,
            .send_queue = EventQueue(Entry).init(allocator),
            .receive_queue = EventQueue(Entry).init(allocator),
            .receive_pending = EventQueue(Entry).init(allocator),
            .polling = try zmq.ZPolling.init(allocator, polling_sockets), 
            .send_socket = send_socket, 
            .on_dispatch = on_dispatch,
            .state = .ready,
        };
    }

    pub fn deinit(self: *EventDispatcher) void {
        self.send_queue.deinit(self.allocator);
        self.receive_queue.deinit(self.allocator);
        self.receive_pending.deinit(self.allocator);
        self.polling.deinit();
        self.* = undefined;
    }

    pub fn post(self: *EventDispatcher, event: types.Event) !void {
        try self.send_queue.enqueue(.{ 
            .allocator = self.allocator, 
            .kind = .post,
            .socket = self.send_socket, .event = try event.clone(self.allocator)
        });
    }

    pub fn reply(self: *EventDispatcher, socket: *zmq.ZSocket, event: types.Event) !void {
        try self.send_queue.enqueue(.{ 
            .allocator = self.allocator, 
            .kind = .reply,
            .socket = socket, .event = try event.clone(self.allocator)
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

    pub fn done(self: *EventDispatcher) !void {
        self.state = .done;
    }

    pub fn isReady(self: *EventDispatcher) bool {
        if (self.receive_queue.hasMore()) return true;
        if (self.send_queue.hasMore()) return true;
        if (self.state == .ready) return true;

        return false;
    }

    pub fn dispatch(self: *EventDispatcher) !?Entry {
        return self.on_dispatch(self);
    }

    pub const Entry = struct {
        allocator: std.mem.Allocator,
        socket: *zmq.ZSocket,
        kind: enum { post, reply, response },
        event: types.Event,

        pub fn deinit(self: @This()) void {
            self.event.deinit(self.allocator);
        }
    };
};