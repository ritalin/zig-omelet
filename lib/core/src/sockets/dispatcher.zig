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
