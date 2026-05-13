const std = @import("std");
const root = @import("../../root.zig");

const types = root.types;
const EventDispatcher = root.socket.EventDispatcher;

pub fn Server(comptime stage_name: types.Symbol, comptime WorkerType: type) type {
    const Trace = Logger.TraceDirect(stage_name);

    return struct {
        allocator: std.mem.Allocator,
        send_socket: *zmq.ZSocket,
        reply_socket: *zmq.ZSocket,
        pull_sink_socket: *PullSinkSocket.Worker(WorkerType),
        dispatcher: *EventDispatcher(stage_name),

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator, context: *zmq.ZContext) !*Self {
            const send_socket = try zmq.ZSocket.init(zmq.ZSocketType.Pub, context);
            errdefer send_socket.deinit();
            const reply_socket = try zmq.ZSocket.init(zmq.ZSocketType.Router, context);
            errdefer reply_socket.deinit();

            const pull_sink_socket = try PullSinkSocket.Worker(WorkerType).init(allocator, context);
            errdefer pull_sink_socket.deinit();

            const self = try allocator.create(Self);
            errdefer self.deinit();

            self.* = .{
                .allocator = allocator,
                .send_socket = send_socket,
                .reply_socket = reply_socket,
                .pull_sink_socket = pull_sink_socket,
                .dispatcher = try EventDispatcher(stage_name).init(
                    allocator, send_socket,
                    &.{reply_socket, pull_sink_socket.socket},
                    onDispatch
                ),
            };

            return self;
        }

        pub fn deinit(self: *Self) void {
            self.reply_socket.deinit();
            self.send_socket.deinit();
            self.pull_sink_socket.deinit();
            self.dispatcher.deinit();
            self.allocator.destroy(self);
        }

        pub fn bind(self: *Self, endpoints: types.Endpoints) !void {
            try self.send_socket.bind(endpoints.pub_sub);
            try self.reply_socket.bind(endpoints.req_rep);
            try self.pull_sink_socket.connect();
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
