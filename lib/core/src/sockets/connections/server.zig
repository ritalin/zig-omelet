const std = @import("std");
const root = @import("../../root.zig");
const nnng = @import("nnng");

const types = root.types;
const ReceiveEntry = root.sockets.ReceiveEntry;
const SendChannel = root.sockets.SendChannel;
const Event = root.events.Event;
const EventHeader = root.events.EventHeader;
const EventDispatcher = root.sockets.EventDispatcher;

const encodeToCbor = @import("../../events/encoder.zig").encodeToCbor;

pub fn Server(comptime stage_name: types.StageName) type {
// pub fn Server(comptime stage_name: types.Symbol, comptime WorkerType: type) type {
    // const Trace = Logger.TraceDirect(stage_name);

    return struct {
        context: nnng.Context,
        reply_socket: nnng.Rep.Protocol(nnng.Transport.Listener, nnng.Pipe.Parallel),
        pull_socket: nnng.Pull.Protocol(nnng.Transport.Listener, nnng.Pipe.Sync),
        cmd_socket: nnng.Pub.Protocol(nnng.Transport.Dialer, nnng.Pipe.Sync),

        const Self = @This();

        pub fn create(io: std.Io, allocator: std.mem.Allocator, endpoints: types.Endpoints) !Self {
            var parallel_limit = try std.Thread.getCpuCount() - 1; // std.io.Threaded default value

            const context = nnng.Context.init(io, allocator);
    
            var pull_socket = socket: {
                const b = try nnng.Pull.open(context);
                break:socket try b.as_listener(endpoints.push_pull);
            };
            errdefer pull_socket.close();
            parallel_limit -= 1;

            var cmd_socket = socket: {
                const b = try nnng.Pub.open(context);
                break:socket try b.as_dialer(endpoints.pub_sub);
            };
            errdefer cmd_socket.close();
            parallel_limit -= 1;

            var reply_socket = socket: {
                const b = try nnng.Rep.open(context);
                break:socket try b.parallel(parallel_limit).as_listener(endpoints.req_rep);
            };
            errdefer reply_socket.close();

            return .{
                .context = context,
                .reply_socket = reply_socket,
                .pull_socket = pull_socket,
                .cmd_socket = cmd_socket,
            };
        }

        pub fn deinit(self: *Self) void {
            self.reply_socket.close();
            self.pull_socket.close();
            self.cmd_socket.close();
        }

        pub fn bind(self: *Self) !void {
            try self.cmd_socket.transport.start(.{ .nonblocking = true });
            try self.reply_socket.transport.start(.{});
            try self.pull_socket.transport.start(.{});
        }

        pub fn configureDispatcher(self: *Self, comptime poller_size: comptime_int) !EventDispatcher.Sized(poller_size) {
            var dispatcher = try EventDispatcher.Sized(poller_size).create(self.context, Self.onPoll);
            try nnng.ReceivePoller(poller_size).Parallel.attach(&dispatcher.poller, &self.reply_socket.pipe);
            try nnng.ReceivePoller(poller_size).Sync.attach(&dispatcher.poller, &self.pull_socket.pipe);

            return dispatcher;
        }

        pub fn createCommandChannel(self: *const Self) !SendChannel {
            return SendChannel.init(self.context.allocator, stage_name, self.cmd_socket.pipe.item.sender());
        }

        fn onPoll(queue: *EventDispatcher.Queue, results: []const nnng.PollEvent) anyerror!void {
            for (results) |result| {
                switch (result) {
                    .failed => |err| {
                        _ = err;
                        // TODO: error handling
                    },
                    .ready => |channel| {
                        var msg = try channel.receiver().drain(.{});
                        if (ReceiveEntry.create(queue.allocator, stage_name, msg, channel.features)) |entry| {
                            try queue.pushReceiveQueue(entry);

                            if (channel.features.replyable) {
                                try writeResponse(&msg, .ack);
                                try channel.sender().submit(msg, .{});
                            }
                        }
                        else |_| {
                            // Todo: send error log

                            if (channel.features.replyable) {
                                try writeResponse(&msg, .nack);
                                try channel.sender().submit(msg, .{});
                            }
                        }
                    }
                }
            }
        //         while (dispatcher.receive_queue.dequeue()) |entry| {
        //             return entry;
        //         }

        //         send_event: {
        //             while (dispatcher.send_queue.dequeue()) |entry| {
        //                 defer entry.deinit();

        //                 Trace.debug("{s}: {} from [{s}] to [{s}] ({}) ", .{
        //                     if (entry.kind == .reply) "Reply" else "Post",
        //                     std.meta.activeTag(entry.event),
        //                     entry.from,
        //                     if (entry.routing_id) |routing_id| routing_id else "all",
        //                     dispatcher.send_queue.count()
        //                 });

        //                 if (entry.routing_id) |routing_id| {
        //                     try events.sendRoutingId(dispatcher.allocator, entry.socket, routing_id);
        //                 }

        //                 events.sendEvent(
        //                     dispatcher.allocator, entry.socket,
        //                     .{ .kind = entry.kind, .from = stage_name, .event = entry.event }
        //                 )
        //                 catch |err| switch (err) {
        //                     else => {
        //                         // Logger.Server.traceLog.debug("Unexpected error on sending: {any}", .{err});
        //                         return err;
        //                     }
        //                 };
        //             }
        //             else if (dispatcher.state.level.done) {
        //                 return null;
        //             }
        //             break:send_event;
        //         }

        //         receive_event: {
        //             while (true) {
        //                 var it = try dispatcher.polling.poll();
        //                 defer it.deinit();

        //                 while (it.next()) |item| {
        //                     const routing_id = try events.receiveRoutingId(dispatcher.allocator, item.socket);

        //                     const packet = events.receiveEventWithPayload(dispatcher.allocator, item.socket) catch |err| switch (err) {
        //                         // error.InvalidResponse => {
        //                         //     try events.sendEvent(dispatcher.allocator, item.socket, .nack);
        //                         //     continue;
        //                         // },
        //                         else => return err,
        //                     };

        //                     Trace.debug("Received command: {} from [{s}]", .{
        //                         packet.event.tag(),
        //                         std.mem.sliceTo(packet.from, 0),
        //                     });

        //                     try dispatcher.receive_queue.enqueue(.{
        //                         .allocator = dispatcher.allocator,
        //                         .kind = .response,
        //                         .socket = item.socket,
        //                         .from = packet.from,
        //                         .event = packet.event,
        //                         .routing_id = routing_id,
        //                     });
        //                 }

        //                 if (dispatcher.receive_queue.count() > 0) break:receive_event;
        //             }
        //         }
        //     }
        }

        fn writeResponse(msg: *nnng.Message, event: Event) !void {
            msg.writer.end = 0;
            try encodeToCbor(&msg.writer, .{ .header = EventHeader.fromEvent(event), .stage_name = stage_name, .event = event });
        }
    };
}

test "Connection.Server test" {
    std.testing.refAllDecls(@This());
}

pub const tests = struct {
    const supports = @import("../../supports/test_support.zig");

    test "start connection" {
        var tmp_dir = try supports.createTmpDir();
        defer tmp_dir.cleanup();

        const ep = try supports.createEndpoint(tmp_dir.dir);
        defer supports.releaseEndpoint(std.testing.allocator, ep);

        var conn = try Server("runner").create(std.testing.io, std.testing.allocator, ep);
        defer conn.deinit();

        try conn.bind();
    }
};
