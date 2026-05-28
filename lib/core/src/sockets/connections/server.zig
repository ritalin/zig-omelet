const std = @import("std");
const root = @import("../../root.zig");
const nnng = @import("nnng");

const types = root.types;
const events = root.events;

const ReceiveEntry = root.sockets.ReceiveEntry;
const SendChannel = root.sockets.SendChannel;
const Event = root.events.Event;
const EventHeader = root.events.EventHeader;
const EventDispatcher = root.sockets.EventDispatcher;
const Logger = root.Logger;

const encodeToCbor = @import("../../events/encoder.zig").encodeToCbor;
const putConsoleLog = @import("../../supports/log_support.zig").putConsoleLog;

const INPROC_URL = "inproc://sync-thread";

pub fn Server(comptime stage_name: types.StageName) type {
// TODO:
// pub fn Server(comptime stage_name: types.Symbol, comptime WorkerType: type) type {

    return struct {
        context: nnng.Context,
        reply_socket: nnng.Rep.Protocol(nnng.Transport.Listener, nnng.Pipe.Parallel),
        pull_socket: nnng.Pull.Protocol(nnng.Transport.Listener, nnng.Pipe.Sync),
        cmd_socket: nnng.Pub.Protocol(nnng.Transport.Dialer, nnng.Pipe.Sync),
        inproc_socket: nnng.Push.Protocol(nnng.Transport.Dialer, nnng.Pipe.Sync),

        const Self = @This();

        pub fn create(io: std.Io, allocator: std.mem.Allocator, parallel_limit: usize, endpoints: types.Endpoints) !Self {
            const context = nnng.Context.init(io, allocator);

            var pull_socket = socket: {
                const b = try nnng.Pull.open(context);
                break:socket try b.as_listener(endpoints.push_pull);
            };
            errdefer pull_socket.close();

            try pull_socket.transport.addChannel(INPROC_URL);

            var cmd_socket = socket: {
                const b = try nnng.Pub.open(context);
                break:socket try b.as_dialer(endpoints.pub_sub);
            };
            errdefer cmd_socket.close();

            var reply_socket = socket: {
                const b = try nnng.Rep.open(context);
                break:socket try b.parallel(parallel_limit).as_listener(endpoints.req_rep);
            };
            errdefer reply_socket.close();

            var inproc_socket = socket: {
                const b = try nnng.Push.open(context);
                break:socket try b.as_dialer(INPROC_URL);
            };
            errdefer inproc_socket.close();

            return .{
                .context = context,
                .reply_socket = reply_socket,
                .pull_socket = pull_socket,
                .cmd_socket = cmd_socket,
                .inproc_socket = inproc_socket,
            };
        }

        pub fn deinit(self: *Self) void {
            self.cmd_socket.close();
            self.inproc_socket.close();
            self.reply_socket.close();
            self.pull_socket.close();
        }

        pub fn bind(self: *Self) !void {
            try self.reply_socket.transport.start(.{});
            errdefer self.reply_socket.close();

            try self.pull_socket.transport.start(.{});
            errdefer self.pull_socket.close();

            try self.inproc_socket.transport.start(.{});
            errdefer self.inproc_socket.close();

            try self.cmd_socket.transport.start(.{ .nonblocking = true });
            errdefer self.cmd_socket.close();
        }

        pub fn enableIntegratedLog(self: *Self) void {
            Logger.enableIntegratedLog(.{
                .ptr = self,
                .handler = Self.doNonIntegratedLog,
            });
        }

        pub fn configureDispatcher(self: *Self, comptime poller_size: comptime_int, options: EventDispatcher.Options) !EventDispatcher.Sized(poller_size) {
            var dispatcher = try EventDispatcher.Sized(poller_size).create(self.context, Self.onPoll, options);
            try nnng.ReceivePoller(poller_size).Parallel.attach(&dispatcher.poller, &self.reply_socket.pipe);
            try nnng.ReceivePoller(poller_size).Sync.attach(&dispatcher.poller, &self.pull_socket.pipe);

            const bootEntry = try ReceiveEntry.booting(stage_name);
            try dispatcher.queue.pushReceiveQueue(bootEntry);

            return dispatcher;
        }

        pub fn commandChannel(self: *const Self) !SendChannel {
            return SendChannel.init(self.context.allocator, self.cmd_socket.pipe.item.id, stage_name, self.cmd_socket.pipe.item.sender());
        }

        pub fn dataChannel(self: *const Self) !SendChannel {
            return SendChannel.init(self.context.allocator, self.inproc_socket.pipe.item.id, stage_name, self.inproc_socket.pipe.item.sender());
        }

        fn onPoll(queue: *EventDispatcher.Queue, results: []const nnng.PollEvent) anyerror!void {
            // TODO: send try error log
            for (results) |result| {
                switch (result) {
                    .failed => |err| {
                        _ = err;
                        // TODO: error handling
                    },
                    .ready => |channel| {
                        var msg = try channel.receiver().drain(.{});
                        if (ReceiveEntry.create(queue.allocator, channel.id, msg, channel.features)) |entry| {
                            try queue.pushReceiveQueue(entry);

                            if (channel.features.replyable) {
                                try writeResponse(&msg, .ack);
                                try channel.sender().submit(msg, .{});
                            }
                        }
                        else |_| {
                            // TODO: send error log

                            if (channel.features.replyable) {
                                try writeResponse(&msg, .nack);
                                try channel.sender().submit(msg, .{});
                            }
                        }
                    }
                }
            }
        
        // TODO:
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
        //     }
        }

        fn doNonIntegratedLog(ptr: *anyopaque, level: events.LogLevel, msg: []const u8)anyerror!void {
            _ = ptr;
            try putConsoleLog(level, stage_name, "{s}", .{ msg });
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

        var conn = try Server("runner").create(std.testing.io, std.testing.allocator, 1, ep);
        defer conn.deinit();

        try conn.bind();
    }
};
