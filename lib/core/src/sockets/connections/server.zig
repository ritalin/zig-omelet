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

pub fn Server(comptime stage_name: types.StageName) type {
// TODO:
// pub fn Server(comptime stage_name: types.Symbol, comptime WorkerType: type) type {

    return struct {
        context: nnng.Context,
        reply_socket: nnng.Rep.Protocol(nnng.Transport.Listener, nnng.Pipe.Parallel),
        pull_socket: nnng.Pull.Protocol(nnng.Transport.Listener, nnng.Pipe.Sync),
        cmd_socket: nnng.Pub.Protocol(nnng.Transport.Listener, nnng.Pipe.Sync),
        inproc_socket: nnng.Push.Protocol(nnng.Transport.Dialer, nnng.Pipe.Sync),

        const Self = @This();

        pub fn create(io: std.Io, allocator: std.mem.Allocator, parallel_limit: usize, endpoints: types.Endpoints) !Self {
            const context = nnng.Context.init(io, allocator);

            var pull_socket = socket: {
                const b = try nnng.Pull.open(context);
                break:socket try b.as_listener(endpoints.push_pull);
            };
            errdefer pull_socket.close();

            try pull_socket.transport.addChannel(types.WORKER_ENDPOINT);

            var cmd_socket = socket: {
                const b = try nnng.Pub.open(context);
                break:socket try b.as_listener(endpoints.pub_sub);
            };
            errdefer cmd_socket.close();

            var reply_socket = socket: {
                const b = try nnng.Rep.open(context);
                break:socket try b.parallel(parallel_limit).as_listener(endpoints.req_rep);
            };
            errdefer reply_socket.close();

            var inproc_socket = socket: {
                const b = try nnng.Push.open(context);
                break:socket try b.as_dialer(types.WORKER_ENDPOINT);
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

            try self.cmd_socket.transport.start(.{});
            errdefer self.cmd_socket.close();
        }

        pub fn enableIntegratedLog(self: *Self) void {
            Logger.enableIntegratedLog(.{
                .ptr = self,
                .handler = Self.doNonIntegratedLog,
            });
        }

        pub fn configureDispatcher(self: *Self, comptime poller_size: comptime_int, options: EventDispatcher.Options) !EventDispatcher.Sized(poller_size) {
            var dispatcher = try EventDispatcher.Sized(poller_size).create(self.context, Self.PollHandler(poller_size).onPoll, options);
            try nnng.ReceivePoller(poller_size).Parallel.attach(&dispatcher.poller, &self.reply_socket.pipe, .{});
            try nnng.ReceivePoller(poller_size).Sync.attach(&dispatcher.poller, &self.pull_socket.pipe, .{});

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

        fn doNonIntegratedLog(ptr: *anyopaque, level: events.LogLevel, msg: []const u8)anyerror!void {
            _ = ptr;
            try putConsoleLog(level, stage_name, "{s}", .{ msg });
        }

        fn writeResponse(msg: *nnng.Message, event: Event) !void {
            msg.writer.end = 0;
            try encodeToCbor(&msg.writer, .{ .header = EventHeader.fromEvent(event), .stage_name = stage_name, .event = event });
        }

        fn PollHandler(comptime poller_size: comptime_int) type {
            return struct {
                fn onPoll(dispatcher: *EventDispatcher.Sized(poller_size), results: []const nnng.PollEvent) anyerror!void {
                    for (results) |result| {
                        switch (result) {
                            .failed => |payload| {
                                try dispatcher.log(.err, stage_name, "Poll failed/pipe_id: {}, err: {s}", .{ payload.id, @errorName(payload.err) });
                            },
                            .ready => |channel| {
                                const receiver = channel.receiver();
                                while (try receiver.tryDrain()) |msg| {
                                    if (ReceiveEntry.create(dispatcher.queue.allocator, channel.id, msg, channel.features)) |entry| {
                                        try dispatcher.queue.pushReceiveQueue(entry);

                                        if (channel.features.replyable) {
                                            var msg_mut = msg;
                                            try writeResponse(&msg_mut, .ack);
                                            try channel.sender().submit(msg_mut);
                                        }
                                    }
                                    else |err| {
                                        try dispatcher.log(.err, stage_name, "Failed decode event/pipe_id: {}, err: {s}", .{ channel.id, @errorName(err) });

                                        if (channel.features.replyable) {
                                            var msg_mut = msg;
                                            try writeResponse(&msg_mut, .nack);
                                            try channel.sender().submit(msg_mut);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            };
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
        defer supports.releaseEndpoint(ep);

        var conn = try Server("runner").create(std.testing.io, std.testing.allocator, 1, ep);
        defer conn.deinit();

        try conn.bind();
    }
};
