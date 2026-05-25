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
const encodeSubscription = @import("../../events/encoder.zig").encodeSubscription;

pub fn Client(comptime stage_name: types.StageName) type {
    return struct {
        context: nnng.Context,
        req_socket: nnng.Req.Protocol(nnng.Transport.Dialer, nnng.Pipe.Sync),
        push_socket: nnng.Push.Protocol(nnng.Transport.Dialer, nnng.Pipe.Sync),
        cmd_socket: nnng.Sub.Protocol(nnng.Transport.Listener, nnng.Pipe.Sync),
        // cmd_socket: nnng.Sub.Protocol(nnng.Transport.Listener, nnng.Pipe.Sync),

        const Self = @This();

        pub fn create(io: std.Io, allocator: std.mem.Allocator, endpoints: types.Endpoints) !Self {
            const context = nnng.Context.init(io, allocator);
            var req_socket = socket: {
                const b = try nnng.Req.open(context);
                break:socket try b.as_dialer(endpoints.req_rep);
            };
            errdefer req_socket.close();

            var push_socket = socket: {
                const b = try nnng.Push.open(context);
                break:socket try b.as_dialer(endpoints.push_pull);
            };
            errdefer push_socket.close();

            var cmd_socket = socket: {
                const b = try nnng.Sub.open(context);
                break:socket try b.as_listener(endpoints.pub_sub);
                // break:socket try b.as_listener(endpoints.pub_sub);
            };
            errdefer cmd_socket.close();

            return .{
                .context = context,
                .req_socket = req_socket,
                .push_socket = push_socket,
                .cmd_socket = cmd_socket,
            };
        }

        pub fn deinit(self: *Self) void {
            self.req_socket.close();
            self.push_socket.close();
            self.cmd_socket.close();
        }

        pub fn connect(self: *Self) !void {
            try self.cmd_socket.transport.start(.{});
            try self.req_socket.transport.start(.{ .nonblocking = true });
            try self.push_socket.transport.start(.{ .nonblocking = true });
        }

        pub fn configureDispatcher(self: *Self, comptime poller_size: comptime_int) !EventDispatcher.Sized(poller_size) {
            var dispatcher = try EventDispatcher.Sized(poller_size).create(self.context, Self.onPoll);
            try nnng.ReceivePoller(poller_size).Sync.attach(&dispatcher.poller, &self.cmd_socket.pipe);

            dispatcher.on_quit = .{
                .ptr = self,
                .handler = Self.doQuit,
            };

            const bootEntry = try ReceiveEntry.booting(stage_name);
            try dispatcher.queue.pushReceiveQueue(bootEntry);

            return dispatcher;
        }

        pub fn subscribe(self: *Self, subscriptions: []const EventHeader) !void {
            var buffer = std.Io.Writer.Allocating.init(self.context.allocator);
            defer buffer.deinit();

            var view = self.cmd_socket.subscriptionView();

            for (subscriptions) |subscription| {
                const bytes = try encodeSubscription(&buffer.writer, subscription);
                try view.subscribe(bytes);
            }
        }

        pub fn requestChannel(self: *const Self) !SendChannel {
            return SendChannel.init(self.context.allocator, stage_name, self.req_socket.pipe.item.sender());
        }

        fn onPoll(queue: *EventDispatcher.Queue, results: []const nnng.PollEvent) anyerror!void {
            for (results) |result| {
                switch (result) {
                    .failed => |err| {
                        _ = err;
                        // TODO: error handling
                    },
                    .ready => |channel| {
                        const msg = try channel.receiver().drain(.{});
                        if (ReceiveEntry.create(queue.allocator, stage_name, msg, channel.features)) |entry| {
                            try queue.pushReceiveQueue(entry);
                        }
                        else |_| {
                            // Todo: send error log
                        }
                    }
                }
            }
        }

        fn doQuit(ptr: *anyopaque) anyerror!void {
            const self: *Self = @ptrCast(@alignCast(ptr));

            var rpc = try nnng.Rpc.create();
            if (rpc.msg) |*msg| {
                try writeEvent(msg, .quit);
            }
            var f = rpc.submit(self.context.io, self.req_socket.pipe.item.sender(), self.req_socket.pipe.item.receiver());
            try f.await(self.context.io);
            // var pipe = self.req_socket.pipe.item;
            // var msg = try nnng.Message.create();
            // try writeEvent(&msg, .quit);
            // try pipe.sender().submit(msg, .{});

            // msg = try pipe.receiver().drain(.{});
            // defer msg.deinit();
        }

        fn writeEvent(msg: *nnng.Message, event: Event) !void {
            msg.writer.end = 0;
            try encodeToCbor(&msg.writer, .{ .header = EventHeader.fromEvent(event), .stage_name = stage_name, .event = event });
        }
    };
}
