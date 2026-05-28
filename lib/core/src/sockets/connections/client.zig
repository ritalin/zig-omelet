const std = @import("std");
const root = @import("../../root.zig");
const nnng = @import("nnng");

const types = root.types;
const events = root.events;

const ReceiveEntry = root.sockets.ReceiveEntry;
const RpcChannel = root.sockets.RpcChannel;
const SendChannel = root.sockets.SendChannel;
const Event = root.events.Event;
const EventHeader = root.events.EventHeader;
const EventDispatcher = root.sockets.EventDispatcher;
const Logger = root.Logger;

const encodeToCbor = @import("../../events/encoder.zig").encodeToCbor;
const encodeSubscription = @import("../../events/encoder.zig").encodeSubscription;
const decodeSubscription = @import("../../events/decoder.zig").decodeSubscription;
const putConsoleLog = @import("../../supports/log_support.zig").putConsoleLog;

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

        pub fn sbscribe(self: *Self, topics: []EventHeader) !void {
            var buffer: std.Io.Writer.Allocating = .init(self.context.allocator);
            defer buffer.deinit();

            var view = self.cmd_socket.subscriptionView();

            for (topics) |topic| {
                buffer.writer.end = 0;
                try encodeSubscription(&buffer, topic);
                try view.subscribe(buffer.written());
            }
        }

        pub fn listSubscriptions(self: *Self, allocator: std.mem.Allocator) !types.Symbol {
            var view = self.cmd_socket.subscriptionView();

            var buffer: std.ArrayListUnmanaged(types.Symbol) = .empty;
            defer buffer.deinit(allocator);
            try view.extractSubscriptions(allocator, &buffer);

            for (buffer.items) |*bytes| {
                bytes.* = try decodeSubscription(bytes.*);
            }

            return std.mem.join(allocator, ", ", buffer.items);
        } 

        pub fn connect(self: *Self) !void {
            try self.cmd_socket.transport.start(.{});
            errdefer self.cmd_socket.close();

            try self.req_socket.transport.start(.{ .nonblocking = true });
            errdefer self.req_socket.close();

            try self.push_socket.transport.start(.{ .nonblocking = true });
            errdefer self.push_socket.close();
        }

        pub fn enableIntegratedLog(self: *Self, log_integrated: bool) void {
            if (log_integrated) {
                Logger.enableIntegratedLog(.{
                    .ptr = self,
                    .handler = Self.doIntegratedLog,
                });
            }
            else {
                Logger.enableIntegratedLog(.{
                    .ptr = self,
                    .handler = Self.doNonIntegratedLog,
                });
            }
        }

        pub fn configureDispatcher(self: *Self, comptime poller_size: comptime_int, options: EventDispatcher.Options) !EventDispatcher.Sized(poller_size) {
            var dispatcher = try EventDispatcher.Sized(poller_size).create(self.context, Self.onPoll, options);
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

        pub fn requestChannel(self: *Self) !RpcChannel {
            return RpcChannel.init(
                stage_name, 
                self.req_socket.pipe.item.sender(), 
                self.req_socket.pipe.item.receiver()
            );
        }

        pub fn dataChannel(self: *Self) !SendChannel {
            return SendChannel.init(
                self.context.allocator, 
                self.push_socket.pipe.item.id,
                stage_name, 
                self.push_socket.pipe.item.sender(), 
            );
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
                        if (ReceiveEntry.create(queue.allocator, channel.id, msg, channel.features)) |entry| {
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

            var channel = try self.requestChannel();
            try channel.encode(.quit);
            try channel.submit(self.context.io);
        }

        fn doIntegratedLog(ptr: *anyopaque, level: events.LogLevel, msg: []const u8) anyerror!void {
            _ = ptr;
            _ = level;
            _ = msg;
            unreachable;
        }

        fn doNonIntegratedLog(ptr: *anyopaque, level: events.LogLevel, msg: []const u8)anyerror!void {
            _ = ptr;
            try putConsoleLog(level, stage_name, "{s}", .{ msg });
        }
    };
}
