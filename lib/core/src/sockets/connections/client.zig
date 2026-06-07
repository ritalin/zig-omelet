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
        cmd_socket: nnng.Sub.Protocol(nnng.Transport.Dialer, nnng.Pipe.Sync),
        pull_worker_socket: nnng.Pull.Protocol(nnng.Transport.Listener, nnng.Pipe.Sync),
        push_worker_socket: nnng.Push.Protocol(nnng.Transport.Dialer, nnng.Pipe.Sync),

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
                break:socket try b.as_dialer(endpoints.pub_sub);
            };
            errdefer cmd_socket.close();

            var pull_worker_socket = socket: {
                const b = try nnng.Pull.open(context);
                break:socket try b.as_listener(endpoints.worker orelse types.WORKER_ENDPOINT);
            };
            errdefer pull_worker_socket.close();

            var push_worker_socket = socket: {
                const b = try nnng.Push.open(context);
                break:socket try b.as_dialer(endpoints.worker orelse types.WORKER_ENDPOINT);
            };
            errdefer push_worker_socket.close();

            return .{
                .context = context,
                .req_socket = req_socket,
                .push_socket = push_socket,
                .cmd_socket = cmd_socket,
                .pull_worker_socket = pull_worker_socket,
                .push_worker_socket = push_worker_socket,
            };
        }

        pub fn deinit(self: *Self) void {
            self.req_socket.close();
            self.push_socket.close();
            self.cmd_socket.close();
            self.push_worker_socket.close();
            self.pull_worker_socket.close();
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
            try self.cmd_socket.transport.start(.{ .nonblocking = true });
            errdefer self.cmd_socket.close();

            try self.req_socket.transport.start(.{ .nonblocking = true });
            errdefer self.req_socket.close();

            try self.push_socket.transport.start(.{ .nonblocking = true });
            errdefer self.push_socket.close();

            try self.pull_worker_socket.transport.start(.{});
            errdefer self.pull_worker_socket.close();

            try self.push_worker_socket.transport.start(.{ .nonblocking = true });
            errdefer self.push_worker_socket.close();
        }

        fn enableIntegratedLog(self: *Self, log_integrated: bool) void {
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
            self.enableIntegratedLog(options.log_style == .integrated);

            var dispatcher = try EventDispatcher.Sized(poller_size).create(self.context, Self.PollHandler(poller_size).doPoll, options);
            try nnng.ReceivePoller(poller_size).Sync.attach(&dispatcher.poller, &self.cmd_socket.pipe, .{});
            try nnng.ReceivePoller(poller_size).Sync.attach(&dispatcher.poller, &self.pull_worker_socket.pipe, .{ .raw_mask = WorkerPipeOption.encode(.{ .forwarding = true }) });

            dispatcher.vtable.on_post = .{
                .ptr = self,
                .handler = Self.RawMessageForwardhandler(poller_size).doPost,
            };
            dispatcher.vtable.on_quit = .{
                .ptr = self,
                .handler = Self.doQuit,
            };

            if (options.log_style == .integrated) {
                dispatcher.vtable.on_log = .{
                    .ptr = self,
                    .handler = Self.ForwardLogHandler(poller_size).doLog,
                };
            }

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

        fn doQuit(ptr: *anyopaque) anyerror!void {
            const self: *Self = @ptrCast(@alignCast(ptr));

            var channel = try self.requestChannel();
            try channel.submit(self.context.io, .quit, .{});
        }

        fn doIntegratedLog(ptr: *anyopaque, level: events.LogLevel, msg: []const u8) anyerror!void {
            const self: *Self = @ptrCast(@alignCast(ptr));

            const log_event = events.Event.Payload.Log.init(.{level, msg});
            var channel = try self.dataChannel();
            try channel.encode(.{ .log = log_event });
            try channel.submit(.{ .flags = .{ .nonblocking = true } });
        }

        fn doNonIntegratedLog(ptr: *anyopaque, level: events.LogLevel, msg: []const u8)anyerror!void {
            _ = ptr;
            try putConsoleLog(level, stage_name, "{s}", .{ msg });
        }

        fn PollHandler(comptime poller_size: comptime_int) type {
            return struct {
                pub fn doPoll(dispatcher: *EventDispatcher.Sized(poller_size), results: []const nnng.PollEvent) !void {
                    for (results) |result| {
                        switch (result) {
                            .failed => |payload| {
                                try dispatcher.log(.err, stage_name, "Poll failed/pipe_id: {}, err: {s}", .{ payload.id, @errorName(payload.err) });
                            },
                            .ready => |channel| {
                                const receiver = channel.receiver();
                                const facts: WorkerPipeOptions = WorkerPipeOption.decode(channel.options.raw_mask);

                                while (try receiver.tryDrain()) |msg| {
                                    if (facts.forwarding) {
                                        try EventDispatcher.Sized(poller_size).RawMessageForwarding.post(dispatcher, msg);
                                    }
                                    else {
                                        if (ReceiveEntry.create(dispatcher.queue.allocator, channel.id, msg, channel.features)) |entry| {
                                            try dispatcher.queue.pushReceiveQueue(entry);
                                        }
                                        else |err| {
                                            try dispatcher.log(.err, stage_name, "Failed decode event/pipe_id: {}, err: {s}", .{ channel.id, @errorName(err) });
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            };
        }

        fn ForwardLogHandler(comptime poller_size: comptime_int) type {
            return struct {
                pub fn doLog(ptr: *anyopaque, dispatcher: *EventDispatcher.Sized(poller_size), log_event: events.Event.Payload.Log, mode: root.Logger.LogIntegratedMode) !void {
                    const self: *Self = @ptrCast(@alignCast(ptr));
                    var channel = try self.dataChannel();
                    const g = channel.sender.lock();
                    defer g.unlock();

                    switch (mode) {
                        .batch => try dispatcher.queue.post(.{ .log = log_event }, channel),
                        .direct => {
                            try channel.encode(.{ .log = log_event });
                            channel.submit(.{ .flags = .{ .nonblocking = true } }) catch |err| switch (err) {
                                error.WouldBlock => {
                                    try dispatcher.queue.postPriority(channel);
                                },
                                else => return err,
                            };
                        },
                    }
                }
            };
        }

        fn RawMessageForwardhandler(comptime poller_size: comptime_int) type {
            return struct {
                pub fn doPost(ptr: *anyopaque, dispatcher: *EventDispatcher.Sized(poller_size), msg: nnng.Message) !void {
                    const self: *Self = @ptrCast(@alignCast(ptr));

                    const channel = SendChannel.fromMessage(
                        self.context.allocator, 
                        self.push_socket.pipe.item.id,
                        stage_name, 
                        self.push_socket.pipe.item.sender(), 
                        msg,
                    );

                    try dispatcher.queue.send_queue.pushBack(
                        dispatcher.queue.allocator, 
                        channel
                    );
                }
            };
        }

        const WorkerPipeOption = enum { 
            forwarding,

            const encode = encodeWorkerPipeOptions;
            const decode = decodeWorkerPipeOptions;
        };
        const WorkerPipeOptions = std.enums.EnumFieldStruct(WorkerPipeOption, bool, false);

        fn encodeWorkerPipeOptions(options: WorkerPipeOptions) u64 {
            return std.enums.EnumSet(WorkerPipeOption).init(options).bits.mask;
        }

        fn decodeWorkerPipeOptions(raw_mask: u64) WorkerPipeOptions {
            const mask: std.enums.EnumSet(WorkerPipeOption) = .{ .bits = .{ .mask = @intCast(raw_mask) } };
            var options: WorkerPipeOptions = .{};
            inline for (std.meta.fields(WorkerPipeOption)) |f| {
                @field(options, f.name) = mask.contains(@enumFromInt(f.value));
            }

            return options;
        }
    };
}
