const std = @import("std");
const zmq = @import("zmq");
const types = @import("../types.zig");

const WORKER_CHANNEL_ROOT = std.fmt.comptimePrint("inproc://workers", .{});

pub fn Worker(comptime WorkerType: type) type {
    return struct {
        const Self = @This();
        const SinkEndpoint = std.fmt.comptimePrint("{s}_{s}", .{WORKER_CHANNEL_ROOT, @typeName(WorkerType)});

        allocator: std.mem.Allocator, 
        context: *zmq.ZContext,
        socket: *zmq.ZSocket,
        endpoint: types.Symbol,
        pool: std.Thread.Pool,

        pub fn init(allocator: std.mem.Allocator, context: *zmq.ZContext) !*Self {
            const socket = try zmq.ZSocket.init(.Pull, context);

            var pool: std.Thread.Pool = undefined;
            try pool.init(.{
                .allocator = allocator, .n_jobs = 0,
            });

            const self = try allocator.create(Self);
            self.* = .{
                .allocator = allocator,
                .context = context,
                .socket = socket,
                .endpoint = SinkEndpoint,
                .pool = pool,
            };

            return self;
        }

        pub fn deinit(self: *Self) void {
            self.pool.deinit();
            self.socket.deinit();
            self.allocator.destroy(self);
        }

        pub fn connect(self: *Self) !void {
            try self.socket.bind(SinkEndpoint);
        }

        pub fn spawn(self: *Self, worker: *WorkerType) !void {
            if (self.pool.threads.len == 0) {
                self.pool.deinit();
                try self.pool.init(.{.allocator = self.allocator});
            }

            try self.pool.spawn(runWorker, .{self, worker, self.endpoint});
        }

        pub fn stop(self: *Self) !void {
            self.pool.deinit();
            try self.pool.init(.{.allocator = self.allocator, .n_jobs = 0});
        }

        pub fn workerSocket(self: *Self) !*zmq.ZSocket {
            return zmq.ZSocket.init(.Push, self.context);
        }

        fn runWorker(self: *Self, worker: *WorkerType, endpoint: types.Symbol) void {
            var socket = self.workerSocket() catch @panic("Failed to create Push socket");
            socket.connect(endpoint) catch @panic("Connection failed");
            defer socket.deinit();
            defer worker.deinit();

            worker.run(socket) catch @panic("Invalid action");
        }
    };
}