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
            self.* = undefined;
        }

        pub fn connect(self: *Self) !void {
            try self.socket.bind(SinkEndpoint);
        }

        pub fn spawn(self: *Self, worker: *WorkerType) !void {
            if (self.pool.threads.len == 0) {
                self.pool.deinit();
                try self.pool.init(.{.allocator = self.allocator});
            }

            const worker_socket = try zmq.ZSocket.init(.Push, self.context);
            try self.pool.spawn(runWorker, .{worker, worker_socket, self.endpoint});
        }

        fn runWorker(worker: *WorkerType, socket: *zmq.ZSocket, endpoint: types.Symbol) void {
            socket.connect(endpoint) catch @panic("Connection failed");
            defer socket.deinit();
            defer worker.deinit();

            worker.run(socket) catch @panic("Invalid action");
        }
    };
}