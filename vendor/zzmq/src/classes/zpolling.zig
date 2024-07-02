const std = @import("std");
const zsocket = @import("./zsocket.zig");
const c = @import("../zmq.zig").c;

const ZSocket = zsocket.ZSocket;

const ZPollEvent = enum(c_int) {
    /// ZMQ_POLLIN: At least one message may be received from the 'socket' without blocking.
    /// For standard sockets this is equivalent to the 'POLLIN' flag of the poll() system call
    /// and generally means that at least one byte of data may be read from 'fd' without blocking.
    ///
    /// For more details, see https://libzmq.readthedocs.io/en/latest/zmq_poll.html
    PollIn = c.ZMQ_POLLIN,

    /// ZMQ_POLLOUT: At least one message may be sent to the 'socket' without blocking.
    /// For standard sockets this is equivalent to the 'POLLOUT' flag of the poll() system call
    /// and generally means that at least one byte of data may be written to 'fd' without blocking.
    ///
    /// For more details, see https://libzmq.readthedocs.io/en/latest/zmq_poll.html
    PollOut = c.ZMQ_POLLOUT,

    /// ZMQ_POLLERR: This flag is passed through zmq_poll() to the underlying poll() system call
    /// and generally means that some sort of error condition is present on the socket specified by 'fd'.
    /// For 0MQ sockets this flag has no effect if set in 'events', and shall never be returned in 'revents' by zmq_poll().
    ///
    /// For more details, see https://libzmq.readthedocs.io/en/latest/zmq_poll.html
    PollErr = c.ZMQ_POLLERR,

    /// ZMQ_POLLPRI: This flags is of no use.
    /// For standard sockets this means there is urgent data to read.
    /// Refer to the POLLPRI flag for more information. For file descriptor,
    /// refer to your use case: as an example, GPIO interrupts are signaled through a POLLPRI event.
    /// This flag has no effect on Windows.
    ///
    /// For more details, see https://libzmq.readthedocs.io/en/latest/zmq_poll.html
    PollPri = c.ZMQ_POLLPRI,
};
const ZPollEventSet = std.enums.EnumSet(ZPollEvent);

pub const ZPollEvents = std.enums.EnumFieldStruct(ZPollEvent, bool, false);

pub const ZPolling = struct {
    allocator: std.mem.Allocator,
    items: []const Item,
    options: Options,

    pub const Options = struct {
        n_retry: usize = 1,
    };

    pub fn init(allocator: std.mem.Allocator, items: []const Item, options: Options) !ZPolling {
        return .{ 
            .allocator = allocator,
            .items = try allocator.dupe(Item, items),
            .options = options,
        };
    }

    pub fn deinit(self: *ZPolling) void {
        self.allocator.free(self.items);
        self.* = undefined;
    }

    pub fn poll(self: ZPolling) !Iterator {
        return self.pollWithTimeout(-1);
    }

    pub fn pollWithTimeout(self: ZPolling, timeout_ms: c_int) !Iterator {
        const raw_items = try self.allocator.alloc(c.zmq_pollitem_t, self.items.len);
        errdefer self.allocator.free(raw_items);

        for (self.items, 0..) |item, i| {
            raw_items[i] = .{
                .socket = item.socket.socket_,
                .fd = 0,
                .events = item.events.bits.mask,
                .revents = 0,
            };
        }

        return self.pollWithTimeoutInternal(raw_items, timeout_ms, self.options.n_retry);
    }

    fn pollWithTimeoutInternal(self: ZPolling, raw_items: []c.zmq_pollitem_t, timeout_ms: c_int, retry_left: usize) !Iterator {
        const result = c.zmq_poll(raw_items.ptr, @as(c_int, @intCast(raw_items.len)), timeout_ms);
        if (result == 0) {
            return error.PollingTimeout;
        }
        else if (result < 0) {
            const err_no = c.zmq_errno();

            return switch (err_no) {
                c.ETERM => error.SocketClosed,
                c.EFAULT => error.InvalidPollingItems,
                c.EINTR => {
                    if (retry_left > 0) {
                        return self.pollWithTimeoutInternal(raw_items, timeout_ms, retry_left-1);
                    }
                    return error.InvalidInterrupted;
                },
                else => error.PollingFailed,
            };
        }

        return .{
            .allocator = self.allocator,
            .items = self.items,
            .raw_items = raw_items,
            .index = 0,
        };
    }

    pub const Item = struct {
        socket: *ZSocket,
        events: ZPollEventSet,

        pub fn fromSocket(socket: *ZSocket, events: ZPollEvents) Item {
            return .{
                .socket = socket,
                .events = ZPollEventSet.init(events),
            };
        }
    };

    pub const Iterator = struct {
        allocator: std.mem.Allocator,
        items: []const Item,
        raw_items: []const c.zmq_pollitem_t,
        index: usize,

        pub fn next(self: *Iterator) ?Item {
            while (self.index < self.items.len) {
                defer self.index += 1;

                const item = self.items[self.index];
                const raw = self.raw_items[self.index];

                const events: ZPollEventSet = .{ .bits = .{ .mask = @intCast(raw.revents) } };

                if (events.intersectWith(item.events).count() > 0) {
                    return item;
                }
            }

            return null;
        }

        pub fn deinit(self: *Iterator) void {
            self.allocator.free(self.raw_items);
            self.* = undefined;
        }
    };
};
