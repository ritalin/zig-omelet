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
    items: []const Item,

    pub fn init(items: []const Item) ZPolling {
        return .{ .items = items };
    }

    pub fn poll(self: ZPolling, allocator: std.mem.Allocator) !Iterator {
        return self.pollWithTimeout(allocator, -1);
    }

    pub fn pollWithTimeout(self: ZPolling, allocator: std.mem.Allocator, timeout_ms: c_int) !Iterator {
        const raw_items = try allocator.alloc(c.zmq_pollitem_t, self.items.len);

        for (self.items, 0..) |item, i| {
            raw_items[i] = .{
                .socket = item.socket.socket_,
                .fd = 0,
                .events = item.events.bits.mask,
                .revents = 0,
            };
        }

        _ = c.zmq_poll(raw_items.ptr, @as(c_int, @intCast(raw_items.len)), timeout_ms);

        return .{
            .allocator = allocator,
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
