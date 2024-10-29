const std = @import("std");
const zcontext = @import("zcontext.zig");
const zmessage = @import("zmessage.zig");
const c = @import("../zmq.zig").c;

pub const ZMessageReceived = struct {
    message_: zmessage.ZMessage = undefined,
    hasMore_: bool = false,

    /// Takes the ownership over the provided raw message.
    ///
    /// Note: the message argument must be initalized, using `zmq_msg_init()` or other similar functions.
    ///
    /// The ownership can be lost when sending the message.
    pub fn init(message: *c.zmq_msg_t, hasMoreParts: bool) !ZMessageReceived {
        return .{
            .message_ = try zmessage.ZMessage.initExternal(message),
            .hasMore_ = hasMoreParts,
        };
    }

    /// Retrieves a slice to the data stored within the message.
    pub fn data(self: *const ZMessageReceived) ![]const u8 {
        return self.message_.data();
    }

    /// Retrieves a size of data within the message.
    pub fn size(self: *const ZMessageReceived) !usize {
        return self.message_.size();
    }

    /// Returns true, if the message is part of a multi-message
    /// and more message parts are available.
    ///
    /// To retrieve the next part, call `socket.receive()` again.
    pub fn hasMore(self: *const ZMessageReceived) bool {
        return self.hasMore_;
    }

    /// Destroys the message and performs clean up.
    pub fn deinit(self: *ZMessageReceived) void {
        self.message_.deinit();
    }
};

pub const ZSocketType = enum(c_int) {
    /// A socket of type ZMQ_PAIR can only be connected to a single peer at any one time.
    ///
    /// No message routing or filtering is performed on messages sent over a ZMQ_PAIR socket.
    ///
    /// For more details, see https://libzmq.readthedocs.io/en/zeromq3-x/zmq_socket.html .
    Pair = c.ZMQ_PAIR,

    /// A socket of type ZMQ_PUB is used by a publisher to distribute data.
    ///
    /// Messages sent are distributed in a fan out fashion to all connected peers.
    /// The zmq_recv function is not implemented for this socket type.
    ///
    /// For more details, see https://libzmq.readthedocs.io/en/zeromq3-x/zmq_socket.html .
    Pub = c.ZMQ_PUB,

    /// A socket of type ZMQ_SUB is used by a subscriber to subscribe to data distributed by a publisher.
    ///
    /// Initially a ZMQ_SUB socket is not subscribed to any messages, use the ZMQ_SUBSCRIBE option
    /// of zmq_setsockopt to specify which messages to subscribe to.
    /// The zmq_send() function is not implemented for this socket type.
    ///
    /// For more details, see https://libzmq.readthedocs.io/en/zeromq3-x/zmq_socket.html .
    Sub = c.ZMQ_SUB,

    /// Same as ZMQ_PUB except that you can receive subscriptions from the peers in form of incoming messages.
    ///
    /// Subscription message is a byte 1 (for subscriptions) or byte 0 (for unsubscriptions) followed by the subscription body.
    ///
    /// For more details, see https://libzmq.readthedocs.io/en/zeromq3-x/zmq_socket.html .
    XPub = c.ZMQ_XPUB,

    /// Same as ZMQ_SUB except that you subscribe by sending subscription messages to the socket.
    ///
    /// Subscription message is a byte 1 (for subscriptions) or byte 0 (for unsubscriptions) followed by the subscription body.
    ///
    /// For more details, see https://libzmq.readthedocs.io/en/zeromq3-x/zmq_socket.html .
    XSub = c.ZMQ_XSUB,

    /// A socket of type ZMQ_REQ is used by a client to send requests to and receive replies from a service.
    ///
    /// This socket type allows only an alternating sequence of zmq_send(request)
    /// and subsequent zmq_recv(reply) calls. Each request sent is round-robined among all services,
    /// and each reply received is matched with the last issued request.
    ///
    /// For more details, see https://libzmq.readthedocs.io/en/zeromq3-x/zmq_socket.html .
    Req = c.ZMQ_REQ,

    /// A socket of type ZMQ_REP is used by a service to receive requests from and send replies to a client.
    ///
    /// This socket type allows only an alternating sequence of zmq_recv(request) and subsequent zmq_send(reply) calls.
    /// Each request received is fair-queued from among all clients, and each reply sent is routed to the client that
    /// issued the last request. If the original requester doesn’t exist any more the reply is silently discarded.
    ///
    /// For more details, see https://libzmq.readthedocs.io/en/zeromq3-x/zmq_socket.html .
    Rep = c.ZMQ_REP,

    /// A socket of type ZMQ_DEALER is an advanced pattern used for extending request/reply sockets.
    ///
    /// Each message sent is round-robined among all connected peers, and each message received is fair-queued from all connected peers.
    ///
    /// For more details, see https://libzmq.readthedocs.io/en/zeromq3-x/zmq_socket.html .
    Dealer = c.ZMQ_DEALER,

    /// A socket of type ZMQ_ROUTER is an advanced socket type used for extending request/reply sockets.
    ///
    /// When receiving messages a ZMQ_ROUTER socket shall prepend a message part containing the identity
    /// of the originating peer to the message before passing it to the application.
    /// Messages received are fair-queued from among all connected peers.
    ///
    /// For more details, see https://libzmq.readthedocs.io/en/zeromq3-x/zmq_socket.html .
    Router = c.ZMQ_ROUTER,

    /// A socket of type ZMQ_PULL is used by a pipeline node to receive messages from upstream pipeline nodes.
    ///
    /// Messages are fair-queued from among all connected upstream nodes. The zmq_send() function is not implemented for this socket type.
    ///
    /// For more details, see https://libzmq.readthedocs.io/en/zeromq3-x/zmq_socket.html .
    Pull = c.ZMQ_PULL,

    /// A socket of type ZMQ_PUSH is used by a pipeline node to send messages to downstream pipeline nodes.
    ///
    /// Messages are round-robined to all connected downstream nodes.
    /// The zmq_recv() function is not implemented for this socket type.
    ///
    /// For more details, see https://libzmq.readthedocs.io/en/zeromq3-x/zmq_socket.html .
    Push = c.ZMQ_PUSH,
};

pub const ZSocketOption = union(enum) {
    /// ZMQ_RCVTIMEO: Maximum time before a recv operation returns with EAGAIN
    ///
    /// Sets the timeout for receive operation on the socket.
    /// If the value is 0, zmq_recv(3) will return immediately, with a EAGAIN error
    /// if there is no message to receive. If the value is -1, it will block until a
    /// message is available. For all other values, it will wait for a message for that
    /// amount of time before returning with an EAGAIN error.
    ///
    /// Unit: milliseconds
    /// Default: -1 (infinite)
    ///
    /// For more details, see https://libzmq.readthedocs.io/en/latest/zmq_setsockopt.html
    ReceiveTimeout: i32,

    /// ZMQ_RCVHWM: Set high water mark for inbound messages
    ///
    /// The ZMQ_RCVHWM option shall set the high water mark for inbound messages on the specified socket.
    /// The high water mark is a hard limit on the maximum number of outstanding messages ØMQ shall
    /// queue in memory for any single peer that the specified socket is communicating with.
    ///
    /// If this limit has been reached the socket shall enter an exceptional state and depending on the socket type,
    /// ØMQ shall take appropriate action such as blocking or dropping sent messages.
    ///
    /// Refer to the individual socket descriptions in zmq_socket(3) for details on the exact action taken for each socket type.
    ///
    /// Unit: message count (0 = unlimited)
    /// Default: 1000
    ///
    /// For more details, see https://libzmq.readthedocs.io/en/latest/zmq_setsockopt.html
    ReceiveHighWaterMark: i32,

    /// ZMQ_RCVBUF: Set kernel receive buffer size
    ///
    /// The ZMQ_RCVBUF option shall set the underlying kernel receive buffer size
    /// for the socket to the specified size in bytes. A value of zero means leave
    /// the OS default unchanged.
    ///
    /// For details refer to your operating system documentation for the SO_RCVBUF socket option.
    ///
    /// Unit: bytes
    /// Default: 0 (use kernel default)
    ///
    /// For more details, see https://libzmq.readthedocs.io/en/latest/zmq_setsockopt.html
    ReceiveBufferSize: i32,

    /// ZMQ_SNDTIMEO: Maximum time before a send operation returns with EAGAIN
    ///
    /// Sets the timeout for send operation on the socket.
    /// If the value is 0, zmq_send(3) will return immediately, with a
    /// EAGAIN error if the message cannot be sent. If the value is -1,
    /// it will block until the message is sent. For all other values,
    /// it will try to send the message for that amount of time before
    /// returning with an EAGAIN error.
    ///
    /// Unit: milliseconds
    /// Default: -1 (infinite)
    ///
    /// For more details, see https://libzmq.readthedocs.io/en/latest/zmq_setsockopt.html
    SendTimeout: i32,

    /// ZMQ_SNDHWM: Set high water mark for outbound messages
    ///
    /// The ZMQ_SNDHWM option shall set the high water mark for outbound messages
    /// on the specified socket. The high water mark is a hard limit on the maximum
    /// number of outstanding messages ØMQ shall queue in memory for any single peer
    /// that the specified socket is communicating with.
    ///
    /// If this limit has been reached the socket shall enter an exceptional state
    /// and depending on the socket type, ØMQ shall take appropriate action such as
    /// blocking or dropping sent messages.
    ///
    /// Refer to the individual socket descriptions
    /// in zmq_socket(3) for details on the exact action taken for each socket type.
    ///
    /// Unit: message count (0 = unlimited)
    /// Default: 1000
    ///
    /// For more details, see https://libzmq.readthedocs.io/en/latest/zmq_setsockopt.html
    SendHighWaterMark: i32,

    /// ZMQ_SNDBUF: Set kernel transmit buffer size
    ///
    /// The ZMQ_SNDBUF option shall set the underlying kernel transmit buffer size
    /// for the socket to the specified size in bytes. A value of zero means leave
    /// the OS default unchanged.
    ///
    /// For details please refer to your operating system documentation for the SO_SNDBUF socket option.
    ///
    /// Unit: bytes
    /// Default: 0 (use kernel default)
    ///
    /// For more details, see https://libzmq.readthedocs.io/en/latest/zmq_setsockopt.html
    SendBufferSize: i32,

    /// ZMQ_LINGER: Set linger period for socket shutdown
    ///
    /// The 'ZMQ_LINGER' option shall set the linger period for the specified 'socket'.
    /// The linger period determines how long pending messages which have yet to be sent
    /// to a peer shall linger in memory after a socket is disconnected with zmq_disconnect or
    /// closed with zmq_close, and further affects the termination of the socket’s context with zmq_ctx_term.
    ///
    /// Unit: milliseconds (0 = don't wait, -1 = infinite)
    /// Default: 0 (don't wait)
    ///
    /// For more details, see https://libzmq.readthedocs.io/en/latest/zmq_setsockopt.html
    LingerTimeout: i32,

    /// ZMQ_ROUTING_ID: Set socket routing id
    ///
    /// The 'ZMQ_ROUTING_ID' option shall set the routing id of the specified 'socket' when connecting to a ROUTER socket.
    ///
    /// A routing id must be at least one byte and at most 255 bytes long. Identities starting with a zero byte are reserved for
    /// use by the 0MQ infrastructure.
    ///
    /// If two clients use the same routing id when connecting to a ROUTER, the results shall
    /// depend on the ZMQ_ROUTER_HANDOVER option setting. If that is not set (or set to the
    /// default of zero), the ROUTER socket shall reject clients trying to connect with an
    /// already-used routing id. If that option is set to 1, the ROUTER socket shall hand-over
    /// the connection to the new client and disconnect the existing one.
    ///
    /// For more details, see https://libzmq.readthedocs.io/en/latest/zmq_setsockopt.html
    RoutingId: []u8,

    /// ZMQ_ROUTER_HANDOVER: handle duplicate client routing ids on ROUTER sockets
    ///
    /// If two clients use the same routing id when connecting to a ROUTER,
    /// the results shall depend on the ZMQ_ROUTER_HANDOVER option setting.
    ///
    /// If that is not set (or set to the default of false), the ROUTER socket shall reject
    /// clients trying to connect with an already-used routing id.
    ///
    /// If that option is set to true, the ROUTER socket shall hand-over the connection
    /// to the new client and disconnect the existing one.
    ///
    /// Default: false (reject client)
    ///
    /// For more details, see https://libzmq.readthedocs.io/en/latest/zmq_setsockopt.html
    RouterHandover: bool,

    /// ZMQ_TYPE: shall retrieve the socket type for the specified 'socket'. 
    /// The socket type is specified at socket creation time and cannot be modified afterwards.
    SocketType: ZSocketType,

    /// ZMQ_SUBSCRIBE: establish a new message filter on a 'ZMQ_SUB' socket.
    /// 
    /// Newly created 'ZMQ_SUB' sockets shall filter out all incoming messages, 
    /// therefore you should call this option to establish an initial message filter.
    /// 
    /// An empty 'option_value' of length zero shall subscribe to all incoming messages. 
    /// A non-empty 'option_value' shall subscribe to all messages beginning with the specified prefix. 
    /// Multiple filters may be attached to a single 'ZMQ_SUB' socket, 
    /// in which case a message shall be accepted if it matches at least one filter.
    /// 
    /// For more details, see https://libzmq.readthedocs.io/en/latest/zmq_setsockopt.html
    Subscribe: []const u8,

    /// ZMQ_UNSUBSCRIBE: option shall remove an existing message filter on a 'ZMQ_SUB' socket.
    /// 
    /// The filter specified must match an existing filter previously established with the 'ZMQ_SUBSCRIBE' option. 
    /// If the socket has several instances of the same filter attached the 'ZMQ_UNSUBSCRIBE' option shall remove only one instance, 
    /// leaving the rest in place and functional.
    /// 
    /// For more details, see https://libzmq.readthedocs.io/en/latest/zmq_setsockopt.html
    Unsubscribe: []const u8,
};

/// System level socket, which allows for opening outgoing and
/// accepting incoming connections.
///
/// It is either used to *bind* to a local port, or *connect*
/// to a remote (or local) port.
pub const ZSocket = struct {
    allocator_: std.mem.Allocator,
    selfArena_: std.heap.ArenaAllocator,
    socket_: *anyopaque,
    endpoint_: ?[]const u8,

    pub fn init(socketType: ZSocketType, context: *zcontext.ZContext) !*ZSocket {
        // try creating the socket, early
        const s = c.zmq_socket(context.ctx_, @intFromEnum(socketType)) orelse return error.SocketCreateFailed;
        errdefer _ = c.zmq_close(s);

        // create the managed object
        const allocator = context.allocator_;

        var selfArena = std.heap.ArenaAllocator.init(allocator);
        errdefer selfArena.deinit();
        const selfAllocator = selfArena.allocator();

        var r = try selfAllocator.create(ZSocket);
        r.allocator_ = allocator;
        r.selfArena_ = selfArena;
        r.socket_ = s;
        r.endpoint_ = null;

        // set socket defaults
        try r.setSocketOption(.{ .SendHighWaterMark = 1000 });
        try r.setSocketOption(.{ .ReceiveHighWaterMark = 1000 });
        try r.setSocketOption(.{ .LingerTimeout = 0 });

        // done
        return r;
    }

    /// Returns the actual endpoint being used for this socket.
    ///
    /// When binding TCP and IPC transports, it will also contain
    /// the actual port being used.
    pub fn endpoint(self: *const ZSocket) ![]const u8 {
        if (self.endpoint_) |e| {
            return e;
        }

        return error.NotBoundOrConnected;
    }

    /// Bind a socket to a endpoint. For tcp:// endpoints, supports
    /// ephemeral ports, if you specify the port number as "*".
    ///
    /// Call `endpoint()` to retrieve the actual endpoint being used.
    ///
    /// Examples:
    ///      tcp://127.0.0.1:*           bind to first free port from C000 up
    ///
    /// Example:
    ///   var socket = ZSocket.init(ZSocketType.Pair);
    ///   defer socket.deinit();
    ///
    ///   const port = try socket.bind("tcp://127.0.0.1:*");
    ///
    /// For more details, see https://libzmq.readthedocs.io/en/zeromq3-x/zmq_tcp.html
    pub fn bind(self: *ZSocket, ep: []const u8) !void {
        const epZ = try self.allocator_.dupeZ(u8, ep);
        defer self.allocator_.free(epZ);

        var result = c.zmq_bind(self.socket_, &epZ[0]);
        if (result < 0) {
            switch (c.zmq_errno()) {
                c.EINVAL => return error.EndpointInvalid,
                c.EPROTONOSUPPORT => return error.TransportUnsupported,
                c.ENOCOMPATPROTO => return error.TransportIncompatible,
                c.EADDRINUSE => return error.AddressAlreadyInUse,
                c.EADDRNOTAVAIL => return error.AddressNotLocal,
                c.ENODEV => return error.AddressInterfaceInvalid,
                c.EMTHREAD => return error.IOThreadsExceeded,
                else => return error.SocketBindFailed,
            }
        }
        errdefer _ = c.zmq_unbind(self.socket_, &epZ[0]);

        // retrieve endpoint value
        var lastEndpoint: [256]u8 = undefined;
        var lastEndpointLen: usize = @sizeOf(@TypeOf(lastEndpoint));

        lastEndpoint[0] = 0; // set 0 terminator

        result = c.zmq_getsockopt(self.socket_, c.ZMQ_LAST_ENDPOINT, &lastEndpoint[0], &lastEndpointLen);
        if (result < 0) {
            return error.GetEndpointFailed;
        }

        const selfAllocator = self.selfArena_.allocator();

        if (self.endpoint_) |e| { // free existing value
            selfAllocator.free(e);
        }

        if (lastEndpoint[0] != 0 and lastEndpointLen > 0) {
            self.endpoint_ = try selfAllocator.dupe(u8, lastEndpoint[0 .. lastEndpointLen - 1]); // cut terminating 0
        } else {
            self.endpoint_ = try selfAllocator.dupe(u8, ep); // copy to managed memory
        }
    }

    /// Connect a socket to an endpoint
    ///
    /// Example:
    ///   var socket = ZSocket.init(ZSocketType.Pair);
    ///   defer socket.deinit();
    ///
    ///   try socket.connect("tcp://127.0.0.1:54321");
    ///
    /// For more details, see https://libzmq.readthedocs.io/en/zeromq3-x/zmq_connect.html .
    pub fn connect(self: *ZSocket, ep: []const u8) !void {
        const epZ = try self.allocator_.dupeZ(u8, ep);
        defer self.allocator_.free(epZ);

        const result = c.zmq_connect(self.socket_, &epZ[0]);
        if (result < 0) {
            switch (c.zmq_errno()) {
                c.EINVAL => return error.EndpointInvalid,
                c.EPROTONOSUPPORT => return error.TransportUnsupported,
                c.ENOCOMPATPROTO => return error.TransportIncompatible,
                c.EMTHREAD => return error.IOThreadsExceeded,
                else => return error.SocketConnectFailed,
            }
        }

        // retrieve endpoint value
        const selfAllocator = self.selfArena_.allocator();

        if (self.endpoint_) |e| { // free existing value
            selfAllocator.free(e);
        }

        self.endpoint_ = try selfAllocator.dupe(u8, ep); // copy to managed memory
    }

    pub fn reconnect(self: *ZSocket) !void {
        if (self.endpoint_ == null) {
            return error.SocketNotConnected;
        }

        const epZ = try self.allocator_.dupeZ(u8, self.endpoint_.?);
        defer self.allocator_.free(epZ);

        var result = c.zmq_disconnect(self.socket_, &epZ[0]);
        if (result < 0) {
            return switch (c.zmq_errno()) {
                c.EINVAL => error.EndpointInvalid,
                c.ETERM => error.SocketTerminated,
                c.ENOTSOCK => error.InvalidSocket,
                c.ENOENT => error.EndpointInvalid,
                else => error.SocketDisconnectFailed,
            };
        }

        result = c.zmq_connect(self.socket_, &epZ[0]);
        if (result < 0) {
            switch (c.zmq_errno()) {
                c.EINVAL => return error.EndpointInvalid,
                c.EPROTONOSUPPORT => return error.TransportUnsupported,
                c.ENOCOMPATPROTO => return error.TransportIncompatible,
                c.EMTHREAD => return error.IOThreadsExceeded,
                else => return error.SocketConnectFailed,
            }
        }
    }

    /// Send a message to a socket.
    ///
    /// Note: The message can lose ownership and become invalid, even on failures!
    ///
    /// Example:
    ///       var message = try ZMessage.init(data);
    ///       defer message.deinit();
    ///
    ///       try socket.send(&message, .{});
    pub fn send(self: *ZSocket, message: *zmessage.ZMessage, options: struct {
        /// Indicates that this message is part of a multi-part message
        /// and more messages will be sent.
        ///
        /// On the receiving side, will cause `message.hasMore()` to return true.
        more: bool = false,

        /// Do not wait for the message to be sent, but return immediately.
        dontwait: bool = false,
    }) !void {
        var flags: c_int = 0;
        if (options.more) flags |= c.ZMQ_SNDMORE;
        if (options.dontwait) flags |= c.ZMQ_DONTWAIT;

        var messageExt = try message.allocExternal();
        defer messageExt.deinit();

        const result = c.zmq_msg_send(try messageExt.move(), self.socket_, flags);
        if (result < 0) {
            messageExt.unmove(); // re-transfer ownership, because `zmq_msg_send()` failed

            const errno = c.zmq_errno();
            switch (errno) {
                c.EAGAIN => return error.NonBlockingQueueFull,
                c.ENOTSUP => return error.SocketTypeUnsupported,
                c.EFSM => return error.SocketStateInvalid,
                c.EINTR => return error.Interrupted,
                c.EFAULT => return error.MessageInvalid,
                else => return error.SendMessageFailed,
            }
        }
    }

    /// Receive a single message of a message from the socket.
    /// Does a blocking recv, if you want to not block then use
    /// zpoller or zloop.
    ///
    /// The caller must invoke `deinit()` on the returned message.
    ///
    /// If receiving a multi-part message, then `message.hasMore()` will return true
    /// and the another receive call should be invoked.
    ///
    /// Example:
    ///       var message = try socket.receive(.{});
    ///       defer message.deinit();
    ///
    ///       const data = try message.data();
    pub fn receive(self: *ZSocket, options: struct {
        /// Do not wait for the message to be received, but return immediately.
        dontwait: bool = false,
    }) !ZMessageReceived {
        var flags: c_int = 0;
        if (options.dontwait) flags |= c.ZMQ_DONTWAIT;

        var message: c.zmq_msg_t = undefined;
        var result = c.zmq_msg_init(&message);
        if (result < 0) {
            return error.InitMessageFailed;
        }

        result = c.zmq_msg_recv(&message, self.socket_, flags);
        if (result < 0) {
            switch (c.zmq_errno()) {
                c.EAGAIN => return error.NonBlockingQueueEmpty,
                c.ENOTSUP => return error.SocketTypeUnsupported,
                c.EFSM => return error.SocketStateInvalid,
                c.EINTR => return error.Interrupted,
                c.EFAULT => return error.MessageInvalid,
                else => return error.ReceiveMessageFailed,
            }
        }

        // check if this is a multi-part message
        var hasMore: c_int = undefined;
        var hasMoreLen: usize = @sizeOf(@TypeOf(hasMore));

        result = c.zmq_getsockopt(self.socket_, c.ZMQ_RCVMORE, &hasMore, &hasMoreLen);
        if (result < 0) {
            return error.CheckForMoreFailed;
        }

        return ZMessageReceived.init(&message, hasMore != 0);
    }

    pub fn receiveIterate(self: *ZSocket) !ReceiveIterator {
        var frame = try self.receive(.{});
        const msg = try frame.message_.allocExternal();

        return .{ 
            .socket_ = self, 
            .current_ = msg,
            .prev_ = msg,
            .need_receve = false,
        };
    }

    pub const ReceiveIterator = struct {
        socket_: *ZSocket,
        current_: ?zmessage.ZMessageExternal,
        prev_: zmessage.ZMessageExternal,
        need_receve: bool,

        pub fn next(self: *ReceiveIterator) !?zmessage.ZMessage {
            if (self.current_) |*msg| {
                if (self.need_receve) {
                    const err = c.zmq_msg_recv(&msg.msg_, self.socket_.socket_, 0);
                    if (err < 0) {
                        return error.ReceiveMessageFailed;
                    }
                }

                const result: zmessage.ZMessage = .{ 
                    .impl_ = .{ .External = msg.* } 
                };

                self.prev_ = msg.*;
                self.need_receve = true;
                
                if (c.zmq_msg_more(&msg.msg_) == 0) {
                    self.current_ = null;
                }

                return result;
            } else {
                return null;
            }
        }

        pub fn deinit(self: *ReceiveIterator) void {
            _ = c.zmq_msg_close(&self.prev_.msg_);
        }
    };

    /// Set an option on the socket. See `ZSocketOption` for details.
    pub fn setSocketOption(self: *ZSocket, opt: ZSocketOption) !void {
        var result: c_int = 0;

        switch (opt) {
            .ReceiveTimeout => {
                result = c.zmq_setsockopt(self.socket_, c.ZMQ_RCVTIMEO, &opt.ReceiveTimeout, @sizeOf(@TypeOf(opt.ReceiveTimeout)));
            },
            .ReceiveHighWaterMark => {
                result = c.zmq_setsockopt(self.socket_, c.ZMQ_RCVHWM, &opt.ReceiveHighWaterMark, @sizeOf(@TypeOf(opt.ReceiveHighWaterMark)));
            },
            .ReceiveBufferSize => {
                result = c.zmq_setsockopt(self.socket_, c.ZMQ_RCVBUF, &opt.ReceiveBufferSize, @sizeOf(@TypeOf(opt.ReceiveBufferSize)));
            },

            .SendTimeout => {
                result = c.zmq_setsockopt(self.socket_, c.ZMQ_SNDTIMEO, &opt.SendTimeout, @sizeOf(@TypeOf(opt.SendTimeout)));
            },
            .SendHighWaterMark => {
                result = c.zmq_setsockopt(self.socket_, c.ZMQ_SNDHWM, &opt.SendHighWaterMark, @sizeOf(@TypeOf(opt.SendHighWaterMark)));
            },
            .SendBufferSize => {
                result = c.zmq_setsockopt(self.socket_, c.ZMQ_SNDBUF, &opt.SendBufferSize, @sizeOf(@TypeOf(opt.SendBufferSize)));
            },

            .LingerTimeout => {
                result = c.zmq_setsockopt(self.socket_, c.ZMQ_LINGER, &opt.LingerTimeout, @sizeOf(@TypeOf(opt.LingerTimeout)));
            },

            .RoutingId => {
                result = c.zmq_setsockopt(self.socket_, c.ZMQ_ROUTING_ID, opt.RoutingId.ptr, opt.RoutingId.len);
            },
            .RouterHandover => {
                const v: c_int = @intFromBool(opt.RouterHandover);

                result = c.zmq_setsockopt(self.socket_, c.ZMQ_ROUTER_HANDOVER, &v, @sizeOf(@TypeOf(v)));
            },
            .Subscribe => |v| {
                result = c.zmq_setsockopt(self.socket_, c.ZMQ_SUBSCRIBE, v.ptr, v.len);
            },
            .Unsubscribe => |v| {
                result = c.zmq_setsockopt(self.socket_, c.ZMQ_UNSUBSCRIBE, v.ptr, v.len);
            },
            .SocketType => {
                return error.UnknownOption;
            },
            //else => return error.UnknownOption,
        }

        if (result < 0) {
            switch (c.zmq_errno()) {
                c.EINVAL => return error.OptionOrValueInvalid,
                c.ETERM => return error.ZContextTerminated,
                c.ENOTSOCK => return error.SocketInvalid,
                c.EINTR => return error.Interrupted,
                else => return error.SetFailed,
            }
        }
    }

    /// Get an option of the socket. See `ZSocketOption` for details.
    pub fn getSocketOption(self: *ZSocket, opt: *ZSocketOption) !void {
        var result: c_int = 0;

        switch (opt.*) {
            .ReceiveTimeout => {
                var length: usize = @sizeOf(@TypeOf(opt.ReceiveTimeout));

                result = c.zmq_getsockopt(self.socket_, c.ZMQ_RCVTIMEO, &opt.ReceiveTimeout, &length);
            },
            .ReceiveHighWaterMark => {
                var length: usize = @sizeOf(@TypeOf(opt.ReceiveHighWaterMark));

                result = c.zmq_getsockopt(self.socket_, c.ZMQ_RCVHWM, &opt.ReceiveHighWaterMark, &length);
            },
            .ReceiveBufferSize => {
                var length: usize = @sizeOf(@TypeOf(opt.ReceiveBufferSize));

                result = c.zmq_getsockopt(self.socket_, c.ZMQ_RCVBUF, &opt.ReceiveBufferSize, &length);
            },

            .SendTimeout => {
                var length: usize = @sizeOf(@TypeOf(opt.SendTimeout));

                result = c.zmq_getsockopt(self.socket_, c.ZMQ_SNDTIMEO, &opt.SendTimeout, &length);
            },
            .SendHighWaterMark => {
                var length: usize = @sizeOf(@TypeOf(opt.SendHighWaterMark));

                result = c.zmq_getsockopt(self.socket_, c.ZMQ_SNDHWM, &opt.SendHighWaterMark, &length);
            },
            .SendBufferSize => {
                var length: usize = @sizeOf(@TypeOf(opt.SendBufferSize));

                result = c.zmq_getsockopt(self.socket_, c.ZMQ_SNDBUF, &opt.SendBufferSize, &length);
            },

            .LingerTimeout => {
                var length: usize = @sizeOf(@TypeOf(opt.LingerTimeout));

                result = c.zmq_getsockopt(self.socket_, c.ZMQ_LINGER, &opt.LingerTimeout, &length);
            },

            .RoutingId => {
                result = c.zmq_getsockopt(self.socket_, c.ZMQ_ROUTING_ID, opt.RoutingId.ptr, &opt.RoutingId.len);
            },
            .RouterHandover => {
                return error.UnknownOption; // ZMQ_ROUTER_HANDOVER cannot be retrieved
            },
            .SocketType => {
                var socket_type: c_int = undefined;
                var length: usize = @sizeOf(@TypeOf(socket_type));
                result = c.zmq_getsockopt(self.socket_, c.ZMQ_TYPE, &socket_type, &length);
                if (result == 0) {
                    opt.*.SocketType = @enumFromInt(socket_type);
                }
            },
            .Subscribe => {
                return error.UnknownOption;
            },
            .Unsubscribe => {
                return error.UnknownOption;
            },

            //else => return error.UnknownOption,
        }

        if (result < 0) {
            switch (c.zmq_errno()) {
                c.EINVAL => return error.OptionOrValueInvalid,
                c.ETERM => return error.ZContextTerminated,
                c.ENOTSOCK => return error.SocketInvalid,
                c.EINTR => return error.Interrupted,
                else => return error.GetFailed,
            }
        }
    }

    /// Destroy the socket and clean up
    pub fn deinit(self: *ZSocket) void {
        _ = c.zmq_close(self.socket_);

        // clean-up arena
        var arena = self.selfArena_; // prevent seg fault
        arena.deinit();
    }
};

test "ZSocket - roundtrip single" {
    const allocator = std.testing.allocator;

    // create the context
    var context = try zcontext.ZContext.init(allocator);
    defer context.deinit();

    // bind the incoming socket
    var incoming = try ZSocket.init(ZSocketType.Pair, &context);
    defer incoming.deinit();

    try incoming.bind("tcp://127.0.0.1:*");
    try std.testing.expect(incoming.endpoint_ != null);

    std.log.info("Endpoint: {s}", .{try incoming.endpoint()});

    // connect to the socket
    var outgoing = try ZSocket.init(ZSocketType.Pair, &context);
    defer outgoing.deinit();

    try outgoing.connect(try incoming.endpoint());
    try std.testing.expect(outgoing.endpoint_ != null);

    // send a message
    const msg = "hello world";

    var outgoingData = try zmessage.ZMessage.initUnmanaged(msg, null);
    defer outgoingData.deinit();
    try std.testing.expectEqual(msg.len, try outgoingData.size());
    try std.testing.expectEqualStrings(msg, try outgoingData.data());

    // send the message
    try outgoing.send(&outgoingData, .{ .dontwait = true });

    // receive the message
    var incomingData = try incoming.receive(.{});
    defer incomingData.deinit();

    try std.testing.expectEqual(msg.len, try incomingData.size());
    try std.testing.expectEqualStrings(msg, try incomingData.data());
    try std.testing.expectEqual(false, incomingData.hasMore());
}

test "ZSocket - roundtrip multi-part" {
    const allocator = std.testing.allocator;

    // create the context
    var context = try zcontext.ZContext.init(allocator);
    defer context.deinit();

    // bind the incoming socket
    var incoming = try ZSocket.init(ZSocketType.Pair, &context);
    defer incoming.deinit();

    try incoming.bind("tcp://127.0.0.1:*");
    try std.testing.expect(incoming.endpoint_ != null);

    std.log.info("Endpoint: {s}", .{try incoming.endpoint()});

    // connect to the socket
    var outgoing = try ZSocket.init(ZSocketType.Pair, &context);
    defer outgoing.deinit();

    try outgoing.connect(try incoming.endpoint());
    try std.testing.expect(outgoing.endpoint_ != null);

    // send a message
    const msg = "hello world";

    var outgoingData = try zmessage.ZMessage.initUnmanaged(msg, null);
    defer outgoingData.deinit();
    try std.testing.expectEqual(msg.len, try outgoingData.size());
    try std.testing.expectEqualStrings(msg, try outgoingData.data());

    // send the first message
    try outgoing.send(&outgoingData, .{ .dontwait = true, .more = true });

    // send the second message (reusing the previous one)
    try outgoing.send(&outgoingData, .{ .dontwait = true });

    // receive the first message of the message
    var incomingData = try incoming.receive(.{});
    defer incomingData.deinit();

    try std.testing.expectEqual(msg.len, try incomingData.size());
    try std.testing.expectEqualStrings(msg, try incomingData.data());
    try std.testing.expectEqual(true, incomingData.hasMore());

    // receive the second message
    var incomingData2 = try incoming.receive(.{});
    defer incomingData2.deinit();

    try std.testing.expectEqual(msg.len, try incomingData2.size());
    try std.testing.expectEqualStrings(msg, try incomingData2.data());
    try std.testing.expectEqual(false, incomingData2.hasMore());
}

test "ZSocket - roundtrip json" {
    const allocator = std.testing.allocator;

    // create the context
    var context = try zcontext.ZContext.init(allocator);
    defer context.deinit();

    // bind the incoming socket
    var incoming = try ZSocket.init(ZSocketType.Pair, &context);
    defer incoming.deinit();

    try incoming.bind("tcp://127.0.0.1:*");

    std.log.info("Endpoint: {s}", .{try incoming.endpoint()});

    // connect to the socket
    var outgoing = try ZSocket.init(ZSocketType.Pair, &context);
    defer outgoing.deinit();

    try outgoing.connect(try incoming.endpoint());

    // send a message
    const Obj = struct {
        hello: []const u8,
        everything: i32,
        ten: []const u16,
    };

    const outgoingObj = Obj{
        .hello = "world",
        .everything = 42,
        .ten = &[_]u16{
            0,
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
        },
    };

    const msg = try std.json.stringifyAlloc(allocator, &outgoingObj, .{});
    defer allocator.free(msg);

    var outgoingData = try zmessage.ZMessage.initUnmanaged(msg, null);
    defer outgoingData.deinit();

    try outgoing.send(&outgoingData, .{ .dontwait = true });

    // receive the first message of the message
    var incomingData = try incoming.receive(.{});
    defer incomingData.deinit();

    const incomingObj = try std.json.parseFromSlice(Obj, allocator, try incomingData.data(), .{});
    defer incomingObj.deinit();

    try std.testing.expectEqualDeep(outgoingObj, incomingObj.value);
}

test "ZSocket - receive timeout" {
    const allocator = std.testing.allocator;

    // create the context
    var context = try zcontext.ZContext.init(allocator);
    defer context.deinit();

    // create the socket
    var incoming = try ZSocket.init(ZSocketType.Rep, &context);
    defer incoming.deinit();

    // set the receive timeout
    {
        var timeout = ZSocketOption{ .ReceiveTimeout = undefined };
        try incoming.getSocketOption(&timeout);
        try std.testing.expectEqual(@as(i32, -1), timeout.ReceiveTimeout);
    }

    try incoming.setSocketOption(.{ .ReceiveTimeout = 500 });

    {
        var timeout = ZSocketOption{ .ReceiveTimeout = undefined };
        try incoming.getSocketOption(&timeout);
        try std.testing.expectEqual(@as(i32, 500), timeout.ReceiveTimeout);
    }

    // bind the port
    try incoming.bind("tcp://127.0.0.1:*");

    std.log.info("Endpoint: {s}", .{try incoming.endpoint()});

    // try to receive the message
    try std.testing.expectError(error.NonBlockingQueueEmpty, incoming.receive(.{}));
}

test "ZSocket - send timeout" {
    const allocator = std.testing.allocator;

    // create the context
    var context = try zcontext.ZContext.init(allocator);
    defer context.deinit();

    // create the socket
    var socket = try ZSocket.init(ZSocketType.Pair, &context);
    defer socket.deinit();

    // set the send timeout
    {
        var timeout = ZSocketOption{ .SendTimeout = undefined };
        try socket.getSocketOption(&timeout);
        try std.testing.expectEqual(@as(i32, -1), timeout.SendTimeout);
    }

    try socket.setSocketOption(.{ .SendTimeout = 500 });

    {
        var timeout = ZSocketOption{ .SendTimeout = undefined };
        try socket.getSocketOption(&timeout);
        try std.testing.expectEqual(@as(i32, 500), timeout.SendTimeout);
    }

    // bind the port
    try socket.bind("tcp://127.0.0.1:*");

    std.log.info("Endpoint: {s}", .{try socket.endpoint()});

    // try to send the message
    var message = try zmessage.ZMessage.initExternalEmpty();
    defer message.deinit();

    try std.testing.expectError(error.NonBlockingQueueFull, socket.send(&message, .{}));

    // try again, the owner should be lost
    try std.testing.expectError(error.MessageOwnershipLost, socket.send(&message, .{}));
}

test "ZSocket - routing id" {
    const allocator = std.testing.allocator;

    // create the context
    var context = try zcontext.ZContext.init(allocator);
    defer context.deinit();

    // create the socket
    var socket = try ZSocket.init(ZSocketType.Router, &context);
    defer socket.deinit();

    // set the routing id
    {
        var v = ZSocketOption{ .RoutingId = undefined };
        try socket.getSocketOption(&v);
        try std.testing.expectEqualStrings("", v.RoutingId);
    }

    try socket.setSocketOption(.{ .RoutingId = @constCast("myRoutingID") });

    {
        var routingId: [255]u8 = undefined;

        var v = ZSocketOption{ .RoutingId = &routingId };
        try socket.getSocketOption(&v);
        try std.testing.expectEqualStrings("myRoutingID", v.RoutingId);
    }

    // set the router handover
    try socket.setSocketOption(.{ .RouterHandover = true });

    {
        var v = ZSocketOption{ .RouterHandover = undefined };
        try std.testing.expectError(error.UnknownOption, socket.getSocketOption(&v));
    }
}

test "ZSocket - PubSub" {
    const allocator = std.testing.allocator;

    // create the context
    var context = try zcontext.ZContext.init(allocator);
    defer context.deinit();

    const endpoint = "inproc://#0";

    // bind the publisher socket
    var publisher = try ZSocket.init(ZSocketType.Pub, &context);
    defer publisher.deinit();

    try publisher.bind(endpoint);
    try std.testing.expect(publisher.endpoint_ != null);

    // connect to the subscriber socket
    var subscriber_a = sub: {
        const socket = try ZSocket.init(ZSocketType.Sub, &context);
        try socket.setSocketOption(.{.Subscribe = ""});

        try socket.connect(endpoint);
        try std.testing.expect(socket.endpoint_ != null);
        break :sub socket;
    };
    defer subscriber_a.deinit();

    var subscriber_b = sub: {
        const socket = try ZSocket.init(ZSocketType.Sub, &context);
        try socket.setSocketOption(.{.Subscribe = ""});

        try socket.connect(endpoint);
        try std.testing.expect(socket.endpoint_ != null);
        break :sub socket;
    };
    defer subscriber_b.deinit();

    const msg = "Hello world";

    var data = try zmessage.ZMessage.initUnmanaged(msg, null);
    defer data.deinit();

    try publisher.send(&data, .{.dontwait = true});

    var frame_a = try subscriber_a.receive(.{});
    defer frame_a.deinit();

    try std.testing.expectEqual(msg.len, try frame_a.size());
    try std.testing.expectEqualStrings(msg, try frame_a.data());
    try std.testing.expectEqual(false, frame_a.hasMore());

    var frame_b = try subscriber_b.receive(.{});
    defer frame_b.deinit();

    try std.testing.expectEqual(msg.len, try frame_b.size());
    try std.testing.expectEqualStrings(msg, try frame_b.data());
    try std.testing.expectEqual(false, frame_b.hasMore());
}

pub fn makeProxy(frontend_socket: *ZSocket, backend_socket: *ZSocket, capture_socket: ?*ZSocket) void {
    _ = c.zmq_proxy(frontend_socket.socket_, backend_socket.socket_, capture_socket orelse null);
}

test "ZSocket - PubSub with filter" {
    const allocator = std.testing.allocator;

    // create the context
    var context = try zcontext.ZContext.init(allocator);
    defer context.deinit();

    const endpoint = "inproc://#0";

    // bind the publisher socket
    var publisher = try ZSocket.init(ZSocketType.Pub, &context);
    defer publisher.deinit();

    try publisher.bind(endpoint);
    try std.testing.expect(publisher.endpoint_ != null);

    // connect to the subscriber socket
    var subscriber_a = sub: {
        const socket = try ZSocket.init(ZSocketType.Sub, &context);
        try socket.setSocketOption(.{.Subscribe = @constCast("Main")});

        try socket.connect(endpoint);
        try std.testing.expect(socket.endpoint_ != null);
        break :sub socket;
    };
    defer subscriber_a.deinit();

    var subscriber_b = sub: {
        const socket = try ZSocket.init(ZSocketType.Sub, &context);
        try socket.setSocketOption(.{.Subscribe = @constCast("Sub")});

        try socket.connect(endpoint);
        try std.testing.expect(socket.endpoint_ != null);
        break :sub socket;
    };
    defer subscriber_b.deinit();

    var topic_a = try zmessage.ZMessage.initUnmanaged("Main:Hello world", null);
    defer topic_a.deinit();
    try publisher.send(&topic_a, .{.dontwait = true});

    var topic_b = try zmessage.ZMessage.initUnmanaged("Sub:Hello world", null);
    defer topic_b.deinit();
    try publisher.send(&topic_b, .{.dontwait = true});

    var frame_a = try subscriber_a.receive(.{});
    defer frame_a.deinit();

    try std.testing.expectEqual(topic_a.size(), try frame_a.size());
    try std.testing.expectEqualStrings(try topic_a.data(), try frame_a.data());
    try std.testing.expectEqual(false, frame_a.hasMore());

    var frame_b = try subscriber_b.receive(.{});
    defer frame_b.deinit();

    try std.testing.expectEqual(topic_b.size(), try frame_b.size());
    try std.testing.expectEqualStrings(try topic_b.data(), try frame_b.data());
    try std.testing.expectEqual(false, frame_b.hasMore());
}

test "ZSocket - Receive iterate more" {
    const allocator = std.testing.allocator;

    // create the context
    var context = try zcontext.ZContext.init(allocator);
    defer context.deinit();

    // bind the incoming socket
    var incoming = try ZSocket.init(ZSocketType.Pair, &context);
    defer incoming.deinit();
    try incoming.bind("tcp://127.0.0.1:*");
    try std.testing.expect(incoming.endpoint_ != null);

    std.log.info("Endpoint: {s}", .{try incoming.endpoint()});

    // connect to the socket
    var outgoing = try ZSocket.init(ZSocketType.Pair, &context);
    defer outgoing.deinit();

    try outgoing.connect(try incoming.endpoint());
    try std.testing.expect(outgoing.endpoint_ != null);

    const msg_1 = "Greetings:";
    const msg_2 = "Hello";
    const msg_3 = "world!";

    send: {
        var outgoingData = try zmessage.ZMessage.initUnmanaged(msg_1, null);
        defer outgoingData.deinit();
        try outgoing.send(&outgoingData, .{ .dontwait = true, .more = true });
        break :send;
    }
    send: {
        var outgoingData = try zmessage.ZMessage.initUnmanaged(msg_2, null);
        defer outgoingData.deinit();
        try outgoing.send(&outgoingData, .{ .dontwait = true, .more = true });
        break :send;
    }
    send: {
        var outgoingData = try zmessage.ZMessage.initUnmanaged(msg_3, null);
        defer outgoingData.deinit();
        try outgoing.send(&outgoingData, .{ .dontwait = true, .more = false });
        break :send;
    }

    var it = try incoming.receiveIterate();
    defer it.deinit();

    var msg = try it.next();
    try std.testing.expect(msg != null);
    var data = try msg.?.data();
    try std.testing.expectEqual(msg_1.len, data.len);
    try std.testing.expectEqualStrings(msg_1, data);

    msg = try it.next();
    try std.testing.expect(msg != null);
    data = try msg.?.data();
    try std.testing.expectEqual(msg_2.len, data.len);
    try std.testing.expectEqualStrings(msg_2, data);

    msg = try it.next();
    try std.testing.expect(msg != null);
    data = try msg.?.data();
    try std.testing.expectEqual(msg_3.len, data.len);
    try std.testing.expectEqualStrings(msg_3, data);

    msg = try it.next();
    try std.testing.expect(msg == null);
}