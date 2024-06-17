const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");

const APP_CONTEXT = "generate-ts";
const Self = @This();

allocator: std.mem.Allocator,
context: zmq.ZContext,
connection: *core.sockets.Connection.Client,
logger: core.Logger,

pub fn init(allocator: std.mem.Allocator) !Self {
    var ctx = try zmq.ZContext.init(allocator);

    var connection = try core.sockets.Connection.Client.init(allocator, &ctx);
    try connection.subscribe_socket.addFilters(.{
        .ready_topic_body = true,
        .topic_body = true,
        .quit_all = true,
        .quit = true,
    });
    try connection.connect();

    return .{
        .allocator = allocator,
        .context = ctx,
        .connection = connection,
        .logger = core.Logger.init(allocator, APP_CONTEXT, connection, false),
    };
}

pub fn deinit(self: *Self) void {
    self.connection.deinit();
    self.context.deinit();
}

pub fn run(self: *Self) !void {
    try self.logger.log(.info, "Beginning...", .{});
    try self.logger.log(.debug, "Subscriber filters: {}", .{self.connection.subscribe_socket.listFilters()});

    launch: {
        try self.connection.dispatcher.post(.{
            .launched = try core.EventPayload.Stage.init(self.allocator, APP_CONTEXT),
        });
        break :launch;
    }

    var current_source: ?Source = null;

    while (self.connection.dispatcher.isReady()) {
        const _item = self.connection.dispatcher.dispatch() catch |err| switch (err) {
            error.InvalidResponse => {
                try self.logger.log(.warn, "Unexpected data received", .{});
                continue;
            },
            else => return err,
        };

        if (_item) |*item| {
            defer item.deinit();

            switch (item.event) {
                .ready_topic_body => {
                    if (current_source) |*source| {
                        source.deinit();
                        current_source = null;
                    }
                    try self.logger.log(.err, "Ready for generating", .{});
                    try self.connection.dispatcher.post(.ready_generate);
                },
                .topic_body => |_| {
                    // TODO コード生成
                    try self.connection.dispatcher.approve();
                    try self.logger.log(.err, "Code generation not implemented", .{});
                    try self.connection.dispatcher.post(.ready_generate);
                },
                .quit_all => {
                    try self.connection.dispatcher.post(.{
                        .quit_accept = try core.EventPayload.Stage.init(self.allocator, APP_CONTEXT),
                    });
                    try self.connection.dispatcher.done();
                },
                .quit => {
                    try self.connection.dispatcher.approve();
                    try self.connection.dispatcher.post(.{
                        .quit_accept = try core.EventPayload.Stage.init(self.allocator, APP_CONTEXT),
                    });
                    try self.connection.dispatcher.done();
                },
                else => {
                    try self.logger.log(.warn, "Discard command: {}", .{std.meta.activeTag(item.event)});
                },
            }
        }
    }
}

const Source = struct {
    header: core.EventPayload.SourcePath,
    bodies: std.BufMap,

    pub fn init(allocator: std.mem.Allocator, header: core.EventPayload.SourcePath) !Source {
        return .{
            .header = try header.clone(allocator),
            .bodies = std.BufMap.init(allocator),
        };
    }

    pub fn deinit(self: *Source) void {
        self.header.deinit();
        self.bodies.deinit();
        self.* = undefined;
    }

    pub fn addPayload(self: *Source, body: core.EventPayload.SourceBody) !void {
        try self.bodies.put(body.topic, body.content);
    }
};