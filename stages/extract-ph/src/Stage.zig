const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");

const APP_CONTEXT = "exctract-ph";
const Self = @This();

// const SQL = "select $id::bigint, $name::varchar from foo where kind = $kind::int";
const SQL = "select $1, $2 from foo where kind = $3";
const PH = "[{\"field\":\"id\", \"type_name\": \"bigint\"}, (cont)...]";

allocator: std.mem.Allocator,
context: zmq.ZContext,
connection: *core.sockets.Connection.Client,
logger: core.Logger,

pub fn init(allocator: std.mem.Allocator) !Self {
    var ctx = try zmq.ZContext.init(allocator);

    var connection = try core.sockets.Connection.Client.init(allocator, &ctx);
    try connection.subscribe_socket.addFilters(.{
        .request_topic = true,
        .source_path = true,
        .end_watch_path = true,
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

    var topic_queue = core.Queue(core.EventPayload.TopicBody).init(self.allocator);
    defer topic_queue.deinit();

    while (self.connection.dispatcher.isReady()) {
        const _item = self.connection.dispatcher.dispatch() catch |err| switch (err) {
            error.InvalidResponse => {
                try self.logger.log(.warn, "Unexpected data received", .{});
                continue;
            },
            else => return err,
        };

        if (_item) |item| {
            defer item.deinit();
            
            switch (item.event) {
                .request_topic => {
                    const topic = try core.EventPayload.Topic.init(
                        self.allocator, &.{"query", "placeholder"}
                    );
                    
                    topics: {
                        try self.connection.dispatcher.post(.{.topic = topic});
                        break :topics;
                    }
                },
                .source_path => |path| {
                    try self.logger.log(.err, "SQL parse not implemented... (.source)", .{});
                    // TODO
                    const query = SQL;
                    const placeholder = PH;

                    const topic_body = try core.EventPayload.TopicBody.init(
                        self.allocator,
                        path,
                        &.{.{"query", query}, .{"placeholder", placeholder}}
                    );

                    try self.connection.dispatcher.post(.{.topic_body = topic_body});
                },
                .end_watch_path => {
                    // TODO 次のパース
                    try self.connection.dispatcher.post(.finish_topic_body);
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
