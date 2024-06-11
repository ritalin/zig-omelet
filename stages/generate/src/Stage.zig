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
        .topic_payload = true,
        .next_generate = true,
        .end_generate = true,
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
    try self.logger.log(.info, "Beginning", .{});
    try self.logger.log(.debug, "Subscriber filters: {}", .{self.connection.subscribe_socket.listFilters()});

    launch: {
        try self.connection.dispatcher.post(.{.launched = .{.stage_name = APP_CONTEXT} });
        break :launch;
    }

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
                .end_generate => {
                    // TODO
                    try self.logger.log(.err, "Not implemented... (.end_generate)", .{});
                    try self.connection.dispatcher.post(.finished);
                },
                .quit_all => {
                    try self.connection.dispatcher.post(.{.quit_accept = .{ .stage_name = APP_CONTEXT }});
                    try self.connection.dispatcher.done();
                },
                .quit => {
                    try self.connection.dispatcher.approve();
                    try self.connection.dispatcher.post(.{.quit_accept = .{ .stage_name = APP_CONTEXT }});
                    try self.connection.dispatcher.done();
                },
                else => {
                    try self.logger.log(.warn, "Discard command: {}", .{std.meta.activeTag(item.event)});
                },
            }
        }
    }
}
