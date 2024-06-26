const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");

const Setting = @import("./Setting.zig");
const CodeBuilder = @import("./CodeBuilder.zig");

const APP_CONTEXT = "generate-ts";
const Self = @This();

// TODO provide from CLI args
const PREFIX = "./_dump/ts";

allocator: std.mem.Allocator,
context: zmq.ZContext,
connection: *core.sockets.Connection.Client(void),
logger: core.Logger,

pub fn init(allocator: std.mem.Allocator, setting: Setting) !Self {
    var ctx = try zmq.ZContext.init(allocator);

    var connection = try core.sockets.Connection.Client(void).init(allocator, &ctx);
    try connection.subscribe_socket.addFilters(.{
        .ready_topic_body = true,
        .topic_body = true,
        .quit_all = true,
        .quit = true,
    });
    try connection.connect(setting.endpoints);

    return .{
        .allocator = allocator,
        .context = ctx,
        .connection = connection,
        .logger = core.Logger.init(allocator, APP_CONTEXT, connection.dispatcher, false),
    };
}

pub fn deinit(self: *Self) void {
    self.connection.deinit();
    self.context.deinit();
}

pub fn run(self: *Self, setting: Setting) !void {
    try self.logger.log(.info, "Beginning...", .{});
    try self.logger.log(.debug, "Subscriber filters: {}", .{self.connection.subscribe_socket.listFilters()});

    dump_setting: {
        try self.logger.log(.debug, "CLI: Req/Rep Channel = {s}", .{setting.endpoints.req_rep});
        try self.logger.log(.debug, "CLI: Pub/Sub Channel = {s}", .{setting.endpoints.pub_sub});
        break :dump_setting;
    }

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
                .topic_body => |source| {
                    try self.connection.dispatcher.approve();

                    try self.logger.log(.trace, "Accept source: `{s}`", .{source.header.path});
                    try self.logger.log(.trace, "Begin generate: `{s}`", .{source.header.name});

                    var output_dir = try std.fs.cwd().makeOpenPath(PREFIX, .{});
                    defer output_dir.close();

                    var builder = try CodeBuilder.init(self.allocator, output_dir, source.header.name);
                    defer builder.deinit();

                    var walker = try CodeBuilder.Parser.beginParse(self.allocator, source.bodies);
                    defer walker.deinit();

                    while (try walker.walk()) |target| switch (target) {
                        .query => |q| {
                            try builder.applyQuery(q);
                        },
                        .parameter => |placeholder| {
                            try builder.applyPlaceholder(placeholder);
                        },
                        .result_set => |field_types| {
                            try builder.applyResultSets(field_types);
                        },
                    };

                    try builder.build();
                    try self.logger.log(.trace, "End generate: `{s}`", .{source.header.name});

                    try self.connection.dispatcher.post(.ready_generate);
                },
                .quit, .quit_all => {
                    try self.connection.dispatcher.post(.{
                        .quit_accept = try core.EventPayload.Stage.init(self.allocator, APP_CONTEXT),
                    });
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
