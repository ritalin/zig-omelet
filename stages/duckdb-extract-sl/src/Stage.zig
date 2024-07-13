const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");

const Setting = @import("./Setting.zig");
const app_context = @import("build_options").app_context;
const c = @import("./duckdb_worker.zig");
const ExtractWorker = @import("./ExtractWorker.zig");

const Symbol = core.Symbol;
const Connection = core.sockets.Connection.Client(app_context, ExtractWorker);
const Logger = core.Logger.withAppContext(app_context);

const Self = @This();

allocator: std.mem.Allocator,
context: *zmq.ZContext,
connection: *Connection,
logger: Logger,
db: c.DatabaseRef,

pub fn init(allocator: std.mem.Allocator, setting: Setting) !Self {
    const ctx = try allocator.create(zmq.ZContext);
    ctx.* = try zmq.ZContext.init(allocator);

    var connection = try Connection.init(allocator, ctx);
    try connection.subscribe_socket.addFilters(.{
        .request_topic = true,
        .source_path = true,
        .finish_source_path = true,
        .quit_all = true,
        .quit = true,
    });
    try connection.connect(setting.endpoints);

    const logger = Logger.init(allocator, connection.dispatcher, setting.standalone);
    
    var database: c.DatabaseRef = undefined;
    _ = c.initDatabase(&database);

    return .{
        .allocator = allocator,
        .context = ctx,
        .connection = connection,
        .logger = logger,
        .db = database,
    };
}

pub fn deinit(self: *Self) void {
    c.deinitDatabase(self.db);
    self.connection.deinit();
    self.context.deinit();
    self.allocator.destroy(self.context);
}

pub fn run(self: *Self, setting: Setting) !void {
    try self.logger.log(.debug, "Beginning...", .{});
    try self.logger.log(.debug, "Subscriber filters: {}", .{self.connection.subscribe_socket.listFilters()});

    dump_setting: {
        try self.logger.log(.debug, "CLI: Req/Rep Channel = {s}", .{setting.endpoints.req_rep});
        try self.logger.log(.debug, "CLI: Pub/Sub Channel = {s}", .{setting.endpoints.pub_sub});
        break :dump_setting;
    }
    
    launch: {
        if (try self.tryLoadSchema(setting.schema_dir_path)) {
            try self.connection.dispatcher.post(.launched);
        }
        else {
            try self.connection.dispatcher.post(.failed_launching);
        }
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

        if (_item) |item| {
            defer item.deinit();
            
            switch (item.event) {
                .request_topic => {
                    const topic = try core.Event.Payload.Topic.init(
                        self.allocator, &.{c.topic_select_list}
                    );
                    
                    topics: {
                        try self.connection.dispatcher.post(.{.topic = topic});
                        break :topics;
                    }
                },
                .source_path => |path| {
                    try self.logger.log(.debug, "Accept source path: {s}", .{path.path});
                    try self.logger.log(.trace, "Begin worker process", .{});

                    const worker = try ExtractWorker.init(self.allocator, path.path);
                    defer worker.deinit();
                    const payload = try worker.run(try self.connection.pull_sink_socket.workerSocket());
                    defer self.allocator.free(payload);

                    const event: core.Event = .{
                        .topic_body = try core.Event.Payload.TopicBody.init(
                            self.allocator,
                            path.values(),
                            &.{ .{c.topic_select_list, payload} }
                        ),
                    };

                    try self.connection.dispatcher.post(event);
                    try self.logger.log(.trace, "End worker process", .{});
                },
                .finish_source_path => {
                    // if (body_lookup.count() == 0) 
                    {
                        try self.connection.dispatcher.post(.finish_topic_body);
                    }
                        try self.connection.dispatcher.state.receiveTerminate();
                },
                .quit => {
                    // if (body_lookup.count() == 0) 
                    {
                        try self.connection.dispatcher.quitAccept();
                    }
                },
                .quit_all => {
                    try self.connection.dispatcher.quitAccept();
                    try self.connection.pull_sink_socket.stop();
                },
                else => {
                    try self.logger.log(.warn, "Discard command: {}", .{std.meta.activeTag(item.event)});
                },
            }
        }
    }
}

fn tryLoadSchema(self: *Self, schema_dir_path: core.FilePath) !bool {
    const err = c.loadSchema(self.db, schema_dir_path.ptr, schema_dir_path.len);
    switch (err) {
        c.schema_dir_not_found => {
            try self.logger.log(.err, "Launch failed. Invalid schema location.", .{});
        },
        c.schema_load_failed => {
            try self.logger.log(.err, "Launch failed. Invalid schema definitions.", .{});
        },
        else => {},
    }

    return err == c.no_error;
}
