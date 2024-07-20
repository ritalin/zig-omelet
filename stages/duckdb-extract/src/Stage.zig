const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");

const Setting = @import("./Setting.zig");
const app_context = @import("build_options").app_context;
const c = @import("worker_runtime");
const ExtractWorker = @import("./ExtractWorker.zig");

const Symbol = core.Symbol;
const Connection = core.sockets.Connection.Client(app_context, ExtractWorker);
const Logger = core.Logger.withAppContext(app_context);

const Self = @This();

allocator: std.mem.Allocator,
context: *zmq.ZContext,
connection: *Connection,
logger: Logger,
database: c.DatabaseRef,

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
        .database = database,
    };
}

pub fn deinit(self: *Self) void {
    c.deinitDatabase(self.database);
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

    var body_lookup = std.StringHashMap(LookupEntry).init(self.allocator);
    defer body_lookup.deinit();

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
                        self.allocator, &.{c.topic_query, c.topic_placeholder, c.topic_select_list}
                    );
                    
                    topics: {
                        try self.connection.dispatcher.post(.{.topic = topic});
                        break :topics;
                    }
                },
                .source_path => |path| {
                    try self.logger.log(.debug, "Accept source path: {s}", .{path.path});
                    try self.logger.log(.trace, "Begin worker process", .{});

                    const p1 = try path.clone(self.allocator);
                    try body_lookup.put(p1.path, .{.path = p1, .ref_count = 0, .item_count = 1});

                    const worker = try ExtractWorker.init(self.allocator, self.database, path.path);
                    try self.connection.pull_sink_socket.spawn(worker);
                },
                .worker_result => |result| {
                    const event = try self.processWorkerResult(item.from, result.content, &body_lookup);
                    try self.connection.dispatcher.post(event);
                    try self.logger.log(.trace, "End worker process", .{});
                },
                .finish_source_path => {
                    if (body_lookup.count() == 0) {
                        try self.connection.dispatcher.post(.finish_topic_body);
                    }
                    else {
                        try self.connection.dispatcher.state.receiveTerminate();
                    }
                },
                .quit => {
                    if (body_lookup.count() == 0) {
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
    const err = c.loadSchema(self.database, schema_dir_path.ptr, schema_dir_path.len);
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

const WorkerResultTags = std.StaticStringMap(core.EventType).initComptime(.{
    .{"topic_body", .topic_body}, 
    .{"log", .log},
});

fn processWorkerResult(self: *Self, from: Symbol, result_content: Symbol, lookup: *std.StringHashMap(LookupEntry)) !core.Event {
    var reader = core.CborStream.Reader.init(result_content);

    const event_tag = try reader.readString();
    const source_path = try reader.readString();

    if (lookup.getPtr(source_path)) |entry| {
        defer {
            if (entry.ref_count == entry.item_count) {
                var path = entry.path;
                _ = lookup.remove(source_path);
                path.deinit();
            }
        }
        defer entry.ref_count += 1;

        if (WorkerResultTags.get(event_tag)) |tag| {
            switch (tag) {
                .topic_body => {
                    return try processParseResult(self.allocator, from, &reader, entry);
                },
                .log => {
                    return try processLogResult(self.allocator, from, &reader, entry);
                },
                else => unreachable,
            }
        }
    }

    return .{
        .log = try core.Event.Payload.Log.init(self.allocator, .{.warn, "Unknown worker result"}),
    };
}

fn processParseResult(allocator: std.mem.Allocator, from: Symbol, reader: *core.CborStream.Reader, entry: *LookupEntry) !core.Event {
    _ = from;
    const item_count = try reader.readUInt(u32);
    const item_index = try reader.readUInt(u32);

    defer entry.item_count = item_count;

    const result = try reader.readTuple(core.StructView(core.Event.Payload.TopicBody.Item));

    var topic_body = try core.Event.Payload.TopicBody.init(allocator, entry.path.values(), &.{result});
    return .{
        .topic_body = topic_body.withNewIndex(item_index, item_count),
    };
}

fn processLogResult(allocator: std.mem.Allocator, from: Symbol, reader: *core.CborStream.Reader, entry: *LookupEntry) !core.Event {
    const log_level = try reader.readString();
    const content = try reader.readString();
    
    const full_from = try std.fmt.allocPrint(allocator, "{s}/{s}", .{app_context, from});
    defer allocator.free(full_from);

    return .{
        .invalid_topic_body = try core.Event.Payload.InvalidTopicBody.init(allocator, 
            entry.path.values(),
            .{core.Logger.stringToLogLevel(log_level), content}
        ),
    };
}

const LookupEntry = struct {
    path: core.Event.Payload.SourcePath,
    ref_count: usize,
    item_count: usize,
};