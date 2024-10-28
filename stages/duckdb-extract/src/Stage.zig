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
        if (try self.tryLoadSchema(setting.schema_dir_set)) {
            try self.connection.dispatcher.post(.launched);
        }
        else {
            try self.connection.dispatcher.post(.failed_launching);
        }
        break :launch;
    }

    var lookup = std.StringHashMap(LookupEntry).init(self.allocator);
    defer lookup.deinit();

    while (self.connection.dispatcher.isReady()) {
        self.waitNextDispatch(setting, &lookup) catch {
            try self.connection.dispatcher.postFatal(@errorReturnTrace());
        };
    }
}

fn waitNextDispatch(self: *Self, setting: Setting, lookup: *std.StringHashMap(LookupEntry)) !void {
    _ = setting;
    
    const _item = self.connection.dispatcher.dispatch() catch |err| switch (err) {
        error.InvalidResponse => {
            try self.logger.log(.warn, "Unexpected data received", .{});
            return;
        },
        else => return err,
    };

    if (_item) |item| {
        defer item.deinit();
        
        switch (item.event) {
            .request_topic => {
                topics: {
                    const topic = try core.Event.Payload.Topic.init(
                        self.allocator, 
                        .source,
                        &.{c.topic_query, c.topic_placeholder, c.topic_placeholder_order, c.topic_select_list, c.topic_bound_user_type, c.topic_anon_user_type},
                        true,
                    );
                    
                    try self.connection.dispatcher.post(.{.topic = topic});
                    break :topics;
                }
                topics: {
                    const topic = try core.Event.Payload.Topic.init(
                        self.allocator, 
                        .schema,
                        &.{c.topic_user_type},
                        false,
                    );
                    
                    try self.connection.dispatcher.post(.{.topic = topic});
                    break :topics;
                }
            },
            .source_path => |path| {
                try self.logger.log(.debug, "Accept source path: {s}", .{path.path});
                try self.logger.log(.trace, "Begin worker process", .{});

                const p1 = try path.clone(self.allocator);
                try lookup.put(p1.path, .{.path = p1, .item_count = 1});

                const worker = try ExtractWorker.init(
                    self.allocator, 
                    path.category, self.database, path.path
                );
                try self.connection.pull_sink_socket.spawn(worker);
            },
            .worker_response => |res| {
                if (try self.processWorkerResponse(item.from, res.content, lookup)) |event| {
                    try self.connection.dispatcher.post(event);
                    try self.logger.log(.trace, "Redirect worker response", .{});
                }
            },
            .finish_source_path => {
                if (lookup.count() == 0) {
                    try self.connection.dispatcher.post(.finish_topic_body);
                }
                else {
                    try self.connection.dispatcher.state.receiveTerminate();
                }
            },
            .quit => {
                if (lookup.count() == 0) {
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

fn tryLoadSchema(self: *Self, schema_dir_set: []const core.FilePath) !bool {
    for (schema_dir_set) |path| {
        const err = c.loadSchema(self.database, path.ptr, path.len);
        switch (err) {
            c.schema_dir_not_found => {
                try self.logger.log(.err, "Launch failed. Invalid schema location. ({s})", .{path});
            },
            c.schema_load_failed => {
                try self.logger.log(.err, "Launch failed. Invalid schema definitions.", .{});
            },
            else => {},
        }
    }

    user_type: {
        const err = c.retainUserTypeName(self.database);
        switch (err) {
            c.invalid_schema_catalog => {
                try self.logger.log(.err, "Launch failed. Invalid schema catalog.", .{});
            },
            else => {},
        }
        break:user_type;
    }

    return true;
}

fn spawnWorker(self: *Self, path: core.Event.Payload.SourcePath) !void {
    const worker = try ExtractWorker.init(
        self.allocator, 
        path.category, self.database, path.path
    );
    try self.connection.pull_sink_socket.spawn(worker);
}

const WorkerResultTags = std.StaticStringMap(core.EventType).initComptime(.{
    .{"topic_body", .topic_body}, 
    .{"log", .log},
});

pub const WorkerResponseTag = enum(u8) {
    worker_progress = c.worker_progress,
    worker_result = c.worker_result,
    worker_finished = c.worker_finished,
    worker_log = c.worker_log,
    worker_skipped = c.worker_skipped,

    pub fn fromString(tag: core.Symbol) WorkerResponseTag {
        return std.meta.stringToEnum(WorkerResponseTag, tag).?;
    }
};

fn processWorkerResponse(self: *Self, from: Symbol, result_content: Symbol, lookup: *std.StringHashMap(LookupEntry)) !?core.Event {
    var reader = core.CborStream.Reader.init(result_content);

    const tag = try reader.readString();
    const source_path = try reader.readString();

    if (lookup.getPtr(source_path)) |entry| {
        switch (WorkerResponseTag.fromString(tag)) {
            .worker_progress => {
                entry.item_count = try reader.readUInt(usize);
                return null;
            },
            .worker_result => {
                return try processExtractResult(self.allocator, from, &reader, entry);
            },
            .worker_finished => {
                var path = entry.path;
                defer path.deinit();

                _ = lookup.remove(source_path);
                return null;
            },
            .worker_log => {
                return try processLogResult(self.allocator, from, &reader, entry);
            },
            .worker_skipped => {
                return try processSkipResult(self.allocator, from, &reader, entry);
            }
        }
    }

    return .{
        .log = try core.Event.Payload.Log.init(self.allocator, .{.warn, "Unknown worker result"}),
    };
}

fn processExtractResult(allocator: std.mem.Allocator, from: Symbol, reader: *core.CborStream.Reader, entry: *LookupEntry) !core.Event {
    _ = from;
    const item_index = try reader.readUInt(u32);

    const items = try reader.readSlice(allocator, core.StructView(core.Event.Payload.TopicBody.Item));
    defer allocator.free(items);

    var topic_body = try core.Event.Payload.TopicBody.init(allocator, entry.path.values(), items);
    return .{
        .topic_body = topic_body.withNewIndex(item_index, entry.item_count),
    };
}

fn processLogResult(allocator: std.mem.Allocator, from: Symbol, reader: *core.CborStream.Reader, entry: *LookupEntry) !core.Event {
    _ = entry;
    const log_level = core.Logger.stringToLogLevel(try reader.readString());
    const content = try reader.readString();
    
    const full_from = try std.fmt.allocPrint(allocator, "{s}/{s}", .{app_context, from});
    defer allocator.free(full_from);

    return .{
        .log = try core.Event.Payload.Log.init(allocator,
            .{log_level, content}
        ),
    };
}

fn processSkipResult(allocator: std.mem.Allocator, from: Symbol, reader: *core.CborStream.Reader, entry: *LookupEntry) !core.Event {
    _ = from;
    const item_index = try reader.readUInt(u32);

    return .{
        .skip_topic_body = try core.Event.Payload.SkipTopicBody.init(allocator, 
            entry.path.values(),
            item_index, 
        ),
    };
}

const LookupEntry = struct {
    path: core.Event.Payload.SourcePath,
    item_count: usize,
};

const WorkerTestContext = struct {
    allocator: std.mem.Allocator,
    stage: Stage,
    src_dir: std.testing.TmpDir,
    lookup: std.StringHashMap(LookupEntry),

    const Stage  = Self;

    pub fn init(arena: *std.heap.ArenaAllocator) !WorkerTestContext {
        const setting: Setting = .{
            .arena = arena,
            .endpoints = core.DebugEndPoint.StageEndpoint,
            .log_level = .info,
            .standalone = true,
            .schema_dir_set = &.{},
        };
        const allocator = arena.allocator();

        return .{
            .allocator = allocator,
            .stage = try Stage.init(allocator, setting),
            .src_dir = std.testing.tmpDir(.{}),
            .lookup = std.StringHashMap(LookupEntry).init(allocator),
        };
    }

    pub fn deinit(self: *WorkerTestContext) void {
        defer self.src_dir.cleanup();
        defer self.lookup.deinit();
        defer self.stage.deinit();
    }

    pub fn pushTestQuery(self: *WorkerTestContext, category: core.TopicCategory, query: core.Symbol) !core.Event.Payload.SourcePath {
        var file = try self.src_dir.dir.createFile("test.sql", .{});
        defer file.close();
        try file.writeAll(query);

        const path = try self.src_dir.dir.realpathAlloc(self.allocator, "test.sql");
        defer self.allocator.free(path);

        const source_path = try core.Event.Payload.SourcePath.init(self.allocator, .{
            category,
            "test",
            path,
            "test",
            1
        });
        try self.lookup.put(source_path.path, .{.path = source_path, .item_count = 0});

        return source_path.clone(self.allocator);
    }

    pub fn expectProgress(self: *WorkerTestContext, path: core.Event.Payload.SourcePath, from: Symbol, event: core.Event, expect_count: usize) !void {
        validate: {
            try std.testing.expectEqual(event.tag(), .worker_response);
                
            var decoder = core.CborStream.Reader.init(event.worker_response.content);
            try std.testing.expectEqualStrings(@tagName(.worker_progress), try decoder.readString());
            try std.testing.expectEqualStrings(path.path, try decoder.readString());
            try std.testing.expectEqual(expect_count, try decoder.readUInt(usize));
            break:validate;
        }
        decode: {
            const next_event = try self.stage.processWorkerResponse(from, event.worker_response.content, &self.lookup);
            try std.testing.expect(next_event == null);
            try std.testing.expectEqual(true, self.lookup.contains(path.path));
            try std.testing.expectEqual(1, self.lookup.get(path.path).?.item_count);
            break:decode;
        }
    }

    pub fn expectResult(self: *WorkerTestContext, path: core.Event.Payload.SourcePath, from: Symbol, event: core.Event, expect_topcs: []const Symbol) !void {
        validate: {
            try std.testing.expectEqual(event.tag(), .worker_response);
                
            var decoder = core.CborStream.Reader.init(event.worker_response.content);
            try std.testing.expectEqualStrings(@tagName(.worker_result), try decoder.readString());
            try std.testing.expectEqualStrings(path.path, try decoder.readString());
            try std.testing.expectEqual(0, try decoder.readUInt(usize));

            const topic_bodies = try decoder.readSlice(self.allocator, core.StructView(core.Event.Payload.TopicBody.Item));
            defer self.allocator.free(topic_bodies);

            try expectTopics(self.allocator, topic_bodies, expect_topcs);
            break:validate;
        }
        decode: {
            const next_event = try self.stage.processWorkerResponse(from, event.worker_response.content, &self.lookup);
            try std.testing.expect(next_event != null);
            defer next_event.?.deinit();

            try std.testing.expectEqual(true, self.lookup.contains(path.path));
            try std.testing.expectEqual(.topic_body, next_event.?.tag());
            try std.testing.expectEqual(1, next_event.?.topic_body.header.item_count);
            try std.testing.expectEqual(0, next_event.?.topic_body.index);
            try std.testing.expectEqualStrings(path.path, next_event.?.topic_body.header.path);
            break:decode;
        }
    }

    fn expectTopics(allocator: std.mem.Allocator, topic_bodies: []const core.StructView(core.Event.Payload.TopicBody.Item), expect_topics: []const Symbol) !void {
        try std.testing.expectEqual(expect_topics.len, topic_bodies.len);
        
        var expect_set = std.BufSet.init(allocator);
        defer expect_set.deinit();

        for (expect_topics) |topic| {
            try expect_set.insert(topic);
        }

        for (topic_bodies) |body| {
            expect_set.remove(body[0]);
        }

        try std.testing.expectEqual(0, expect_set.count());
    }

    pub fn expectFinished(self: *WorkerTestContext, path: core.Event.Payload.SourcePath, from: Symbol, event: core.Event) !void {
        validate: {
            try std.testing.expectEqual(event.tag(), .worker_response);
                
            var decoder = core.CborStream.Reader.init(event.worker_response.content);
            try std.testing.expectEqualStrings(@tagName(.worker_finished), try decoder.readString());
            try std.testing.expectEqualStrings(path.path, try decoder.readString());
            break:validate;
        }
        decode: {
            const next_event = try self.stage.processWorkerResponse(from, event.worker_response.content, &self.lookup);
            try std.testing.expect(next_event == null);
            try std.testing.expectEqual(false, self.lookup.contains(path.path));
            break:decode;
        }
    }

    pub fn expectLog(self: *WorkerTestContext, path: core.Event.Payload.SourcePath, from: Symbol, event: core.Event, expect_log_level: core.LogLevel) !void {
        validate: {
            try std.testing.expectEqual(event.tag(), .worker_response);
                
            var decoder = core.CborStream.Reader.init(event.worker_response.content);
            try std.testing.expectEqualStrings(@tagName(.worker_log), try decoder.readString());
            try std.testing.expectEqualStrings(path.path, try decoder.readString());

            const log_level = try decoder.readString();
            const log_message = try decoder.readString();
            try std.testing.expectEqual(std.meta.stringToEnum(core.LogLevel, log_level).?, expect_log_level);
            try std.testing.expect(log_message.len > 0);
            break:validate;
        }
        decode: {
            const next_event = try self.stage.processWorkerResponse(from, event.worker_response.content, &self.lookup);
            try std.testing.expect(next_event != null);
            try std.testing.expectEqual(true, self.lookup.contains(path.path));
            try std.testing.expectEqual(.log, next_event.?.tag());
            try std.testing.expectEqual(expect_log_level, next_event.?.log.level);
            try std.testing.expect(next_event.?.log.content.len > 0);
            break:decode;
        }
    }

    pub fn expectSkipResult(self: *WorkerTestContext, path: core.Event.Payload.SourcePath, from: Symbol, event: core.Event, expect_item_index: usize) !void {
        validate: {
            try std.testing.expectEqual(event.tag(), .worker_response);

            var decoder = core.CborStream.Reader.init(event.worker_response.content);
            try std.testing.expectEqualStrings(@tagName(.worker_skipped), try decoder.readString());
            try std.testing.expectEqualStrings(path.path, try decoder.readString());
            try std.testing.expectEqual(expect_item_index, try decoder.readUInt(usize));
            break:validate;
        }
        decode: {
            const next_event = try self.stage.processWorkerResponse(from, event.worker_response.content, &self.lookup);
            try std.testing.expect(next_event != null);
            try std.testing.expectEqual(true, self.lookup.contains(path.path));
            try std.testing.expectEqual(.skip_topic_body, next_event.?.tag());
            try std.testing.expectEqual(expect_item_index, next_event.?.skip_topic_body.index);
            break:decode;
        }
    }
};

test "worker workflow/single query (success flow)" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var ctx = try WorkerTestContext.init(&arena);
    defer ctx.deinit();

    const path = try ctx.pushTestQuery(.source,
        \\ select 1
    );
    defer path.deinit();

    try ctx.stage.spawnWorker(path);

    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectProgress(path, item.from, item.event, 1);
        break:receive;
    }
    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectResult(path, item.from, item.event, &.{
            c.topic_query, c.topic_placeholder, c.topic_placeholder_order, 
            c.topic_select_list, c.topic_bound_user_type, c.topic_anon_user_type
        });
        break:receive;
    }
    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectFinished(path, item.from, item.event);
        break:receive;
    }
}

test "worker workflow/single query (unssuported query flow)" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var ctx = try WorkerTestContext.init(&arena);
    defer ctx.deinit();

    const path = try ctx.pushTestQuery(.source,
        \\ create table T (id int primary key)
    );
    defer path.deinit();

    try ctx.stage.spawnWorker(path);

    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectProgress(path, item.from, item.event, 1);
        break:receive;
    }
    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectLog(path, item.from, item.event, .warn);
        break:receive;
    }
    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectSkipResult(path, item.from, item.event, 0);
        break:receive;
    }
    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectFinished(path, item.from, item.event);
        break:receive;
    }
}

test "worker workflow/multiple query (success flow) " {

}

test "worker workflow/multiple query (partially unssuported query flow)" {

}

test "worker workflow/empty query" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var ctx = try WorkerTestContext.init(&arena);
    defer ctx.deinit();

    const path = try ctx.pushTestQuery(.source, "");
    defer path.deinit();

    try ctx.stage.spawnWorker(path);

    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectLog(path, item.from, item.event, .warn);
        break:receive;
    }
    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectSkipResult(path, item.from, item.event, 0);
        break:receive;
    }
    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectFinished(path, item.from, item.event);
        break:receive;
    }
}

test "worker workflow/empty query#2" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var ctx = try WorkerTestContext.init(&arena);
    defer ctx.deinit();

    const path = try ctx.pushTestQuery(.source, "  \n\n ");
    defer path.deinit();

    try ctx.stage.spawnWorker(path);

    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectLog(path, item.from, item.event, .warn);
        break:receive;
    }
    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectSkipResult(path, item.from, item.event, 0);
        break:receive;
    }
    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectFinished(path, item.from, item.event);
        break:receive;
    }
}

test "worker workflow/invalid query" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var ctx = try WorkerTestContext.init(&arena);
    defer ctx.deinit();

    const path = try ctx.pushTestQuery(.source,
        \\ SELCT 1
    );
    defer path.deinit();

    try ctx.stage.spawnWorker(path);

    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectLog(path, item.from, item.event, .err);
        break:receive;
    }
    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectSkipResult(path, item.from, item.event, 0);
        break:receive;
    }
    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectFinished(path, item.from, item.event);
        break:receive;
    }
}

test "worker workflow/single schema (success flow)" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var ctx = try WorkerTestContext.init(&arena);
    defer ctx.deinit();

    const path = try ctx.pushTestQuery(.schema,
        \\ create type Visibility as enum ('hide', 'visible')
    );
    defer path.deinit();

    try ctx.stage.spawnWorker(path);

    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectProgress(path, item.from, item.event, 1);
        break:receive;
    }
    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectResult(path, item.from, item.event, &.{
            c.topic_user_type, c.topic_bound_user_type, c.topic_anon_user_type
        });
        break:receive;
    }
    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectFinished(path, item.from, item.event);
        break:receive;
    }
}

test "worker workflow/single schema (unssuported flow)" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var ctx = try WorkerTestContext.init(&arena);
    defer ctx.deinit();

    const path = try ctx.pushTestQuery(.schema,
        \\ select 1
    );
    defer path.deinit();

    try ctx.stage.spawnWorker(path);

    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectProgress(path, item.from, item.event, 1);
        break:receive;
    }
    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectLog(path, item.from, item.event, .warn);
        break:receive;
    }
    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectSkipResult(path, item.from, item.event, 0);
        break:receive;
    }
    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectFinished(path, item.from, item.event);
        break:receive;
    }
}

test "worker workflow/multiple schema (success flow) " {
}

test "worker workflow/multiple schema (partially unssuported flow)" {
}

test "worker workflow/empty schema" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var ctx = try WorkerTestContext.init(&arena);
    defer ctx.deinit();

    const path = try ctx.pushTestQuery(.schema, "");
    defer path.deinit();

    try ctx.stage.spawnWorker(path);

    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectLog(path, item.from, item.event, .warn);
        break:receive;
    }
    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectSkipResult(path, item.from, item.event, 0);
        break:receive;
    }
    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectFinished(path, item.from, item.event);
        break:receive;
    }
}

test "worker workflow/empty schema#2" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var ctx = try WorkerTestContext.init(&arena);
    defer ctx.deinit();

    const path = try ctx.pushTestQuery(.schema, "  \n\n ");
    defer path.deinit();

    try ctx.stage.spawnWorker(path);

    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectLog(path, item.from, item.event, .warn);
        break:receive;
    }
    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectSkipResult(path, item.from, item.event, 0);
        break:receive;
    }
    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectFinished(path, item.from, item.event);
        break:receive;
    }
}

test "worker workflow/invalid schema" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var ctx = try WorkerTestContext.init(&arena);
    defer ctx.deinit();

    const path = try ctx.pushTestQuery(.schema,
        \\ CREAT TYPE X AS ENUM ('x')
    );
    defer path.deinit();

    try ctx.stage.spawnWorker(path);

    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectLog(path, item.from, item.event, .err);
        break:receive;
    }
    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectSkipResult(path, item.from, item.event, 0);
        break:receive;
    }
    receive: {
        const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
        defer item.deinit();

        try ctx.expectFinished(path, item.from, item.event);
        break:receive;
    }
}
