const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");

const app_context = @import("build_options").app_context;
const Setting = @import("./settings/Setting.zig");
const StageCount = @import("./configs/Config.zig").StageCount;
const PayloadCacheManager = @import("./cache_manager.zig").PayloadCacheManager(app_context);
const CommandPallet = @import("./CommandPallet.zig");

const Symbol = core.Symbol;
const systemLog = core.Logger.SystemDirect(app_context);
const traceLog = core.Logger.TraceDirect(app_context);
const log = core.Logger.Stage.log;

const Self = @This();

allocator: std.mem.Allocator,
context: zmq.ZContext,
connection: *core.sockets.Connection.Server(app_context, CommandPallet),

pub fn init(allocator: std.mem.Allocator, setting: Setting) !Self {
    var ctx = try zmq.ZContext.init(allocator);
    errdefer ctx.deinit();

    var connection = try core.sockets.Connection.Server(app_context, CommandPallet).init(allocator, &ctx);
    errdefer connection.deinit();
    try connection.bind(setting.general.runner_endpoints);

    return .{
        .allocator = allocator,
        .context = ctx,
        .connection = connection,
    };
}

pub fn deinit(self: *Self) void {
    self.connection.deinit();
    self.context.deinit();
}

pub fn run(self: *Self, stage_count: StageCount, setting: Setting) !void {
    systemLog.debug("Launched", .{});

    dump_setting: {
        systemLog.debug("CLI: Req/Rep Channel = {s}", .{setting.general.runner_endpoints.req_rep});
        systemLog.debug("CLI: Pub/Sub Channel = {s}", .{setting.general.runner_endpoints.pub_sub});
        systemLog.debug("CLI: Watch mode = {}", .{setting.command.watchModeEnabled()});
        break :dump_setting;
    }

    var left_launching = stage_count.stage_watch + stage_count.stage_extract + stage_count.stage_generate;
    var left_topic_stage = stage_count.stage_extract;
    var left_launched = stage_count.stage_watch + stage_count.stage_extract + stage_count.stage_generate;

    var source_cache = try PayloadCacheManager.init(self.allocator);
    defer source_cache.deinit();

    const watch_mode = setting.command.watching();
    if (watch_mode) {
        try self.spawnCommandPallet();
    }

    try self.connection.dispatcher.state.ready();
    
    while (self.connection.dispatcher.isReady()) {
        const _item = try self.connection.dispatcher.dispatch();

        if (_item) |*item| {
            defer item.deinit();

            switch (item.event) {
                .launched => {
                    try self.connection.dispatcher.reply(item.socket, .ack, item.routing_id);
                    
                    if (left_launching > 0) {
                        left_launching -= 1;
                        systemLog.debug("Received launched: '{s}' (left: {})", .{item.from, left_launching});
                    }
                    else {
                        systemLog.debug("Received rebooted: '{s}' (left: {})", .{item.from, left_launching});
                    }

                    if (left_launching <= 0) {
                        try self.onAfterLaunch(item.socket, item.routing_id);
                    }
                },
                .failed_launching => {
                    try self.connection.dispatcher.reply(item.socket, .ack, item.routing_id);
                    try self.connection.dispatcher.state.receiveTerminate();

                    if (left_launching > 0) {
                        left_launching -= 1;
                        systemLog.debug("Received to failed launching: '{s}' (left: {})", .{item.from, left_launching});
                    }
                    if (left_launching <= 0) {
                        try self.onAfterLaunch(item.socket, item.routing_id);
                    }
                },
                .topic => |payload| {
                    try self.connection.dispatcher.reply(item.socket, .ack, item.routing_id);

                    try source_cache.topics_map.addTopics(payload.category, payload.names);

                    if (!payload.has_more) {
                        left_topic_stage -= 1;
                    }
                    systemLog.debug("Receive 'topic' ({})", .{left_topic_stage});
                    try source_cache.topics_map.dumpTopics(self.allocator);

                    if (left_topic_stage <= 0) {
                        try self.connection.dispatcher.post(.ready_watch_path);
                    }
                },
                .source_path => |path| {
                    try self.connection.dispatcher.reply(item.socket, .ack, item.routing_id);

                    if (try source_cache.addNewEntryGroup(path)) {
                        systemLog.debug("Received source name: {s}, path: {s}, hash: {s}", .{path.name, path.path, path.hash});
                        try self.connection.dispatcher.post(.{.source_path = try path.clone(self.allocator)});
                    }
                },
                .finish_watch_path => {
                    traceLog.debug("Watching stage finished", .{});
                    if (!watch_mode) {
                        // request quit for Watch stage
                        traceLog.debug("Request quit for Watching stage", .{});
                        try self.connection.dispatcher.reply(item.socket, .quit, item.routing_id);
                        try self.connection.dispatcher.state.receiveTerminate();

                        if (source_cache.isEmpty()) {
                            try self.connection.dispatcher.post(.finish_source_path);
                        }
                    }
                    else {
                        try self.connection.dispatcher.reply(item.socket, .ack, item.routing_id);
                    }
                },
                .topic_body => |payload| {
                    // delay 1 cycle
                    try self.connection.dispatcher.delay(item.socket, item.from, .pending_finish_source_path, item.routing_id);

                    if (try self.handleTopicBody(payload, &source_cache)) |next_event| {
                        try self.connection.dispatcher.post(next_event);
                    }
                },
                .skip_topic_body => |payload| {
                    try self.handleSkipTopicBody(payload, &source_cache);
                    // delay 1 cycle
                    try self.connection.dispatcher.delay(item.socket, item.from, .pending_finish_source_path, item.routing_id);
                },
                .pending_finish_source_path => {
                    if ((self.connection.dispatcher.state.level.terminating) and (source_cache.cache.count() == 0)) {
                        systemLog.debug("No more source path", .{});
                        try self.connection.dispatcher.reply(item.socket, .ack, item.routing_id);
                        try self.connection.dispatcher.post(.finish_source_path);
                    }
                    else {
                        systemLog.debug("Wait receive next source path", .{});
                        try self.connection.dispatcher.reply(item.socket, .ack, item.routing_id);
                    }
                },
                .finish_topic_body => {
                    if (!watch_mode) {
                        // request quit for Extract stage
                        try self.connection.dispatcher.reply(item.socket, .quit, item.routing_id);
                    }
                    else {
                        try self.connection.dispatcher.reply(item.socket, .ack, item.routing_id);
                    }

                    if ((self.connection.dispatcher.state.level.terminating) and source_cache.isEmpty()) {
                        try self.connection.dispatcher.post(.finish_topic_body);
                    }
                },
                .ready_generate => {
                    if (source_cache.ready_queue.dequeue()) |source| {
                        systemLog.debug("Send source: {s}", .{source.header.name});
                        try self.connection.dispatcher.reply(item.socket, .{.topic_body = source}, item.routing_id);
                    }
                    else {
                        // delay 1 cycle
                        try self.connection.dispatcher.delay(item.socket, item.from, .pending_finish_topic_body, item.routing_id);
                    }
                },
                .pending_finish_topic_body => {
                    if ((self.connection.dispatcher.state.level.terminating) and (source_cache.isEmpty())) {
                        systemLog.debug("No more sources", .{});
                        try self.connection.dispatcher.reply(item.socket, .finish_topic_body, item.routing_id);
                    }
                    else {
                        systemLog.debug("Wait receive next source", .{});
                        try self.connection.dispatcher.reply(item.socket, .ack, item.routing_id);
                    }
                },
                .finish_generate => {
                    if (!watch_mode) {
                        // request quit for Generate stage
                        try self.connection.dispatcher.reply(item.socket, .quit, item.routing_id);
                    }
                    else {
                        try self.connection.dispatcher.reply(item.socket, .ack, item.routing_id);
                    }
                },
                .pending_fatal_quit => {
                    try self.connection.dispatcher.post(.quit_all);
                },
                .worker_response => |payload| {
                    if (try self.handleWorkerResponse(payload)) |next_event| {
                        try self.connection.dispatcher.post(next_event);
                    }
                },
                .quit_accept => {
                    try self.connection.dispatcher.reply(item.socket, .ack, item.routing_id);

                    left_launched -= 1;
                    systemLog.debug("Quit acceptrd: {s} (left: {})", .{item.from, left_launched});

                    if (left_launched <= 0) {
                        systemLog.debug("All Quit acceptrd", .{});
                        try self.connection.dispatcher.state.done();
                    }
                },
                .log => |payload| {
                    try self.connection.dispatcher.reply(item.socket, .ack, item.routing_id);
                    log(payload.level, item.from, payload.content);
                },
                .report_fatal => |payload| {
                    try self.connection.dispatcher.reply(item.socket, .quit, item.routing_id);
                    log(payload.level, item.from, payload.content);
                    try self.connection.dispatcher.delay(item.socket, item.from, .pending_fatal_quit, item.routing_id);
                },
                else => {
                    try self.connection.dispatcher.reply(item.socket, .ack, item.routing_id);
                    systemLog.debug("Discard command: {}", .{std.meta.activeTag(item.event)});
                },
            }
        }
    }

    systemLog.debug("terminated", .{});
}

fn onAfterLaunch(self: Self, socket: *zmq.ZSocket, routing_id: ?core.Symbol) !void {
    if (self.connection.dispatcher.state.level.terminating) {
        traceLog.debug("Stopping launch process", .{});
        try self.connection.dispatcher.delay(socket, app_context, .pending_fatal_quit, routing_id);
    }
    else {
        traceLog.debug("Received launched all", .{});
        // collect topics
        try self.connection.dispatcher.post(.request_topic);
    }
}

fn handleTopicBody(self: Self, topic_body: core.Event.Payload.TopicBody, source_cache: *PayloadCacheManager) !?core.Event {
    _ = self;

    switch (try source_cache.update(topic_body)) {
        .expired => {
            systemLog.debug("Content expired: {s}", .{topic_body.header.path});
        },
        .missing => {
            systemLog.debug("Waiting left content: {s}", .{topic_body.header.path});
        },
        .fulfil => {
            systemLog.debug("Source is ready: {s}", .{topic_body.header.name});
            if (try source_cache.ready(topic_body.header, topic_body.index)) {
                return .ready_topic_body;
            }
        },
    }

    return null;
}

fn handleSkipTopicBody(self: Self, topic_body: core.Event.Payload.SkipTopicBody, source_cache: *PayloadCacheManager) !void {
    _ = self;
    try source_cache.dismiss(topic_body.header, topic_body.index);
}

fn spawnCommandPallet(self: Self) !void {
    const worker = try CommandPallet.init(self.allocator);
    try self.connection.pull_sink_socket.spawn(worker);
}

fn handleWorkerResponse(self: Self, res: core.Event.Payload.WorkerResponse) !?core.Event {
    var reader = core.CborStream.Reader.init(res.content);

    switch (try reader.readEnum(CommandPallet.Status)) {
        .invalid => {
            const message = try reader.readString();
            std.debug.print("{s}\n", .{message});
            try self.spawnCommandPallet();
            return null;
        },
        .accept => {
            return try self.handleCommand(try reader.readEnum(CommandPallet.Command));
        }
}
}

fn handleCommand(self: Self, command: CommandPallet.Command) !?core.Event {
    switch (command) {
        .help => {
            try CommandPallet.showCommandhelp(self.allocator);
            try self.spawnCommandPallet();
            return null;
        },
        .quit => {
            return .quit_all;
        },
        .run => {
            try self.spawnCommandPallet();
            return .ready_watch_path;
        }
    }
}

const RunnerTestContext = struct {
    allocator: std.mem.Allocator, 
    runner: Runner,
    cache_manager: PayloadCacheManager,
    category: core.TopicCategory,

    const Runner = Self;

    pub fn init(allocator: std.mem.Allocator, category: core.TopicCategory) !RunnerTestContext {
        const setting: Setting = .{
            .arena = undefined,
            .general = .{
                .runner_endpoints = core.DebugEndPoint.RunnerEndpoint,
                .stage_endpoints = core.DebugEndPoint.StageEndpoint,
                .log_level = .info,
            },
            .command = undefined,
        };
        try core.makeIpcChannelRoot(setting.general.stage_endpoints);
        defer core.cleanupIpcChannelRoot(setting.general.stage_endpoints);

        var self = .{
            .allocator = allocator,
            .runner = try Runner.init(allocator, setting),
            .cache_manager = try PayloadCacheManager.init(allocator),
            .category = category,
        };

        try self.cache_manager.topics_map.addTopics(category, &.{"test1", "test2"});

        return self;
    }

    pub fn deinit(self: *RunnerTestContext) void {
        self.cache_manager.deinit();
        self.runner.deinit();
    }

    pub fn newEntry(self: *RunnerTestContext, path: core.FilePath, item_count: usize) !core.Event.Payload.SourcePath {
        const source_path = try core.Event.Payload.SourcePath.init(
            self.allocator, 
            .{
                self.category,
                "test",
                path,
                path,
                item_count,
            }
        );

        const add_result = try self.cache_manager.addNewEntryGroup(source_path);
        try std.testing.expect(add_result);

        return source_path;
    }
};

test "Event: Receive topic body/single" {
    const allocator = std.testing.allocator;

    var ctx = try RunnerTestContext.init(allocator, .source);
    defer ctx.deinit();

    receive: {
        const source_path = try ctx.newEntry("/path/to/test_file", 1);
        defer source_path.deinit();
        var topic_body = try core.Event.Payload.TopicBody.init(
            allocator,
            source_path.values(),
            &.{ .{ "test1", "test1" }, .{ "test2", "test2" } }
        );
        defer topic_body.deinit();

        const next_event = try ctx.runner.handleTopicBody(topic_body.withNewIndex(0, source_path.item_count), &ctx.cache_manager);

        try std.testing.expectEqual(true, next_event != null);
        try std.testing.expectEqual(.ready_topic_body, next_event.?.tag());

        try std.testing.expectEqual(false, ctx.cache_manager.cache.contains(source_path.path));
        try std.testing.expectEqual(1, ctx.cache_manager.ready_queue.count());
        break:receive;
    }
}

test "Event: Receive topic body/single (incompleted)" {
    const allocator = std.testing.allocator;

    var ctx = try RunnerTestContext.init(allocator, .source);
    defer ctx.deinit();

    const source_path = try ctx.newEntry("/path/to/test_file", 1);
    defer source_path.deinit();

    receive: {
        var topic_body = try core.Event.Payload.TopicBody.init(
            allocator,
            source_path.values(),
            &.{ .{ "test1", "test1" } }
        );
        defer topic_body.deinit();

        const next_event = try ctx.runner.handleTopicBody(topic_body.withNewIndex(0, source_path.item_count), &ctx.cache_manager);

        try std.testing.expectEqual(false, next_event != null);
        try std.testing.expectEqual(true, ctx.cache_manager.cache.contains(source_path.path));
        try std.testing.expectEqual(0, ctx.cache_manager.ready_queue.count());
        break:receive;
    }
    receive: {
        var topic_body = try core.Event.Payload.TopicBody.init(
            allocator,
            source_path.values(),
            &.{ .{ "test2", "test2" } }
        );
        defer topic_body.deinit();

        const next_event = try ctx.runner.handleTopicBody(topic_body.withNewIndex(0, source_path.item_count), &ctx.cache_manager);

        try std.testing.expectEqual(true, next_event != null);
        try std.testing.expectEqual(.ready_topic_body, next_event.?.tag());
        try std.testing.expectEqual(false, ctx.cache_manager.cache.contains(source_path.path));
        try std.testing.expectEqual(1, ctx.cache_manager.ready_queue.count());
        break:receive;
    }
}

test "Event: Receive topic body/multiple" {
    const allocator = std.testing.allocator;

    var ctx = try RunnerTestContext.init(allocator, .source);
    defer ctx.deinit();

    const source_path = try ctx.newEntry("/path/to/test_file", 2);
    defer source_path.deinit();

    receive: {
        var topic_body = try core.Event.Payload.TopicBody.init(
            allocator,
            source_path.values(),
            &.{ .{ "test1", "test1" }, .{ "test2", "test2" } }
        );
        defer topic_body.deinit();

        const next_event = try ctx.runner.handleTopicBody(topic_body.withNewIndex(0, source_path.item_count), &ctx.cache_manager);

        try std.testing.expectEqual(true, next_event != null);
        try std.testing.expectEqual(.ready_topic_body, next_event.?.tag());

        try std.testing.expectEqual(true, ctx.cache_manager.cache.contains(source_path.path));
        try std.testing.expectEqual(1, ctx.cache_manager.ready_queue.count());
        break:receive;
    }
    receive: {
        var topic_body = try core.Event.Payload.TopicBody.init(
            allocator,
            source_path.values(),
            &.{ .{ "test1", "test1" }, .{ "test2", "test2" } }
        );
        defer topic_body.deinit();

        const next_event = try ctx.runner.handleTopicBody(topic_body.withNewIndex(1, source_path.item_count), &ctx.cache_manager);

        try std.testing.expectEqual(true, next_event != null);
        try std.testing.expectEqual(.ready_topic_body, next_event.?.tag());

        try std.testing.expectEqual(false, ctx.cache_manager.cache.contains(source_path.path));
        try std.testing.expectEqual(2, ctx.cache_manager.ready_queue.count());
        break:receive;
    }
}

test "Event: Receive topic body/multiple (incompleted)" {
    const allocator = std.testing.allocator;

    var ctx = try RunnerTestContext.init(allocator, .source);
    defer ctx.deinit();

    const source_path = try ctx.newEntry("/path/to/test_file", 2);
    defer source_path.deinit();

    receive: {
        var topic_body = try core.Event.Payload.TopicBody.init(
            allocator,
            source_path.values(),
            &.{ .{ "test1", "test1" } }
        );
        defer topic_body.deinit();

        const next_event = try ctx.runner.handleTopicBody(topic_body.withNewIndex(0, source_path.item_count), &ctx.cache_manager);

        try std.testing.expectEqual(false, next_event != null);
        try std.testing.expectEqual(true, ctx.cache_manager.cache.contains(source_path.path));
        try std.testing.expectEqual(0, ctx.cache_manager.ready_queue.count());
        break:receive;
    }
    receive: {
        var topic_body = try core.Event.Payload.TopicBody.init(
            allocator,
            source_path.values(),
            &.{ .{ "test1", "test1" }, .{ "test2", "test2" } }
        );
        defer topic_body.deinit();

        const next_event = try ctx.runner.handleTopicBody(topic_body.withNewIndex(1, source_path.item_count), &ctx.cache_manager);

        try std.testing.expectEqual(true, next_event != null);
        try std.testing.expectEqual(.ready_topic_body, next_event.?.tag());

        try std.testing.expectEqual(true, ctx.cache_manager.cache.contains(source_path.path));
        try std.testing.expectEqual(1, ctx.cache_manager.ready_queue.count());
        break:receive;
    }
    receive: {
        var topic_body = try core.Event.Payload.TopicBody.init(
            allocator,
            source_path.values(),
            &.{ .{ "test2", "test2" } }
        );
        defer topic_body.deinit();

        const next_event = try ctx.runner.handleTopicBody(topic_body.withNewIndex(0, source_path.item_count), &ctx.cache_manager);

        try std.testing.expectEqual(true, next_event != null);
        try std.testing.expectEqual(.ready_topic_body, next_event.?.tag());

        try std.testing.expectEqual(false, ctx.cache_manager.cache.contains(source_path.path));
        try std.testing.expectEqual(2, ctx.cache_manager.ready_queue.count());
        break:receive;
    }
}

test "Event: Receive cancel topic body/single" {
    const allocator = std.testing.allocator;

    var ctx = try RunnerTestContext.init(allocator, .source);
    defer ctx.deinit();

    receive: {
        const source_path = try ctx.newEntry("/path/to/test_file", 1);
        defer source_path.deinit();

        const topic_body = try core.Event.Payload.SkipTopicBody.init(
            allocator,
            source_path.values(),
            0,
        );
        defer topic_body.deinit();

        try ctx.runner.handleSkipTopicBody(topic_body, &ctx.cache_manager);
        try std.testing.expectEqual(false, ctx.cache_manager.cache.contains(source_path.path));
        try std.testing.expectEqual(false, ctx.cache_manager.ready_queue.peek() != null);
        break:receive;
    }
}

test "Event: Receive cancel topic body/multiple" {
    const allocator = std.testing.allocator;

    var ctx = try RunnerTestContext.init(allocator, .source);
    defer ctx.deinit();

    const source_path = try ctx.newEntry("/path/to/test_file", 2);
    defer source_path.deinit();

    receive: {
        const topic_body = try core.Event.Payload.SkipTopicBody.init(
            allocator,
            source_path.values(),
            0,
        );
        defer topic_body.deinit();

        try ctx.runner.handleSkipTopicBody(topic_body, &ctx.cache_manager);
        try std.testing.expectEqual(true, ctx.cache_manager.cache.contains(source_path.path));
        try std.testing.expectEqual(false, ctx.cache_manager.ready_queue.peek() != null);
        break:receive;
    }
    receive: {
        const topic_body = try core.Event.Payload.SkipTopicBody.init(
            allocator,
            source_path.values(),
            0,
        );
        defer topic_body.deinit();

        try ctx.runner.handleSkipTopicBody(topic_body, &ctx.cache_manager);
        try std.testing.expectEqual(true, ctx.cache_manager.cache.contains(source_path.path));
        try std.testing.expectEqual(false, ctx.cache_manager.ready_queue.peek() != null);
        break:receive;
    }
    receive: {
        const topic_body = try core.Event.Payload.SkipTopicBody.init(
            allocator,
            source_path.values(),
            1,
        );
        defer topic_body.deinit();

        try ctx.runner.handleSkipTopicBody(topic_body, &ctx.cache_manager);
        try std.testing.expectEqual(false, ctx.cache_manager.cache.contains(source_path.path));
        try std.testing.expectEqual(false, ctx.cache_manager.ready_queue.peek() != null);
        break:receive;
    }
}
