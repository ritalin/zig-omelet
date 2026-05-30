const std = @import("std");
const core = @import("core");

const types = core.types;
const events = core.events;

const app_context = @import("build_options").app_context;
const poller_size = 6;
const ReceiveEntry = core.sockets.ReceiveEntry;

const Setting = @import("./settings/Setting.zig");
const StageCount = @import("./configs/Config.zig").StageCount;
const PayloadCacheManager = @import("./cache_manager.zig").PayloadCacheManager(app_context);

const HeartbeatTask = @import("./tasks/HeartbeatTask.zig");
const TaskReaper = @import("./supports/TaskReaper.zig");

const BootPhaseState = @import("./phases/boot_phase.zig").BootPhaseState(HostRunner);
const TerminatePhaseState = @import("./phases/terminate_phase.zig").TerminatePhaseState(HostRunner);

const EventDispatcher = core.sockets.EventDispatcher;

const Symbol = core.Symbol;
const TIMER_INTERVAL = std.Io.Duration.fromMilliseconds(50);

const task_support = @import("./supports/task_support.zig");

const HostRunner = @This();

allocator: std.mem.Allocator,
io: std.Io,
setting: *const Setting,
connection: *HostRunner.Connection,
dispatcher: EventDispatcher.Sized(poller_size),
state: State,
guest_names: []const types.Symbol,
reapers: *TaskReaper,

// TODO:
// const CommandPallet = @import("./CommandPallet.zig");
// const Connection = core.sockets.Connection.Server(app_context, CommandPallet);
pub const Connection = core.sockets.Connection.Server(app_context);

pub fn create(io: std.Io, allocator: std.mem.Allocator, connection: *Connection, guest_names: []const types.StageName, setting: *const Setting) !HostRunner {
    try connection.bind();

    const options: EventDispatcher.Options = .{ 
        .log_style = setting.general.log_style,
        .no_color = setting.general.no_color, 
    };

    return .{
        .allocator = allocator,
        .io = io,
        .setting = setting,
        .connection = connection,
        .dispatcher = try connection.configureDispatcher(poller_size, options),
        .guest_names = guest_names,
        .state = .{ .booting = try BootPhaseState.create(allocator, guest_names, setting.general.boot_limit) },
        .reapers = try TaskReaper.init(io, allocator),
    };
}

pub fn deinit(self: *HostRunner) void {
    self.state.deinit();
    self.reapers.deinit(self.allocator);
    self.dispatcher.deinit();
}

pub fn run(self: *HostRunner) !void {
    // TODO:
    // var left_topic_stage = stage_count.stage_extract;

    self.dispatcher.run(app_context, HostRunner.onDispatch) catch |err| {
        // TODO: fatal error log
        return err;
    };    

    // TODO:
    // var source_cache = try PayloadCacheManager.init(self.allocator);
    // defer source_cache.deinit();

    // const watch_mode = setting.command.watching();
    // if (watch_mode) {
    //     try self.spawnCommandPallet();
    // }
    
    //         switch (item.event) {
    //             .topic => |payload| {
    //                 try self.connection.dispatcher.reply(item.socket, .ack, item.routing_id);

    //                 try source_cache.topics_map.addTopics(payload.category, payload.names);

    //                 if (!payload.has_more) {
    //                     left_topic_stage -= 1;
    //                 }
    //                 systemLog.debug("Receive 'topic' ({})", .{left_topic_stage});
    //                 try source_cache.topics_map.dumpTopics(self.allocator);

    //                 if ((left_topic_stage <= 0) and (!watch_mode)) {
    //                     try self.connection.dispatcher.post(.ready_watch_path);
    //                 }
    //             },
    //             .source_path => |path| {
    //                 try self.connection.dispatcher.reply(item.socket, .ack, item.routing_id);

    //                 if (try source_cache.addNewEntryGroup(path)) {
    //                     systemLog.debug("Received source name: {s}, path: {s}, hash: {s}", .{path.name, path.path, path.hash});
    //                     try self.connection.dispatcher.post(.{.source_path = try path.clone(self.allocator)});
    //                 }
    //             },
    //             .finish_watch_path => {
    //                 traceLog.debug("Watching stage finished", .{});
    //                 if (!watch_mode) {
    //                     // request quit for Watch stage
    //                     traceLog.debug("Request quit for Watching stage", .{});
    //                     try self.connection.dispatcher.reply(item.socket, .quit, item.routing_id);
    //                     try self.connection.dispatcher.state.receiveTerminate();

    //                     if (source_cache.isEmpty()) {
    //                         try self.connection.dispatcher.post(.finish_source_path);
    //                     }
    //                 }
    //                 else {
    //                     try self.connection.dispatcher.reply(item.socket, .ack, item.routing_id);
    //                 }
    //             },
    //             .topic_body => |payload| {
    //                 // delay 1 cycle
    //                 try self.connection.dispatcher.delay(item.socket, item.from, .pending_finish_source_path, item.routing_id);

    //                 if (try self.handleTopicBody(payload, &source_cache)) |next_event| {
    //                     try self.connection.dispatcher.post(next_event);
    //                 }
    //             },
    //             .skip_topic_body => |payload| {
    //                 try self.handleSkipTopicBody(payload, &source_cache);
    //                 // delay 1 cycle
    //                 try self.connection.dispatcher.delay(item.socket, item.from, .pending_finish_source_path, item.routing_id);
    //             },
    //             .pending_finish_source_path => {
    //                 if ((self.connection.dispatcher.state.level.terminating) and (source_cache.cache.count() == 0)) {
    //                     systemLog.debug("No more source path", .{});
    //                     try self.connection.dispatcher.reply(item.socket, .ack, item.routing_id);
    //                     try self.connection.dispatcher.post(.finish_source_path);
    //                 }
    //                 else {
    //                     systemLog.debug("Wait receive next source path", .{});
    //                     try self.connection.dispatcher.reply(item.socket, .ack, item.routing_id);
    //                 }
    //             },
    //             .finish_topic_body => {
    //                 if (!watch_mode) {
    //                     // request quit for Extract stage
    //                     try self.connection.dispatcher.reply(item.socket, .quit, item.routing_id);
    //                 }
    //                 else {
    //                     try self.connection.dispatcher.reply(item.socket, .ack, item.routing_id);
    //                 }

    //                 if ((self.connection.dispatcher.state.level.terminating) and source_cache.isEmpty()) {
    //                     try self.connection.dispatcher.post(.finish_topic_body);
    //                 }
    //             },
    //             .ready_generate => {
    //                 if (source_cache.ready_queue.dequeue()) |source| {
    //                     systemLog.debug("Send source: {s}", .{source.header.name});
    //                     try self.connection.dispatcher.reply(item.socket, .{.topic_body = source}, item.routing_id);
    //                 }
    //                 else {
    //                     // delay 1 cycle
    //                     try self.connection.dispatcher.delay(item.socket, item.from, .pending_finish_topic_body, item.routing_id);
    //                 }
    //             },
    //             .pending_finish_topic_body => {
    //                 if ((self.connection.dispatcher.state.level.terminating) and (source_cache.isEmpty())) {
    //                     systemLog.debug("No more sources", .{});
    //                     try self.connection.dispatcher.reply(item.socket, .finish_topic_body, item.routing_id);
    //                 }
    //                 else {
    //                     systemLog.debug("Wait receive next source", .{});
    //                     try self.connection.dispatcher.reply(item.socket, .ack, item.routing_id);
    //                 }
    //             },
    //             .finish_generate => {
    //                 if (!watch_mode) {
    //                     // request quit for Generate stage
    //                     try self.connection.dispatcher.reply(item.socket, .quit, item.routing_id);
    //                 }
    //                 else {
    //                     try self.connection.dispatcher.reply(item.socket, .ack, item.routing_id);
    //                 }
    //             },
    //             .pending_fatal_quit => {
    //                 try self.connection.dispatcher.post(.quit_all);
    //             },
    //             .worker_response => |payload| {
    //                 const x: ?core.Event = try self.handleWorkerResponse(payload);
    //                 if (x) |next_event| {
    //                     try self.connection.dispatcher.post(next_event);
    //                 }
    //             },
    //             .quit_accept => {
    //                 try self.connection.dispatcher.reply(item.socket, .ack, item.routing_id);

    //                 left_launched -= 1;
    //                 systemLog.debug("Quit acceptrd: {s} (left: {})", .{item.from, left_launched});

    //                 if (left_launched <= 0) {
    //                     systemLog.debug("All Quit acceptrd", .{});
    //                     try self.connection.dispatcher.state.done();
    //                 }
    //             },
    //             .log => |payload| {
    //                 try self.connection.dispatcher.reply(item.socket, .ack, item.routing_id);
    //                 log(payload.level, item.from, payload.content);
    //             },
    //             .report_fatal => |payload| {
    //                 try self.connection.dispatcher.reply(item.socket, .quit, item.routing_id);
    //                 log(payload.level, item.from, payload.content);
    //                 try self.connection.dispatcher.delay(item.socket, item.from, .pending_fatal_quit, item.routing_id);
    //             },
    //             else => {
    //                 try self.connection.dispatcher.reply(item.socket, .ack, item.routing_id);
    //                 systemLog.debug("Discard command: {}", .{std.meta.activeTag(item.event)});
    //             },
    //         }
    //     }
    // }

    // systemLog.debug("terminated", .{});
}

pub fn log(self: *HostRunner, comptime level: events.LogLevel, comptime fmt: []const u8, args: anytype) !void {
    if (! comptime std.log.logEnabled(level.toStdLevel(), .default)) return;
    try self.dispatcher.log(level, app_context, fmt, args);
}

fn onDispatch(dispatcher: *EventDispatcher.Sized(poller_size), entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) anyerror!void {
    const self: *HostRunner = @alignCast(@fieldParentPtr("dispatcher", dispatcher));

    switch (self.state) {
        .booting => |*state| {
            try state.handle(self, self.setting, entry, dirty);
        },
        .terminating => |*state| {
            try state.handle(self, entry, dirty);
        },
        else => {
            // TODO:
            // Invalid phase
            unreachable;
        }
    }
}

pub fn defaultHandler(self: *HostRunner, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {    
    switch (entry.event) {
        .log => {
            unreachable;
        },
        .heartbeat => {
            // discard
            try self.dispatcher.log(.debug, app_context, "Discard/event:heartbeat, phase: {s}", .{@tagName(self.dispatcher.phase)});
        },
        else => {
            dirty.* = .unhandled;
        }
    }
}

pub fn transitPhase(self: *HostRunner, phase: EventDispatcher.Phase) !void {
    switch (phase) {
        .request => try self.doRequestPhase(),
        .terminating => try self.doTerminatePhase(),
        .quitting => {
            self.dispatcher.phase = .quitting;
        },
        else => unreachable,
    }
}

fn doRequestPhase(self: *HostRunner) !void {
    // TODO:
    // stub impl
    try self.doTerminatePhase();

    // self.state.deinit();
    // self.state = .{ .request = RequestPhaseState.create() };

    // // TODO:
    // // Send command

    // self.dispatcher.phase = .request;
    // try self.dispatcher.log(.debug, app_context, "Switched to phase: {s}", .{self.dispatcher.phase});
}

fn doTerminatePhase(self: *HostRunner) !void {
    self.state.deinit();
    self.state = .{ .terminating = try TerminatePhaseState.create(self.allocator, self.guest_names, .unlimited) };

    try self.sendProbe(.quit_all, 1, self.state.terminating.limit);

    self.dispatcher.phase = .terminating;
    try self.log(.debug, "Start quitting...", .{});
}

pub fn sendProbe(stage: *HostRunner, event: events.Event, count: usize, limit: HeartbeatTask.Limit) !void {
    return task_support.sendProbe(
        stage.io, stage.reapers, 
        app_context, stage.connection, 
        poller_size, &stage.dispatcher, 
        event, count, limit, TIMER_INTERVAL
    );
}

// TODO:
// fn onAfterLaunch(self: HostRnner, socket: *zmq.ZSocket, routing_id: ?core.Symbol) !void {
//     if (self.connection.dispatcher.state.level.terminating) {
//         traceLog.debug("Stopping launch process", .{});
//         try self.connection.dispatcher.delay(socket, app_context, .pending_fatal_quit, routing_id);
//     }
// }

fn handleTopicBody(self: *const HostRunner, topic_body: core.events.Event.Payload.TopicBody, source_cache: *PayloadCacheManager) !?core.Event {
    switch (try source_cache.update(topic_body)) {
        .expired => {
            self.log(.debug, "Content expired: {s}", .{topic_body.header.path});
        },
        .missing => {
            self.log(.debug, "Waiting left content: {s}", .{topic_body.header.path});
        },
        .fulfil => {
            self.log(.debug, "Source is ready: {s}", .{topic_body.header.name});
            if (try source_cache.ready(topic_body.header, topic_body.index)) {
                return .ready_topic_body;
            }
        },
    }

    return null;
}

fn handleSkipTopicBody(self: *const HostRunner, topic_body: core.Event.Payload.SkipTopicBody, source_cache: *PayloadCacheManager) !void {
    _ = self;
    try source_cache.dismiss(topic_body.header, topic_body.index);
}

// TODO:
// fn spawnCommandPallet(self: *HostRnner) !void {
//     const worker = try CommandPallet.init(self.allocator);
//     try self.connection.pull_sink_socket.spawn(worker);
// }

// fn handleWorkerResponse(self: *HostRnner, res: core.Event.Payload.WorkerResponse) !?core.Event {
//     var reader = core.CborStream.Reader.init(res.content);

//     switch (try reader.readEnum(CommandPallet.Status)) {
//         .invalid => {
//             const message = try reader.readString();
//             std.debug.print("{s}\n", .{message});
//             try self.spawnCommandPallet();
//             return null;
//         },
//         .accept => {
//             return try self.handleCommand(try reader.readEnum(CommandPallet.Command));
//         }
//     }
// }

// fn handleCommand(self: *Self, command: CommandPallet.Command) !?core.Event {
//     switch (command) {
//         .help => {
//             try CommandPallet.showCommandhelp(self.allocator);
//             try self.spawnCommandPallet();
//             return null;
//         },
//         .quit => {
//             return .quit_all;
//         },
//         .run => {
//             try self.spawnCommandPallet();
//             return .ready_watch_path;
//         }
//     }
// }

const State = union(EventDispatcher.Phase) {
    booting: BootPhaseState,
    request: RequestPhaseState,
    ready: void,
    terminating: TerminatePhaseState,
    quitting: void,

    const deinit = deinitState;
};

fn deinitState(self: *State) void {
    switch (self.*) {
        .booting => |*state| state.deinit(),
        .request => |*state| state.deinit(),
        .terminating => |*state| state.deinit(),
        else => unreachable,
    }
}

const RequestPhaseState = struct {
    const Self = @This();

    pub fn create() !Self {
        return .{};
    }

    pub fn deinit(self: *Self) void {
        _ = self;
    }
};

test "Runner tests" {
    std.testing.refAllDecls(@This());
}

pub const tests = struct {
    const RunnerTestContext = struct {
        allocator: std.mem.Allocator, 
        runner: Runner,
        cache_manager: PayloadCacheManager,
        category: core.TopicCategory,

        const Runner = HostRunner;

        pub fn init(allocator: std.mem.Allocator, category: core.TopicCategory) !RunnerTestContext {
            const setting: Setting = .{
                .arena = undefined,
                .general = .{
                    .runner_endpoints = core.DebugEndPoint.RunnerEndpoint,
                    .stage_endpoints = core.DebugEndPoint.StageEndpoint,
                    .log_level = .info,
                    .scope = "default",
                },
                .command = undefined,
            };
            try core.makeIpcChannelRoot(setting.general.stage_endpoints);
            defer core.cleanupIpcChannelRoot(setting.general.stage_endpoints);

            var self: RunnerTestContext = .{
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
};