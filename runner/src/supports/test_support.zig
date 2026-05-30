const std = @import("std");
const core = @import("core");

const events = core.events;

const Setting = @import("../settings/Setting.zig");
const TaskReaper = @import("../supports/TaskReaper.zig");
const HeartbeatTask = @import("../tasks/HeartbeatTask.zig");

const stage_name = "host";
const EventDispatcher = core.sockets.EventDispatcher;
const ReceiveEntry = core.sockets.ReceiveEntry;

const task_support = @import("./task_support.zig");

pub const createTmpDir = core.test_support.createTmpDir;
pub const createEndpoint = core.test_support.createEndpoint;
pub const releaseEndpoint = core.test_support.releaseEndpoint;

pub const POLLER_SIZE = 4;
pub const Connection = core.sockets.Connection.Server(stage_name);

pub fn sendMessage(channel: core.sockets.RpcChannel, event: core.events.Event) void {
    channel.submit(std.testing.io, event, .{}) catch |err| {
        std.debug.print("*** Test:sendMessage/err: {}\n", .{err});
    };
}

pub const TestStage = struct {
    setting: Setting,
    connection: *Connection,
    dispatcher: EventDispatcher.Sized(POLLER_SIZE),
    reaper: *TaskReaper,
    on_default: ?*const fn (self: *TestStage, entry: ReceiveEntry) anyerror!void = null,
    err: ?anyerror = null,

    pub fn init(connection: *Connection, ep: core.types.Endpoints, limit: HeartbeatTask.Limit) !TestStage {
        return .{
            .setting = .{
                .general = .{
                    .log_level = .debug,
                    .log_style = .discard,
                    .no_color = false,
                    .stage_endpoints = ep,
                    .boot_limit = limit,
                },
            },
            .connection = connection,
            .dispatcher = try connection.configureDispatcher(4, .{ .log_style = .discard }),
            .reaper = try TaskReaper.init(std.testing.io, std.testing.allocator),
        };
    }

    pub fn deinit(self: *TestStage) void {
        self.reaper.deinit(std.testing.allocator);
        self.dispatcher.deinit();
    }

    pub fn sendProbe(stage: *TestStage, event: events.Event, count: usize, limit: HeartbeatTask.Limit) !void {
        return task_support.sendProbe(
            std.testing.io, stage.reaper, 
            stage_name, stage.connection, 
            4, &stage.dispatcher, 
            event, count, limit, std.Io.Duration.fromMilliseconds(50)
        );
    }

    pub fn log(self: *TestStage, comptime level: core.events.LogLevel, comptime fmt: []const u8, args: anytype) !void {
        return self.dispatcher.log(level, stage_name, fmt, args);
    }

    pub fn run(self: *TestStage, on_dispatch: EventDispatcher.Sized(4).DispatchFn) void {
        self.dispatcher.run(stage_name, on_dispatch) catch |err| {
            self.err = err;
        };
    }

    pub fn transitPhase(self: *TestStage, phase: EventDispatcher.Phase) !void {
        _ = phase;
        self.dispatcher.phase = .quitting;
    }

    pub fn defaultHandler(self: *TestStage, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
        _ = dirty;

        if (self.on_default) |on_default| {
            try on_default(self, entry);
        }
    }
};

pub fn cleanup() void {
    std.Io.sleep(std.testing.io, std.Io.Duration.fromMilliseconds(50), .awake) catch {};
}