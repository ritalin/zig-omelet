const std = @import("std");
const core = @import("core");

const events = core.events;

const Setting = @import("../settings/Setting.zig");
const HeartbeatTask = @import("../tasks/HeartbeatTask.zig");

const stage_name = "host";
const EventDispatcher = core.sockets.EventDispatcher;
const ReceiveEntry = core.sockets.ReceiveEntry;
const TaskReaper = core.TaskReaper;

const task_support = @import("./task_support.zig");

pub const createTmpDir = core.test_supports.createTmpDir;
pub const testEndpointConfig = core.test_supports.testEndpointConfig;
pub const releaseEndpoint = core.test_supports.releaseEndpoint;

pub const POLLER_SIZE = 4;
pub const Connection = core.sockets.Connection.Server(stage_name);

pub fn writeAssetFile(tmp_dir: *const std.testing.TmpDir, options: core.configs.supports.FileResolveOptions, content: core.types.Symbol) !void {
    const io = std.testing.io;
    const allocator = std.testing.allocator;

    const category_name: core.types.Symbol = @tagName(options.category);
    const dir_path = try std.fmt.allocPrint(allocator, "{f}", .{std.fs.path.fmtJoin(&.{options.root.current_dir.?, category_name, options.scope})});
    defer allocator.free(dir_path);

    const dir = try tmp_dir.dir.createDirPathOpen(io, dir_path, .{});
    defer dir.close(io);

    const file_name = try std.fmt.allocPrint(allocator, "{s}.zon", .{options.command});
    defer allocator.free(file_name);

    const file = try dir.createFile(io, file_name, .{});
    defer file.close(io);

    var buffer: [1024]u8 = undefined;
    var writer = file.writer(io, &buffer);
    try writer.interface.writeAll(content);
    try writer.flush();
}

pub fn sendMessage(channel: core.sockets.RpcChannel, event: core.events.Event) void {
    channel.submit(std.testing.io, event, .{}) catch |err| {
        std.debug.print("*** Test:sendMessage/err: {}\n", .{err});
    };
}

pub const TestStage = struct {
    setting: Setting,
    heartbeat_limit: HeartbeatTask.Limit,
    connection: *Connection,
    dispatcher: EventDispatcher.Sized(POLLER_SIZE),
    reaper: *TaskReaper,
    on_default: ?*const fn (self: *TestStage, entry: ReceiveEntry) anyerror!void = null,
    err: ?anyerror = null,

    pub fn init(connection: *Connection, ep: core.types.Endpoints, limit: HeartbeatTask.Limit) !TestStage {
        return .{
            .setting = .{
                .base = .{
                    .log_level = .debug,
                    .log_quiet = false,
                    .no_color = false,
                    .interactive = false,
                    .endpoints = ep,
                    .ipc_config = .default,
                    .scope = "default",
                    .config_scope = "default",
                },
                .command = .{.@"init-config" = .{ .source_dir_path = "", .output_dir_path = "", .target_scope = "" } },
            },
            .heartbeat_limit = limit,
            .connection = connection,
            .dispatcher = try connection.configureDispatcher(4, .{ .log_style = .discard }),
            .reaper = try TaskReaper.init(std.testing.io, std.testing.allocator),
        };
    }

    pub fn deinit(self: *TestStage) void {
        self.reaper.deinit(std.testing.allocator);
        self.dispatcher.deinit();
    }

    pub fn sendProbe(stage: *TestStage, event: events.Event, count: u64, limit: HeartbeatTask.Limit, interval: std.Io.Duration) !void {
        return task_support.sendProbe(
            std.testing.io, stage.reaper, 
            stage_name, stage.connection, 
            4, &stage.dispatcher, 
            event, count, limit, interval
        );
    }

    pub fn sendProbeHeartbeat(stage: *TestStage, _: events.EventType, phase: events.EventPhase.Kind, count: u64) !void {
        const interval = TestStage.nextInterval(0);
        return stage.sendProbe(
            .{.probe = phase},
            count,
            stage.heartbeat_limit,
            interval
        );
    }

    pub fn log(self: *TestStage, comptime level: core.events.LogLevel, comptime fmt: []const u8, args: anytype) !void {
        return self.dispatcher.log(level, stage_name, fmt, args);
    }

    pub fn run(self: *TestStage, on_dispatch: EventDispatcher.Sized(4).VTable.DispatchFn) void {
        self.dispatcher.run(stage_name, on_dispatch) catch |err| {
            self.err = err;
        };
    }

    pub fn transitPhase(self: *TestStage, phase_kind: events.EventPhase.Kind, phase_agree: events.EventPhase.Agreement) !void {
        _ = phase_kind;
        _ = phase_agree;
        self.dispatcher.phase = .{.kind = .quitting, .agreement = .confirmed};
    }

    pub fn defaultHandler(self: *TestStage, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
        _ = dirty;

        if (self.on_default) |on_default| {
            try on_default(self, entry);
        }
    }

    pub fn nextInterval(_: u64) std.Io.Duration {
        return .fromMilliseconds(50);
    }
};

pub fn cleanup() void {
    std.Io.sleep(std.testing.io, std.Io.Duration.fromMilliseconds(50), .awake) catch {};
}