const std = @import("std");
const core = @import("core");
const app_context = @import("build_options").app_context;

const types = core.types;
const events = core.events;

const poller_size = 6;

const Symbol = core.Symbol;
const ReceiveEntry = core.sockets.ReceiveEntry;
const EventDispatcher = core.sockets.EventDispatcher;
const TaskReaper = core.TaskReaper;

const Setting = @import("./settings/Setting.zig");
const CacheManager = @import("./cache_manager.zig").CacheManager;
const PayloadCacheManager = CacheManager.Payload(app_context);

const Config = @import("./configs/Config.zig");

const HeartbeatTask = @import("./tasks/HeartbeatTask.zig");
const task_support = @import("./supports/task_support.zig");

const BootPhaseState = @import("./phases/boot_phase.zig").BootPhaseState(HostRunner);
const RequestPhaseState = @import("./phases/request_phase.zig").RequestPhaseState(HostRunner);
const ReadyPhaseState = @import("./phases/ready_phase.zig").ReadyPhaseState(HostRunner);
const TerminatePhaseState = @import("./phases/terminate_phase.zig").TerminatePhaseState(HostRunner);

const HostRunner = @This();

allocator: std.mem.Allocator,
io: std.Io,
setting: *const Setting,
connection: *HostRunner.Connection,
dispatcher: EventDispatcher.Sized(poller_size),
state: State = undefined,
config: *const Config,
guest_names: std.BufSet,
reapers: *TaskReaper,

// TODO:
// const CommandPallet = @import("./CommandPallet.zig");
// const Connection = core.sockets.Connection.Server(app_context, CommandPallet);
pub const Connection = core.sockets.Connection.Server(app_context);

pub fn create(io: std.Io, allocator: std.mem.Allocator, connection: *Connection, config: *const Config, setting: *const Setting) !HostRunner {
    try connection.bind();

    const options: EventDispatcher.Options = .{ 
        .log_style = if (setting.base.log_quiet) .discard else .stderr,
        .no_color = setting.base.no_color, 
    };

    var guests = std.BufSet.init(allocator);
    for (config.guests.items(.name)) |name| {
        try guests.insert(name);
    }

    return .{
        .allocator = allocator,
        .io = io,
        .setting = setting,
        .connection = connection,
        .dispatcher = try connection.configureDispatcher(poller_size, options),
        .config = config,
        .guest_names = guests,
        .reapers = try TaskReaper.init(io, allocator),
    };
}

pub fn deinit(self: *HostRunner) void {
    self.guest_names.deinit();
    self.state.deinit();
    self.reapers.deinit(self.allocator);
    self.dispatcher.deinit();
}

pub fn run(self: *HostRunner) !void {
    self.dispatcher.run(app_context, HostRunner.onDispatch) catch |err| {
        // TODO: fatal error log
        return err;
    };    
}

pub fn log(self: *HostRunner, comptime level: events.LogLevel, comptime fmt: []const u8, args: anytype) !void {
    if (! comptime std.log.logEnabled(level.toStdLevel(), .default)) return;
    try self.dispatcher.log(level, app_context, fmt, args);
}

pub fn isHost(stage_name: types.StageName) bool {
    return std.mem.eql(u8, stage_name, app_context);
}

fn onDispatch(dispatcher: *EventDispatcher.Sized(poller_size), entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) anyerror!void {
    const self: *HostRunner = @alignCast(@fieldParentPtr("dispatcher", dispatcher));
    try self.reapers.tick();

    if (!std.mem.eql(u8, entry.from_stage, app_context)) {
        if (!self.guest_names.contains(entry.from_stage)) {
            try self.log(.warn, "Forbid external guest event/name: {s}, event: {s}", .{entry.from_stage, @tagName(std.meta.activeTag(entry.event))});
            return;
        }
    }

    switch (self.state) {
        .launching => |*state| {
            try state.handle(self, entry, dirty);
        },
        .request => |*state| {
            try state.handle(self, entry, dirty);
        },
        .ready => |*state| {
            try state.handle(self, entry, dirty);
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
        .log => |payload| {
            if (core.Logger.accepted(payload.level)) {
                try self.dispatcher.log_router.handleLogEvent(entry.from_stage, payload);
            }
        },
        .heartbeat => |payload| {
            // discard
            try self.log(.debug, "Discard/event:heartbeat.{s}, phase: {s}", .{@tagName(payload.event_type), @tagName(self.dispatcher.phase.kind)});
        },
        else => {
            dirty.* = .unhandled;
        }
    }
}

pub fn transitPhase(self: *HostRunner, phase_kind: events.EventPhase.Kind, phase_agree: events.EventPhase.Agreement) !void {
    const phase: events.EventPhase = .{ .kind = phase_kind, .agreement = phase_agree};

    if (phase_agree == .pending) {
        switch (phase_kind) {
            .launching => try self.doBootingPhase(),
            .request => try self.doRequestPhase(),
            .ready => try self.doReadyPhase(),
            .terminating => try self.doTerminatePhase(),
            .quitting => {},
        }
    }
    else {
        // TODO: move to on_quit callback
        if (phase_kind == .quitting) {
            self.reapers.cancel(self.io);
        }
    }
    self.dispatcher.phase = phase;
}

fn doBootingPhase(self: *HostRunner) !void {
    const next_state = try BootPhaseState.create(
        self.allocator, 
        &self.guest_names
    ); 
    self.state = .{ .launching = next_state };
}

fn doRequestPhase(self: *HostRunner) !void {
    const next_state = try RequestPhaseState.create(
        self.allocator, 
        &self.guest_names
    );

    self.state.deinit();
    self.state = .{ .request = next_state };

    try self.sendProbe(.{.probe =.request}, 1, self.config.host.heartbeat_limit, self.config.host.heartbeat_interval);

}

fn doReadyPhase(self: *HostRunner) !void {
    const next_state = try ReadyPhaseState.create(
        self.allocator,
        &self.guest_names,
        &self.config.guests,
        try self.state.request.drainTopics(self.allocator)
    );
    self.state.deinit();
    self.state = .{ .ready = next_state };

    try self.sendProbe(.{.probe = .ready}, 1, self.config.host.heartbeat_limit, self.config.host.heartbeat_interval);
}

fn doTerminatePhase(self: *HostRunner) !void {
    self.state.deinit();
    self.state = .{ .terminating = try TerminatePhaseState.create(self.allocator, &self.guest_names) };

    try self.sendProbe(.{.probe = .terminating}, 1, self.config.host.heartbeat_limit, self.config.host.heartbeat_interval);
    try self.log(.debug, "Start quitting...", .{});
}

pub fn sendProbe(self: *HostRunner, event: events.Event, count: usize, limit: HeartbeatTask.Limit, interval: std.Io.Duration) !void {
    return task_support.sendProbe(
        self.io, self.reapers, 
        app_context, self.connection, 
        poller_size, &self.dispatcher, 
        event, count, limit, interval
    );
}

pub fn sendProbeHeartbeat(self: *HostRunner, event_type: events.EventType, phase: events.EventPhase.Kind, count: u64) !void {
    if ((event_type == .probe) and (std.meta.eql(self.dispatcher.phase, .{.kind = phase, .agreement = .pending}))) {
        const interval = self.config.host.heartbeat_interval;
        try self.sendProbe(.{.probe = phase}, count, self.config.host.heartbeat_limit, interval);
    }
    else {
        return error.DiscardProbe;
    }
}

pub fn sendProgressHeartbeat(self: *HostRunner) !void {
    const interval = self.config.host.ready_progress_interval;
    try self.sendProbe(.ready_progress, 1, self.config.host.heartbeat_limit, interval);
}

// TODO:
// fn onAfterLaunch(self: HostRnner, socket: *zmq.ZSocket, routing_id: ?core.Symbol) !void {
//     if (self.connection.dispatcher.state.level.terminating) {
//         traceLog.debug("Stopping launch process", .{});
//         try self.connection.dispatcher.delay(socket, app_context, .pending_fatal_quit, routing_id);
//     }
// }

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

const State = union(events.EventPhase.Kind) {
    launching: BootPhaseState,
    request: RequestPhaseState,
    ready: ReadyPhaseState,
    terminating: TerminatePhaseState,
    quitting: void,

    const deinit = deinitState;
};

fn deinitState(self: *State) void {
    switch (self.*) {
        .launching => |*state| state.deinit(),
        .request => |*state| state.deinit(),
        .ready => |*state| state.deinit(),
        .terminating => |*state| state.deinit(),
        else => unreachable,
    }
}
