const std = @import("std");
const core = @import("core");
const c = @import("c");
const app_context = @import("build_options").app_context;
const forward_worker = @import("build_options").forward_worker;

const events = core.events;

const EventDispatcher = core.sockets.EventDispatcher;
const Logger = core.Logger.withAppContext(app_context);
const ReceiveEntry = core.sockets.ReceiveEntry;

const BootPhaseState = core.guest_phases.BootPhaseState(GuestStage);
const ConnectPhaseState = core.guest_phases.ConnectPhaseState(GuestStage);
const RequestTopicPhaseState = @import("./phases/request_phase.zig").RequestTopicPhaseState(GuestStage);
const ExtractTopicBodyState = @import("./phases/ready_phase.zig").ExtractTopicBodyState(GuestStage);

const Setting = @import("./Setting.zig");
const ExtractWorker = @import("./ExtractWorker.zig");

const Symbol = core.Symbol;

const GuestStage = @This();

allocator: std.mem.Allocator,
setting: *const Setting,
connection: *GuestStage.Connection,
dispatcher: EventDispatcher.Sized(1),
database: c.DatabaseRef,
state: State,
reapers: *core.TaskReaper,

pub const Connection = core.sockets.Connection.Client(app_context);

pub fn create(io: std.Io, allocator: std.mem.Allocator, connection: *Connection, setting: *const Setting) !GuestStage {
    errdefer connection.deinit();

    try connection.subscribe(&.{
        .probe,
        .ready_progress,

        .source_path,
        .finish_source_path,
    });
    try connection.connect();

    const options: EventDispatcher.Options = .{ 
        .log_style = setting.log_style,
        .no_color = setting.no_color, 
        .forward_worker = forward_worker,
    };
    const dispatcher = try connection.configureDispatcher(1, options);

    var database: c.DatabaseRef = undefined;
    _ = c.initDatabase(&database);

    var phase = BootPhaseState.init;
    phase.vtable.on_prepare = GuestStage.doPrepareLaunching;

    return .{
        .allocator = allocator,
        .setting = setting,
        .connection = connection,
        .dispatcher = dispatcher,
        .database = database,
        .state = .preboot,
        .reapers = try core.TaskReaper.init(io, allocator),
    };
}

pub fn deinit(self: *GuestStage) void {
    c.deinitDatabase(self.database);
    self.state.deinit(self.allocator);
    self.reapers.deinit(self.allocator);
    self.dispatcher.deinit();
}

pub fn run(self: *GuestStage) !void {
    self.dispatcher.run(app_context, GuestStage.onDispatch) catch |err| {
        // TODO: fatal error log
        // try self.connection.dispatcher.postFatal(@errorReturnTrace());
        return err;
    };
}

pub fn log(self: *GuestStage, comptime level: events.LogLevel, comptime fmt: []const u8, args: anytype) !void {
    if (! comptime std.log.logEnabled(level.toStdLevel(), .default)) return;
    try self.dispatcher.log(level, app_context, fmt, args);
}

pub fn transitPhase(self: *GuestStage, phase_kind: events.EventPhase.Kind, phase_agree: events.EventPhase.Agreement) !void {
    const phase: events.EventPhase = .{ .kind = phase_kind, .agreement = phase_agree};
    if (std.meta.eql(self.dispatcher.phase, phase)) return;

    if (phase_agree == .pending) {
        switch (phase_kind) {
            .boot => try self.doBootPhase(),
            .connecting => try self.doConnectingPhase(),
            .request => try self.doRequestPhase(),
            .ready => try self.doReadyPhase(),
            .terminating => {},
            .quitting => {},
            else => unreachable,
        }
    }
    self.dispatcher.phase = phase;
}

pub fn defaultHandler(self: *GuestStage, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
    switch (entry.event) {
        .probe => |phase| {
            if ((phase == .terminating)) {
                try self.transitPhase(.quitting, .confirmed);
                return;
            }

            if (self.dispatcher.phase.kind != phase) {
                try self.log(.debug, "Phase unmatched/phase: {s}, current: {s}", .{@tagName(phase), @tagName(self.dispatcher.phase.kind)});
                return;
            }
            if (self.dispatcher.phase.agreement == .confirmed) {
                try self.log(.debug, "Discard probe/phase: {s}", .{@tagName(phase)});
                return;
            }
            switch (phase) {
                .terminating => {
                    try self.transitPhase(.quitting, .confirmed);
                },
                else => {
                    dirty.* = .unhandled;
                }
            }
        },
        .ready_progress => {
            // discard
            try self.log(.trace, "Discard ready progress", .{});
        },
        else => {
            dirty.* = .unhandled;
        }
    }
}

fn doBootPhase(self: *GuestStage) !void {
    self.state.deinit(self.allocator);
    self.state = .{ .boot = BootPhaseState.init };

    try self.dispatcher.queue.pushReceiveQueue(try core.sockets.ReceiveEntry.booting(GuestStage.Connection.stage_name));
}

fn doConnectingPhase(self: *GuestStage) !void {
    self.state.deinit(self.allocator);
    self.state = .{ .connecting = ConnectPhaseState.init };
}

fn doRequestPhase(self: *GuestStage) !void {
    self.state.deinit(self.allocator);
    self.state = .{ .request = .create };
}

fn doReadyPhase(self: *GuestStage) !void {
    self.state.deinit(self.allocator);
    self.state = .{ .ready = try .create(self.allocator) };
}

fn doPrepareLaunching(self: *GuestStage) !void {
    var failed = false;

    for (self.setting.schema_dir_set) |path| {
        try self.log(.info, "Loading schema/path: {s}", .{path});

        const err = c.loadSchema(self.database, .{.ptr = path.ptr, .len = path.len});
        switch (err) {
            c.schema_dir_not_found => {
                try self.log(.err, "Loading schema failed. Invalid schema location", .{});
                failed = true;
            },
            c.schema_load_failed => {
                try self.log(.err, "Launch failed. Invalid schema definitions", .{});
                failed = true;
            },
            else => {},
        }
    }
    user_type: {
        const err = c.retainUserTypeName(self.database);
        switch (err) {
            c.invalid_schema_catalog => {
                try self.log(.err, "Launch failed. Invalid schema catalog.", .{});
                failed = true;
            },
            else => {},
        }
        break:user_type;
    }

    if (failed) return error.LaunchFailed;
}

fn onDispatch(dispatcher: *EventDispatcher.Sized(1), entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) anyerror!void {
    const self: *GuestStage = @alignCast(@fieldParentPtr("dispatcher", dispatcher));

    try self.reapers.tick();

    switch (self.state) {
        .boot => |state| {
            try state.handle(self, entry, dirty);
        },
        .connecting => |state| {
            try state.handle(self, entry, dirty);
        },
        .request => |*state| {
            try state.handle(self, entry, dirty);
        },
        .ready => |state| {
            try state.handle(self, entry, dirty);
        },
        else => {
            unreachable;
        }
    }
}

const State = union(events.EventPhase.Kind) {
    preboot: void,
    boot: BootPhaseState,
    connecting: ConnectPhaseState,
    request: RequestTopicPhaseState,
    ready: ExtractTopicBodyState,
    terminating: void,
    quitting: void,
    quit_done: void,

    const deinit = deinitState;
};

fn deinitState(self: *State, allocator: std.mem.Allocator) void {
    switch (self.*) {
        .preboot => {},
        .boot => |*state| state.deinit(),
        .connecting => |*state| state.deinit(),
        .request => |*state| state.deinit(),
        .ready => |*state| state.deinit(allocator),
        else => unreachable,
    }
}

