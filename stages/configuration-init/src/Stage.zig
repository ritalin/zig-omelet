const std = @import("std");
const core = @import("core");
const app_context = @import("build_options").app_context;

const events = core.events;

const EventDispatcher = core.sockets.EventDispatcher;
const Logger = core.Logger.withAppContext(app_context);
const ReceiveEntry = core.sockets.ReceiveEntry;

const BootPhaseState = core.guest_phases.BootPhaseState(GuestStage);

const Setting = @import("./Setting.zig");
// const PathMatcher = @import("./PathMatcher.zig").PathMatcher(u21);

const NewConfigurationState = @import("./phases/ready_phase.zig").NewConfigurationState(GuestStage);

const GuestStage = @This();

allocator: std.mem.Allocator,
setting: *const Setting,
connection: *GuestStage.Connection,
dispatcher: EventDispatcher.Sized(1),
state: State,

// TODO:
// const Connection = core.sockets.Connection.Client(app_context, GenerateWorker);
pub const Connection = core.sockets.Connection.Client(app_context);

pub fn create(allocator: std.mem.Allocator, connection: *Connection, setting: *const Setting) !GuestStage {
    errdefer connection.deinit();

    try connection.subscribe(&.{
        .probe,
        .ready_progress,
    });
    try connection.connect();

    const options: EventDispatcher.Options = .{ 
        .log_style = setting.log_style,
        .no_color = setting.no_color, 
    };
    const dispatcher = try connection.configureDispatcher(1, options);

    return .{
        .allocator = allocator,
        .setting = setting,
        .connection = connection,
        .dispatcher = dispatcher,
        .state = .{ .launching = BootPhaseState.init },
    };
}

pub fn deinit(self: *GuestStage) void {
    self.state.deinit();
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
            .request => {}, 
            .ready => try self.doReadyPhase(),
            .quitting => {},
            else => unreachable,
        }
    }
    self.dispatcher.phase = phase;
}

pub fn defaultHandler(self: *GuestStage, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
    switch (entry.event) {
        .probe => |phase| {
            // TODO: stum impl
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
                .request => {
                    var channel = try self.connection.requestChannel();
                    try channel.submit(self.connection.context.io, .finish_topic, .{});
                    try self.transitPhase(.ready, .pending);
                },
                .terminating => {
                    // TODO: pending -> confirmed
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

fn doReadyPhase(self: *GuestStage) !void {
    self.state.deinit();
    self.state = .{ .ready = NewConfigurationState.create };
}

fn onDispatch(dispatcher: *EventDispatcher.Sized(1), entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) anyerror!void {
    const self: *GuestStage = @alignCast(@fieldParentPtr("dispatcher", dispatcher));

    switch (self.state) {
        .launching => |state| {
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
    launching: BootPhaseState,
    request: void,
    ready: NewConfigurationState,
    terminating: void,
    quitting: void,

    const deinit = deinitState;
};

fn deinitState(self: *State) void {
    switch (self.*) {
        .launching => |*state| state.deinit(),
        .ready => |*state| state.deinit(),
        else => unreachable,
    }
}
