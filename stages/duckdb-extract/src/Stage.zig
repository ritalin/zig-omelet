const std = @import("std");
const core = @import("core");
// TODO:
// const c = @import("worker_runtime");
const app_context = @import("build_options").app_context;

const events = core.events;

const EventDispatcher = core.sockets.EventDispatcher;
const Logger = core.Logger.withAppContext(app_context);
const ReceiveEntry = core.sockets.ReceiveEntry;

const BootPhaseState = core.guest_phases.BootPhaseState(GuestStage);
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
// TODO:
// database: c.DatabaseRef,
state: State,

// TODO:
// const Connection = core.sockets.Connection.Client(app_context, ExtractWorker);
pub const Connection = core.sockets.Connection.Client(app_context);

pub fn create(allocator: std.mem.Allocator, connection: *Connection, setting: *const Setting) !GuestStage {
    errdefer connection.deinit();

    try connection.subscribe(&.{
        .probe_launching,
        .probe_request,
        .probe_ready,

        .source_path,
        .finish_source_path,
        .quit_all,
    });
    try connection.connect();

    const options: EventDispatcher.Options = .{ 
        .log_style = setting.log_style,
        .no_color = setting.no_color, 
    };
    const dispatcher = try connection.configureDispatcher(1, options);

    // TODO:
    // var database: c.DatabaseRef = undefined;
    // _ = c.initDatabase(&database);

    return .{
        .allocator = allocator,
        .setting = setting,
        .connection = connection,
        .dispatcher = dispatcher,
        // .database = database,
        .state = .{ .booting = BootPhaseState.init },
    };
}

pub fn deinit(self: *GuestStage) void {
    // c.deinitDatabase(self.database);
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

pub fn transitPhase(self: *GuestStage, phase: EventDispatcher.Phase) !void {
    if (self.dispatcher.phase == phase) return;
    switch (phase) {
        .request => try self.doRequestPhase(),
        .ready => try self.doReadyPhase(),
        else => unreachable,
    }
}

pub fn defaultHandler(self: *GuestStage, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
    switch (entry.event) {
        .quit_all => {
            self.dispatcher.phase = .quitting;
        },
        else => {
            dirty.* = .unhandled;
        }
    }
}

fn doRequestPhase(self: *GuestStage) !void {
    self.state.deinit();
    self.state = .{ .request = .create };
    self.dispatcher.phase = .request;
}

fn doReadyPhase(self: *GuestStage) !void {
    self.state.deinit();
    self.state = .{ .ready = .create };
    self.dispatcher.phase = .ready;
}

fn onDispatch(dispatcher: *EventDispatcher.Sized(1), entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) anyerror!void {
    const self: *GuestStage = @alignCast(@fieldParentPtr("dispatcher", dispatcher));

    switch (self.state) {
        .booting => |state| {
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

const State = union(EventDispatcher.Phase) {
    booting: BootPhaseState,
    request: RequestTopicPhaseState,
    ready: ExtractTopicBodyState,
    terminating: void,
    quitting: void,

    const deinit = deinitState;
};

fn deinitState(self: *State) void {
    switch (self.*) {
        .booting => |*state| state.deinit(),
        .request => |*state| state.deinit(),
        .ready => |*state| state.deinit(),
        else => unreachable,
    }
}

test "extract/duckdb" {
    std.testing.refAllDecls(@This());
}
