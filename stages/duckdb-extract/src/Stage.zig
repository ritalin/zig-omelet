const std = @import("std");
const core = @import("core");
const c = @import("c");
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
database: c.DatabaseRef,
state: State,

// TODO:
// const Connection = core.sockets.Connection.Client(app_context, ExtractWorker);
pub const Connection = core.sockets.Connection.Client(app_context);

pub fn create(allocator: std.mem.Allocator, connection: *Connection, setting: *const Setting) !GuestStage {
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
        .state = .{ .launching = phase },
    };
}

pub fn deinit(self: *GuestStage) void {
    c.deinitDatabase(self.database);
    self.state.deinit(self.allocator);
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
            .request => try self.doRequestPhase(),
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

    if (failed) return error.LaunchFailed;
}

fn onDispatch(dispatcher: *EventDispatcher.Sized(1), entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) anyerror!void {
    const self: *GuestStage = @alignCast(@fieldParentPtr("dispatcher", dispatcher));

    switch (self.state) {
        .launching => |state| {
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
    launching: BootPhaseState,
    request: RequestTopicPhaseState,
    ready: ExtractTopicBodyState,
    terminating: void,
    quitting: void,

    const deinit = deinitState;
};

fn deinitState(self: *State, allocator: std.mem.Allocator) void {
    switch (self.*) {
        .launching => |*state| state.deinit(),
        .request => |*state| state.deinit(),
        .ready => |*state| state.deinit(allocator),
        else => unreachable,
    }
}

test "extract/duckdb" {
    std.testing.refAllDecls(@This());
}
