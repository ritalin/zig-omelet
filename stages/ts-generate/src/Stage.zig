const std = @import("std");
const core = @import("core");
const app_context = @import("build_options").app_context;

const events = core.events;

const EventDispatcher = core.sockets.EventDispatcher;
const Logger = core.Logger.withAppContext(app_context);
const ReceiveEntry = core.sockets.ReceiveEntry;

const BootPhaseState = core.guest_phases.BootPhaseState(GuestStage);
const ReadyPhaseState = @import("./phases/ready_phase.zig").ReadyPhaseState(GuestStage);

const GenerateWorker = @import("./GenerateWorker.zig");
const Setting = @import("./Setting.zig");
const CodeBuilder = @import("./CodeBuilder.zig");

const GuestStage = @This();

allocator: std.mem.Allocator,
setting: *const Setting,
connection: *GuestStage.Connection,
dispatcher: EventDispatcher.Sized(1),
state: State,

// TODO:
// pub const Connection = core.sockets.Connection.Client(app_context, GenerateWorker);
pub const Connection = core.sockets.Connection.Client(app_context);

pub fn create(allocator: std.mem.Allocator, connection: *Connection, setting: *const Setting) !GuestStage {
    errdefer connection.deinit();

    try connection.subscribe(&.{
        .probe,

        .ready_topic_body,
        .topic_body,
        .finish_topic_body,
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
        // .logger = Logger.init(allocator, connection.dispatcher, setting.standalone),
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
            .quitting => self.doQuitPhase(),
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
        else => {
            dirty.* = .unhandled;
        }
    }
}

fn doReadyPhase(self: *GuestStage) !void {
    self.state.deinit();
    self.state = .{ .ready = ReadyPhaseState.create() };
}

fn doQuitPhase(self: *GuestStage) void {
    self.state.deinit();
}

fn onDispatch(dispatcher: *EventDispatcher.Sized(1), entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) anyerror!void {
    const self: *GuestStage = @alignCast(@fieldParentPtr("dispatcher", dispatcher));

    switch (self.state) {
        .launching => |state| {
            try state.handle(self, entry, dirty);
        },
        .ready => |*state| {
            try state.handle(self, entry, dirty);
        },
        else => {
            unreachable;
        }
    }
    // TODO:
    // const _item = self.connection.dispatcher.dispatch() catch |err| switch (err) {
    //     error.InvalidResponse => {
    //         try self.logger.log(.warn, "Unexpected data received", .{});
    //         return;
    //     },
    //     else => return err,
    // };

    // if (_item) |*item| {
    //     defer item.deinit();

    //     switch (item.event) {
    //         .ready_topic_body => {
    //             try self.logger.log(.debug, "Ready for generating", .{});
    //             try self.connection.dispatcher.post(.ready_generate);
    //         },
    //         .topic_body => |source| {
    //             try self.connection.dispatcher.approve();
    //             try self.logger.log(.debug, "Accept source: `{s}`", .{source.header.path});

    //             const path = try source.header.clone(self.allocator);
    //             try lookup.put(path.path, path);

    //             const worker = try GenerateWorker.init(self.allocator, source, setting.output_dir_path);
    //             try self.connection.pull_sink_socket.spawn(worker);
    //             try self.connection.dispatcher.post(.ready_generate);
    //         },
    //         .worker_response => |res| {
    //             try self.processWorkResult(res.content, lookup);

    //             if (self.connection.dispatcher.state.level.terminating) {
    //                 if (lookup.count() == 0) {
    //                     try self.connection.dispatcher.post(.ready_generate);
    //                 }
    //             }
    //         },
    //         .finish_topic_body => {
    //             try self.connection.dispatcher.approve();
    //             try self.connection.dispatcher.state.receiveTerminate();

    //             if (lookup.count() == 0) {
    //                 if (self.connection.dispatcher.state.level.terminating) {
    //                     try self.connection.dispatcher.post(.finish_generate);
    //                 }
    //             }
    //             else {
    //                 try self.logger.log(.debug, "Cannot finish yet (left: {})", .{lookup.count()});
    //             }
    //         },
    //     }
    // }
}

fn processWorkResult(self: *GuestStage, result_content: core.Symbol, lookup: *std.StringHashMap(core.Event.Payload.SourcePath)) !void {
    var reader = core.CborStream.Reader.init(result_content);

    const source_path = try reader.readString();
    const dest_name = try reader.readString();
    const message = try reader.readString();
    const status = try reader.readEnum(GenerateWorker.ResultStatus);

    const kv_ = lookup.fetchRemove(source_path);
    defer {
        if (kv_) |*kv| kv.value.deinit();
    }

    if (status == .generate_failed) {
        try self.logger.log(.err, "{s} of `{s}`", .{
            message, source_path,
        });
    }
    else {
        try self.logger.log(.info, "{s} of `{s}` {s}", .{
            message,
            dest_name,
            if (status == .new_file) "✨" else "",
        });
    }
    try self.logger.log(.trace, "End generate from `{s}`", .{if (kv_) |kv| kv.value.name else "????"});
}

const State = union(events.EventPhase.Kind) {
    launching: BootPhaseState,
    request: void,
    ready: ReadyPhaseState,
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
