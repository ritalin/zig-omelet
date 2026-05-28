const std = @import("std");
const core = @import("core");

// TODO:
// const Connection = core.sockets.Connection.Client(app_context, GenerateWorker);
const EventDispatcher = core.sockets.EventDispatcher;
const Logger = core.Logger.withAppContext(app_context);
const ReceiveEntry = core.sockets.ReceiveEntry;

const GenerateWorker = @import("./GenerateWorker.zig");
const Setting = @import("./Setting.zig");
const CodeBuilder = @import("./CodeBuilder.zig");
const app_context = @import("build_options").app_context;

const GuestStage = @This();

allocator: std.mem.Allocator,
setting: *const Setting,
connection: *GuestStage.Connection,
dispatcher: EventDispatcher.Sized(1),
state: State,

pub const Connection = core.sockets.Connection.Client(app_context);

pub fn create(allocator: std.mem.Allocator, connection: *Connection, setting: *const Setting) !GuestStage {
    errdefer connection.deinit();

    try connection.subscribe(&.{
        .probe_launching,
        .ready_topic_body,
        .topic_body,
        .finish_topic_body,
        .quit_all,
    });
    try connection.connect();

    const options: EventDispatcher.Options = .{ 
        .force_concurrent = false, 
        .log_style = setting.log_style,
        .no_color = setting.no_color, 
    };
    const dispatcher = try connection.configureDispatcher(1, options);

    return .{
        .allocator = allocator,
        .setting = setting,
        .connection = connection,
        .dispatcher = dispatcher,
        .state = .{ .booting = BootPhaseState.init(3) },
        // .logger = Logger.init(allocator, connection.dispatcher, setting.standalone),
    };
}

pub fn deinit(self: *GuestStage) void {
    self.state.deinit();
    self.dispatcher.deinit();
}

pub fn run(self: *GuestStage) !void {
    self.dispatcher.run(app_context, GuestStage.onDispatch) catch |err| {
        // TODO:
        // try self.connection.dispatcher.postFatal(@errorReturnTrace());
        return err;
    };
}

fn onDispatch(dispatcher: *EventDispatcher.Sized(1), entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) anyerror!void {
    const self: *GuestStage = @alignCast(@fieldParentPtr("dispatcher", dispatcher));

    switch (self.state) {
        .booting => |state| {
            try state.handle(self, entry, dirty);
        },
        .ready => |state| try state.handle(self, entry, dirty),
        else => {
            // TODO:
            // Invalid phase
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
    //         .quit => {
    //             if (lookup.count() == 0) {
    //                 try self.connection.dispatcher.quitAccept();
    //             }
    //         },
    //         .quit_all => {
    //             try self.connection.dispatcher.quitAccept();
    //             try self.connection.pull_sink_socket.stop();
    //         },
    //         .log => |log| {
    //             try self.logger.log(log.level, "{s}", .{log.content});
    //         },
    //         else => {
    //             try self.logger.log(.warn, "Discard command: {}", .{std.meta.activeTag(item.event)});
    //         },
    //     }
    // }
}

fn defaultHandler(self: *GuestStage, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
    switch (entry.event) {
        .quit_all => {
            self.dispatcher.phase = .quitting;
        },
        else => {
            dirty.* = .unhandled;
        }
    }
}

fn doReadyPhase(self: *GuestStage) void {
    self.state.deinit();
    self.state = .{ .ready = ReadyPhaseState.init() };
    self.dispatcher.phase = .ready;
}

fn doQuitPhase(self: *GuestStage) void {
    self.state.deinit();
    self.dispatcher.phase = .quitting;
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

const State = union(EventDispatcher.Phase) {
    booting: BootPhaseState,
    request: void,
    ready: ReadyPhaseState,
    terminating: void,
    quitting: void,

    const deinit = deinitState;
};

fn deinitState(self: *State) void {
    switch (self.*) {
        .booting => |*state| state.deinit(),
        .ready => |*state| state.deinit(),
        else => unreachable,
    }
}

const BootPhaseState = struct {
    retry_count: usize,

    const Self = @This();

    pub fn init(retry_count: usize) Self {
        return .{
            .retry_count = retry_count,
        };
    }

    pub fn deinit(_: *Self) void {}

    pub fn handle(self: *const Self, stage: *GuestStage, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
        switch (entry.event) {
            .launching => {
                try self.bootLog(stage);
            },
            .probe_launching => {
                handshake(stage.connection, self.retry_count) catch |err| {
                    if (err == error.LaunchFailed) {
                        var channel = try stage.connection.dataChannel();
                        try channel.encode(.failed_launching);
                        try stage.dispatcher.queue.post(channel);
                    }
                    return err;
                };
                stage.doReadyPhase();
            },
            else => {
                try stage.defaultHandler(entry, dirty);
            }
        }
    }

    pub fn bootLog(self: *const BootPhaseState, stage: *GuestStage) !void {
        _ = self;
        const ep = stage.setting.endpoints;

        try stage.dispatcher.log(.debug, app_context, "Beginning...", .{});

        dump_subscription: {
            var arena = std.heap.ArenaAllocator.init(stage.allocator);
            defer arena.deinit();

            try stage.dispatcher.log(.debug, app_context, "Subscriber filters: {s}", .{try stage.connection.listSubscriptions(arena.allocator())});
            break:dump_subscription;
        }
        dump_setting: {
            try stage.dispatcher.log(.debug, app_context, "CLI: Req/Rep Channel = {s}", .{ep.req_rep});
            try stage.dispatcher.log(.debug, app_context, "CLI: Pub/Sub Channel = {s}", .{ep.pub_sub});
            try stage.dispatcher.log(.debug, app_context, "CLI: Push/pull Channel = {s}", .{ep.push_pull});
            break :dump_setting;
        }
    }
};

fn handshake(conn: *Connection, retry_count: usize) !void {
    // TODO:
    // Retrying itself
    var i: usize = 0;
    while (i < retry_count) {
        var channel = try conn.requestChannel();
        defer channel.deinit();
        try channel.encode(.launched);

        return (channel.submit(conn.context.io)) catch { i += 1; };
    }

    return error.LunchFailed;
}

const ReadyPhaseState = struct {
    const Self = @This();

    pub fn init() Self {
        return .{};
    }

    pub fn deinit(self: *Self) void {
        _ = self;
    }

    pub fn handle(self: *const Self, stage: *GuestStage, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
        // TODO:
        // var lookup = std.StringHashMap(core.Event.Payload.SourcePath).init(self.allocator);
        // defer lookup.deinit();

        _ = self;

        switch (entry.event) {
            else => {
                try stage.defaultHandler(entry, dirty);
            }
        }
    }
};