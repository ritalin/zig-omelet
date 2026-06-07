const std = @import("std");
const core = @import("core");
const app_context = @import("build_options").app_context;

const events = core.events;

const EventDispatcher = core.sockets.EventDispatcher;
const Logger = core.Logger.withAppContext(app_context);
const ReceiveEntry = core.sockets.ReceiveEntry;
const EventPhase = core.events.EventPhase;

const BootPhaseState = core.guest_phases.BootPhaseState(GuestStage);
const ReadyWatchFileState = @import("./phases/ready_phase.zig").ReadyWatchFileState(GuestStage);

const Setting = @import("./Setting.zig");

io: std.Io,
allocator: std.mem.Allocator,
setting: *const Setting,
connection: *GuestStage.Connection,
dispatcher: EventDispatcher.Sized(1),
state: State,

const GuestStage = @This();

// TODO:
// const Connection = core.sockets.Connection.Client(app_context, GenerateWorker);
pub const Connection = core.sockets.Connection.Client(app_context);

pub fn create(io: std.Io, allocator: std.mem.Allocator, connection: *Connection, setting: *const Setting) !GuestStage {
    errdefer connection.deinit();

    try connection.subscribe(&.{
        .probe,
        .ready_source_path,
        .ready_progress,
    });
    try connection.connect();

    const options: EventDispatcher.Options = .{ 
        .log_style = setting.log_style,
        .no_color = setting.no_color, 
    };
    const dispatcher = try connection.configureDispatcher(1, options);

    return .{
        .io = io,
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

pub fn transitPhase(self: *GuestStage, phase_kind: EventPhase.Kind, phase_agree: EventPhase.Agreement) !void {
    const phase: EventPhase = .{ .kind = phase_kind, .agreement = phase_agree};
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
                try self.log(.debug, "Phase unmatched/phase: {s}, current-phase: {s}, ack: {s}", .{@tagName(phase), @tagName(self.dispatcher.phase.kind), @tagName(self.dispatcher.phase.agreement)});
                return;
            }
            if (self.dispatcher.phase.agreement == .confirmed) {
                try self.log(.debug, "Discard probe/phase: {s}", .{@tagName(phase)});
                return;
            }
            switch (phase) {
                .request => {
                    try self.dispatcher.queue.post(.finish_topic, try self.connection.dataChannel());
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

fn doRequestPhase(self: *GuestStage) !void {
    self.state.deinit();
}

fn doReadyPhase(self: *GuestStage) !void {
    self.state.deinit();
    self.state = .{ .ready = ReadyWatchFileState.create };
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
}

const State = union(EventPhase.Kind) {
    launching: BootPhaseState,
    request: void,
    ready: ReadyWatchFileState,
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

test "test stage" {
    std.testing.refAllDecls(@This());
}

pub const tests = struct {
    const test_support = core.test_support;
    const Connecion = core.sockets.Connection.Client("test");
    const Dispatcher = core.sockets.EventDispatcher.Sized(8);

    const PathMatcher = @import("./PathMatcher.zig").PathMatcher(u21);
    const IterateFileWorker = @import("./watch_worker.zig").FileIterateWorker;

    const toUnicodeString = @import("./PathMatcher.zig").toUnicodeString;

    test "post simple file path" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var tmp_dir = std.testing.tmpDir(.{});
        defer tmp_dir.cleanup();
        const tmp_dir_path = try tmp_dir.dir.realPathFileAlloc(io, ".", allocator);
        defer allocator.free(tmp_dir_path);

        const file = try tmp_dir.dir.createFile(io, "foo.sql", .{});
        defer file.close(io);
        fill: {
            var buffer: [16]u8 = undefined;
            var w = file.writer(io, &buffer);
            try w.interface.writeAll("X" ** 10000);
            try w.interface.flush();
            break:fill;
        }

        const file_path = try tmp_dir.dir.realPathFileAlloc(io, "foo.sql", allocator);
        defer allocator.free(file_path);

        const ep = try test_support.createEndpoint(tmp_dir.dir);
        defer test_support.releaseEndpoint(ep);

        var filter_builder: PathMatcher.Builder = .init;
        var filter = try filter_builder.build(allocator);
        defer filter.deinit();

        const setting: Setting = .{
            .endpoints = ep,
            .log_level = .debug,
            .log_style = .discard,
            .no_color = false,
            .sources = &.{
                .{ .category = .source, .dir_path = file_path },
            },
            .filter = filter,
            .default_dialect = "duckdb",
            .watch = false,
        };

        var conn = try Connection.create(io, allocator, ep);
        defer conn.deinit();

        var stage = try GuestStage.create(io, allocator, &conn, &setting);
        defer stage.deinit();

        try std.testing.expectEqual(0, stage.dispatcher.queue.send_queue.len);

        try IterateFileWorker(GuestStage).run(&stage);

        try std.testing.expectEqual(2, stage.dispatcher.queue.send_queue.len);

        channel: {
            var channel: ?core.sockets.SendChannel = stage.dispatcher.queue.send_queue.popFront();
            defer if (channel) |*c| c.deinit();

            try std.testing.expectEqual(conn.push_socket.pipe.item.id, channel.?.pipe_id);

            const packet = try events.EventPacket.decode(allocator, channel.?.msg.bytes());
            try std.testing.expectEqual(.source_path, std.meta.activeTag(packet.event));
            try std.testing.expectEqual(.source, packet.event.source_path.category);
            try std.testing.expectEqualStrings("foo", packet.event.source_path.name);
            try std.testing.expectEqualStrings(file_path, packet.event.source_path.path);
            try std.testing.expectEqualStrings(setting.default_dialect, packet.event.source_path.dialect);
            break:channel;
        }
        channel: {
            var channel: ?core.sockets.SendChannel = stage.dispatcher.queue.send_queue.popFront();
            defer if (channel) |*c| c.deinit();

            try std.testing.expectEqual(conn.push_socket.pipe.item.id, channel.?.pipe_id);
            const packet = try events.EventPacket.decode(allocator, channel.?.msg.bytes());
            try std.testing.expectEqual(.finish_source_path, packet.event);
            break:channel;
        }
    }

    test "post nested file path" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var tmp_dir = std.testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        const base_dir = try tmp_dir.dir.createDirPathOpen(io, "x/y/z", .{});
        defer base_dir.close(io);
        const base_dir_path = try tmp_dir.dir.realPathFileAlloc(io, "x", allocator);
        defer allocator.free(base_dir_path);

        const file1 = try base_dir.createFile(io, "foo.sql", .{});
        defer file1.close(io);
        const file1_path = try base_dir.realPathFileAlloc(io, "foo.sql", allocator);
        defer allocator.free(file1_path);

        const file2 = try base_dir.createFile(io, "foo-bar.sql", .{});
        defer file2.close(io);
        const file2_path = try base_dir.realPathFileAlloc(io, "foo-bar.sql", allocator);
        defer allocator.free(file2_path);

        const ep = try test_support.createEndpoint(tmp_dir.dir);
        defer test_support.releaseEndpoint(ep);

        var filter_builder: PathMatcher.Builder = .init;
        var filter = try filter_builder.build(allocator);
        defer filter.deinit();

        const setting: Setting = .{
            .endpoints = ep,
            .log_level = .debug,
            .log_style = .discard,
            .no_color = false,
            .sources = &.{
                .{ .category = .source, .dir_path = base_dir_path },
            },
            .filter = filter,
            .default_dialect = "duckdb",
            .watch = false,
        };

        var conn = try Connection.create(io, allocator, ep);
        defer conn.deinit();

        var stage = try GuestStage.create(io, allocator, &conn, &setting);
        defer stage.deinit();

        try std.testing.expectEqual(0, stage.dispatcher.queue.send_queue.len);

        try IterateFileWorker(GuestStage).run(&stage);

        try std.testing.expectEqual(3, stage.dispatcher.queue.send_queue.len);

        const expects: []const events.Event.Payload.SourcePath = &.{
            .{
                .category = .source,
                .name = "y/z/foo",
                .path = file1_path,
                .dialect = setting.default_dialect,
                .hash = "dummy",
                .item_count = 1,
            },
            .{
                .category = .source,
                .name = "y/z/foo-bar",
                .path = file2_path,
                .dialect = setting.default_dialect,
                .hash = "dummy",
                .item_count = 1,
            },
        };

        channel: {
            var channel: ?core.sockets.SendChannel = stage.dispatcher.queue.send_queue.popFront();
            defer if (channel) |*c| c.deinit();

            try std.testing.expectEqual(conn.push_socket.pipe.item.id, channel.?.pipe_id);

            const packet = try events.EventPacket.decode(allocator, channel.?.msg.bytes());
            try std.testing.expectEqual(.source_path, std.meta.activeTag(packet.event));
            const expect = if (std.mem.eql(u8, packet.event.source_path.name, expects[0].name)) expects[0] else expects[1];

            try std.testing.expectEqual(expect.category, packet.event.source_path.category);
            try std.testing.expectEqualStrings(expect.name, packet.event.source_path.name);
            try std.testing.expectEqualStrings(expect.path, packet.event.source_path.path);
            try std.testing.expectEqualStrings(expect.dialect, packet.event.source_path.dialect);
            break:channel;
        }
        channel: {
            var channel: ?core.sockets.SendChannel = stage.dispatcher.queue.send_queue.popFront();
            defer if (channel) |*c| c.deinit();

            const packet = try events.EventPacket.decode(allocator, channel.?.msg.bytes());
            try std.testing.expectEqual(.source_path, std.meta.activeTag(packet.event));
            const expect = if (std.mem.eql(u8, packet.event.source_path.name, expects[0].name)) expects[0] else expects[1];

            try std.testing.expectEqual(expect.category, packet.event.source_path.category);
            try std.testing.expectEqualStrings(expect.name, packet.event.source_path.name);
            try std.testing.expectEqualStrings(expect.path, packet.event.source_path.path);
            try std.testing.expectEqualStrings(expect.dialect, packet.event.source_path.dialect);
            break:channel;
        }
        channel: {
            var channel: ?core.sockets.SendChannel = stage.dispatcher.queue.send_queue.popFront();
            defer if (channel) |*c| c.deinit();

            try std.testing.expectEqual(conn.push_socket.pipe.item.id, channel.?.pipe_id);
            const packet = try events.EventPacket.decode(allocator, channel.?.msg.bytes());
            try std.testing.expectEqual(.finish_source_path, packet.event);
            break:channel;
        }
    }

    test "post nested file path with included pattern" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var tmp_dir = std.testing.tmpDir(.{});
        defer tmp_dir.cleanup();
        const tmp_dir_path = try tmp_dir.dir.realPathFileAlloc(io, ".", allocator);
        defer allocator.free(tmp_dir_path);

        const base_dir = try tmp_dir.dir.createDirPathOpen(io, "x/y/z", .{});
        defer base_dir.close(io);
        const base_dir_path = try tmp_dir.dir.realPathFileAlloc(io, "x", allocator);
        defer allocator.free(base_dir_path);

        const file1 = try base_dir.createFile(io, "foo.sql", .{});
        defer file1.close(io);
        const file1_path = try base_dir.realPathFileAlloc(io, "foo.sql", allocator);
        defer allocator.free(file1_path);

        const file2 = try base_dir.createFile(io, "foo-bar.sql", .{});
        defer file2.close(io);
        const file2_path = try base_dir.realPathFileAlloc(io, "foo-bar.sql", allocator);
        defer allocator.free(file2_path);

        const ep = try test_support.createEndpoint(tmp_dir.dir);
        defer test_support.releaseEndpoint(ep);

        var filter_builder: PathMatcher.Builder = .init;
        defer filter_builder.deinit(allocator);
        const filter_path = try toUnicodeString(allocator, "bar");
        defer allocator.free(filter_path);

        try filter_builder.addFilterDir(allocator, .include, filter_path);
        var filter = try filter_builder.build(allocator);
        defer filter.deinit();

        const setting: Setting = .{
            .endpoints = ep,
            .log_level = .debug,
            .log_style = .discard,
            .no_color = false,
            .sources = &.{
                .{ .category = .source, .dir_path = base_dir_path },
            },
            .filter = filter,
            .default_dialect = "duckdb",
            .watch = false,
        };

        var conn = try Connection.create(io, allocator, ep);
        defer conn.deinit();

        var stage = try GuestStage.create(io, allocator, &conn, &setting);
        defer stage.deinit();

        try std.testing.expectEqual(0, stage.dispatcher.queue.send_queue.len);

        try IterateFileWorker(GuestStage).run(&stage);

        try std.testing.expectEqual(2, stage.dispatcher.queue.send_queue.len);

        const expect: events.Event.Payload.SourcePath = .{
                .category = .source,
            .name = "y/z/foo-bar",
            .path = file2_path,
            .dialect = setting.default_dialect,
            .hash = "dummy",
            .item_count = 1,
        };

        channel: {
            var channel: ?core.sockets.SendChannel = stage.dispatcher.queue.send_queue.popFront();
            defer if (channel) |*c| c.deinit();

            const packet = try events.EventPacket.decode(allocator, channel.?.msg.bytes());
            try std.testing.expectEqual(.source_path, std.meta.activeTag(packet.event));
            try std.testing.expectEqual(expect.category, packet.event.source_path.category);
            try std.testing.expectEqualStrings(expect.name, packet.event.source_path.name);
            try std.testing.expectEqualStrings(expect.path, packet.event.source_path.path);
            try std.testing.expectEqualStrings(expect.dialect, packet.event.source_path.dialect);
            break:channel;
        }
        channel: {
            var channel: ?core.sockets.SendChannel = stage.dispatcher.queue.send_queue.popFront();
            defer if (channel) |*c| c.deinit();

            try std.testing.expectEqual(conn.push_socket.pipe.item.id, channel.?.pipe_id);
            const packet = try events.EventPacket.decode(allocator, channel.?.msg.bytes());
            try std.testing.expectEqual(.finish_source_path, packet.event);
            break:channel;
        }
    }

    test "post nested file path with excluded pattern" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var tmp_dir = std.testing.tmpDir(.{});
        defer tmp_dir.cleanup();
        const tmp_dir_path = try tmp_dir.dir.realPathFileAlloc(io, ".", allocator);
        defer allocator.free(tmp_dir_path);

        const base_dir = try tmp_dir.dir.createDirPathOpen(io, "x/y/z", .{});
        defer base_dir.close(io);
        const base_dir_path = try tmp_dir.dir.realPathFileAlloc(io, "x", allocator);
        defer allocator.free(base_dir_path);

        const file1 = try base_dir.createFile(io, "foo.sql", .{});
        defer file1.close(io);
        const file1_path = try base_dir.realPathFileAlloc(io, "foo.sql", allocator);
        defer allocator.free(file1_path);

        const file2 = try base_dir.createFile(io, "foo-bar.sql", .{});
        defer file2.close(io);
        const file2_path = try base_dir.realPathFileAlloc(io, "foo-bar.sql", allocator);
        defer allocator.free(file2_path);

        const ep = try test_support.createEndpoint(tmp_dir.dir);
        defer test_support.releaseEndpoint(ep);

        var filter_builder: PathMatcher.Builder = .init;
        defer filter_builder.deinit(allocator);
        const filter_path = try toUnicodeString(allocator, "bar");
        defer allocator.free(filter_path);

        try filter_builder.addFilterDir(allocator, .exclude, filter_path);
        var filter = try filter_builder.build(allocator);
        defer filter.deinit();

        const setting: Setting = .{
            .endpoints = ep,
            .log_level = .debug,
            .log_style = .discard,
            .no_color = false,
            .sources = &.{
                .{ .category = .source, .dir_path = base_dir_path },
            },
            .filter = filter,
            .default_dialect = "duckdb",
            .watch = false,
        };

        var conn = try Connection.create(io, allocator, ep);
        defer conn.deinit();

        var stage = try GuestStage.create(io, allocator, &conn, &setting);
        defer stage.deinit();

        try std.testing.expectEqual(0, stage.dispatcher.queue.send_queue.len);

        try IterateFileWorker(GuestStage).run(&stage);

        try std.testing.expectEqual(2, stage.dispatcher.queue.send_queue.len);

        const expect: events.Event.Payload.SourcePath = .{
                .category = .source,
            .name = "y/z/foo",
            .path = file1_path,
            .dialect = setting.default_dialect,
            .hash = "dummy",
            .item_count = 1,
        };

        channel: {
            var channel: ?core.sockets.SendChannel = stage.dispatcher.queue.send_queue.popFront();
            defer if (channel) |*c| c.deinit();

            const packet = try events.EventPacket.decode(allocator, channel.?.msg.bytes());
            try std.testing.expectEqual(.source_path, std.meta.activeTag(packet.event));
            try std.testing.expectEqual(expect.category, packet.event.source_path.category);
            try std.testing.expectEqualStrings(expect.name, packet.event.source_path.name);
            try std.testing.expectEqualStrings(expect.path, packet.event.source_path.path);
            try std.testing.expectEqualStrings(expect.dialect, packet.event.source_path.dialect);
            break:channel;
        }
        channel: {
            var channel: ?core.sockets.SendChannel = stage.dispatcher.queue.send_queue.popFront();
            defer if (channel) |*c| c.deinit();

            try std.testing.expectEqual(conn.push_socket.pipe.item.id, channel.?.pipe_id);
            const packet = try events.EventPacket.decode(allocator, channel.?.msg.bytes());
            try std.testing.expectEqual(.finish_source_path, packet.event);
            break:channel;
        }
    }

    test "post nested file path with dialect" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var tmp_dir = std.testing.tmpDir(.{});
        defer tmp_dir.cleanup();
        const tmp_dir_path = try tmp_dir.dir.realPathFileAlloc(io, ".", allocator);
        defer allocator.free(tmp_dir_path);

        const file = try tmp_dir.dir.createFile(io, "foo.sqlite.sql", .{});
        defer file.close(io);
        fill: {
            var buffer: [16]u8 = undefined;
            var w = file.writer(io, &buffer);
            try w.interface.writeAll("X" ** 10000);
            try w.interface.flush();
            break:fill;
        }

        const file_path = try tmp_dir.dir.realPathFileAlloc(io, "foo.sqlite.sql", allocator);
        defer allocator.free(file_path);

        const ep = try test_support.createEndpoint(tmp_dir.dir);
        defer test_support.releaseEndpoint(ep);

        var filter_builder: PathMatcher.Builder = .init;
        var filter = try filter_builder.build(allocator);
        defer filter.deinit();

        const setting: Setting = .{
            .endpoints = ep,
            .log_level = .debug,
            .log_style = .discard,
            .no_color = false,
            .sources = &.{
                .{ .category = .source, .dir_path = file_path },
            },
            .filter = filter,
            .default_dialect = "duckdb",
            .watch = false,
        };

        var conn = try Connection.create(io, allocator, ep);
        defer conn.deinit();

        var stage = try GuestStage.create(io, allocator, &conn, &setting);
        defer stage.deinit();

        try std.testing.expectEqual(0, stage.dispatcher.queue.send_queue.len);

        try IterateFileWorker(GuestStage).run(&stage);

        try std.testing.expectEqual(2, stage.dispatcher.queue.send_queue.len);

        const expect: events.Event.Payload.SourcePath = .{
                .category = .source,
            .name = "foo",
            .path = file_path,
            .dialect = "sqlite",
            .hash = "dummy",
            .item_count = 1,
        };
        
        channel: {
            var channel: ?core.sockets.SendChannel = stage.dispatcher.queue.send_queue.popFront();
            defer if (channel) |*c| c.deinit();

            try std.testing.expectEqual(conn.push_socket.pipe.item.id, channel.?.pipe_id);

            const packet = try events.EventPacket.decode(allocator, channel.?.msg.bytes());
            try std.testing.expectEqual(.source_path, std.meta.activeTag(packet.event));
            try std.testing.expectEqual(expect.category, packet.event.source_path.category);
            try std.testing.expectEqualStrings(expect.name, packet.event.source_path.name);
            try std.testing.expectEqualStrings(expect.path, packet.event.source_path.path);
            try std.testing.expectEqualStrings(expect.dialect, packet.event.source_path.dialect);
            break:channel;
        }
        channel: {
            var channel: ?core.sockets.SendChannel = stage.dispatcher.queue.send_queue.popFront();
            defer if (channel) |*c| c.deinit();

            try std.testing.expectEqual(conn.push_socket.pipe.item.id, channel.?.pipe_id);
            const packet = try events.EventPacket.decode(allocator, channel.?.msg.bytes());
            try std.testing.expectEqual(.finish_source_path, packet.event);
            break:channel;
        }
    }
};