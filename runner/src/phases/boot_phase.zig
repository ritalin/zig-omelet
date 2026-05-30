const std = @import("std");
const core = @import("core");

const types = core.types;
const events = core.events;

const EventDispatcher = core.sockets.EventDispatcher;
const ReceiveEntry = core.sockets.ReceiveEntry;

const Setting = @import("../settings/Setting.zig");
const HeartbeatTask = @import("../tasks/HeartbeatTask.zig");

pub fn BootPhaseState(comptime HostRunner: type) type {
    return struct {
        guests: std.BufSet,
        left_guest: std.BufSet,
        limit: HeartbeatTask.Limit,

        const Self = @This();

        pub fn create(allocator: std.mem.Allocator, guest_names: []const types.StageName, heartbeat_limit: HeartbeatTask.Limit) !Self {
            var guests = std.BufSet.init(allocator);
            for (guest_names) |name| {
                try guests.insert(name);
            }

            return .{
                .guests = guests,
                .left_guest = try guests.cloneWithAllocator(allocator),
                .limit = heartbeat_limit,
            };
        }

        pub fn deinit(self: *Self) void {
            self.guests.deinit();
            self.left_guest.deinit();
        }

        pub fn handle(self: *Self, stage: *HostRunner, setting: *const Setting, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
            switch (entry.event) {
                .launching => {
                    const ep = setting.general.stage_endpoints;

                    try stage.log(.debug, "Launched", .{});

                    dump_setting: {
                        try stage.log(.debug, "CLI: Req/Rep Channel = {s}", .{ep.req_rep});
                        try stage.log(.debug, "CLI: Pub/Sub Channel = {s}", .{ep.pub_sub});
                        try stage.log(.debug, "CLI: Push/Pull Channel = {s}", .{ep.push_pull});
                        //  TODO:
                        // stage.dispatcher.log(.debug, app_context, "CLI: Watch mode = {}", .{stage.setting.command.watchModeEnabled()});
                        break :dump_setting;
                    }

                    try stage.sendProbe(.probe_launching, 1, self.limit);
                },
                .failed_launching => {
                    if (self.guests.contains(entry.from_stage)) {
                        try stage.log(.warn, "Launching failed/guest: {s}", .{entry.from_stage});
                        try stage.log(.warn, "Stopping launch process", .{});
                        try stage.transitPhase(.terminating);
                    }
                },
                .launched => {
                    if (! self.guests.contains(entry.from_stage)) {
                        try stage.log(.warn, "Unexpected boot/guest: {s}", .{entry.from_stage});
                        return;
                    }

                    self.left_guest.remove(entry.from_stage);

                    try stage.log(.info, "Launch accepted/guest: {s}", .{entry.from_stage});

                    if (self.left_guest.count() == 0) {
                        try stage.log(.info, "All guests launched", .{});
                        try stage.transitPhase(.request);
                    }
                },
                .heartbeat => |payload| {
                    if (self.left_guest.count() > 0) {
                        switch (payload.event_type) {
                            .probe_launching => {
                                try stage.sendProbe(.probe_launching, payload.count, self.limit);
                            },
                            else => {
                                try stage.defaultHandler(entry, dirty);
                            }
                        }
                    }
                },
                else => {
                    try stage.defaultHandler(entry, dirty);
                }
            }
        }
    };
}

test "boot phase" {
    std.testing.refAllDecls(@This());
}

pub const tests = struct {
    const task_support = @import("../supports/task_support.zig");
    const test_support = @import("../supports/test_support.zig");

    const Client = core.sockets.Connection.Client;
    const Connection = test_support.Connection;
    const TestStage = test_support.TestStage;
    
    const PhaseTestHarness = struct {
        stage: TestStage,
        state: BootPhaseState(TestStage),

        pub fn run(self: *PhaseTestHarness) !void {
            self.stage.run(PhaseTestHarness.onDispatch);
        }

        fn onDispatch(dispatcher: *EventDispatcher.Sized(test_support.POLLER_SIZE), entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) anyerror!void {
            const stage: *TestStage = @alignCast(@fieldParentPtr("dispatcher", dispatcher));
            const self: *PhaseTestHarness = @alignCast(@fieldParentPtr("stage", stage));
            try self.state.handle(stage, &stage.setting, entry, dirty);
        }
    };

    test "booting" {
        const tmpDIr: std.testing.TmpDir = try test_support.createTmpDir();
        const ep: types.Endpoints = try test_support.createEndpoint(tmpDIr.dir);
        defer test_support.releaseEndpoint(ep);
        defer test_support.cleanup();

        const io = std.testing.io;
        const allocator = std.testing.allocator;

        const guest_names = &.{ "guest-a", "guest-b" };
        var connection = try Connection.create(io, allocator , guest_names.len, ep);
        defer connection.deinit();
        try connection.bind();

        var runner: PhaseTestHarness = .{
            .stage = try TestStage.init(&connection, ep, .unlimited),
            .state = try BootPhaseState(TestStage).create(allocator, guest_names, .unlimited),
        };
        defer runner.stage.deinit();
        defer runner.state.deinit();

        runner.stage.dispatcher.phase = .booting;

        var client1 = try Client("guest-a").create(io, allocator , ep);
        defer client1.deinit();
        try client1.connect();

        var client2 = try Client("guest-b").create(io, allocator , ep);
        defer client2.deinit();
        try client2.connect();

        var tasks: std.Io.Group = .init;
        try tasks.concurrent(io, test_support.sendMessage, .{ try client1.requestChannel(), .launched });
        try tasks.concurrent(io, test_support.sendMessage, .{ try client2.requestChannel(), .launched });
        try tasks.concurrent(io, PhaseTestHarness.run, .{ &runner });
        try tasks.await(io);

        try std.testing.expectEqual(null, runner.stage.err);
        try std.testing.expectEqual(.quitting, runner.stage.dispatcher.phase);
        try std.testing.expectEqual(0, runner.state.left_guest.count());
    }

    test "boot no response" {
        const tmpDIr: std.testing.TmpDir = try test_support.createTmpDir();
        const ep: types.Endpoints = try test_support.createEndpoint(tmpDIr.dir);
        defer test_support.releaseEndpoint(ep);
        defer test_support.cleanup();

        const io = std.testing.io;
        const allocator = std.testing.allocator;

        const guest_names = &.{ "guest-a", "guest-b" };
        var connection = try Connection.create(io, allocator , guest_names.len, ep);
        defer connection.deinit();
        try connection.bind();

        var runner: PhaseTestHarness = .{
            .stage = try TestStage.init(&connection, ep, .{ .count = 1 }),
            .state = try BootPhaseState(TestStage).create(allocator, guest_names, .{ .count = 1 }),
        };
        defer runner.stage.deinit();
        defer runner.state.deinit();

        runner.stage.dispatcher.phase = .booting;

        var tasks: std.Io.Group = .init;
        try tasks.concurrent(io, PhaseTestHarness.run, .{ &runner });
        try tasks.await(io);

        try std.testing.expectEqual(error.Timeout, runner.stage.err.?);
        try std.testing.expectEqual(.booting, runner.stage.dispatcher.phase);
        try std.testing.expectEqual(2, runner.state.left_guest.count());
    }

    test "guest launch failed" {
        const tmpDIr: std.testing.TmpDir = try test_support.createTmpDir();
        const ep: types.Endpoints = try test_support.createEndpoint(tmpDIr.dir);
        defer test_support.releaseEndpoint(ep);
        defer test_support.cleanup();

        const io = std.testing.io;
        const allocator = std.testing.allocator;

        const guest_names = &.{ "guest-a", "guest-b" };
        var connection = try Connection.create(io, allocator , guest_names.len, ep);
        defer connection.deinit();
        try connection.bind();

        var client1 = try Client("guest-a").create(io, allocator , ep);
        defer client1.deinit();
        try client1.connect();

        var runner: PhaseTestHarness = .{
            .stage = try TestStage.init(&connection, ep, .{ .count = 1 }),
            .state = try BootPhaseState(TestStage).create(allocator, guest_names, .{ .count = 1 }),
        };
        defer runner.stage.deinit();
        defer runner.state.deinit();

        runner.stage.dispatcher.phase = .booting;

        var tasks: std.Io.Group = .init;
        try tasks.concurrent(io, test_support.sendMessage, .{ try client1.requestChannel(), .failed_launching });
        try tasks.concurrent(io, PhaseTestHarness.run, .{ &runner });
        try tasks.await(io);

        try std.testing.expectEqual(null, runner.stage.err);
        try std.testing.expectEqual(.quitting, runner.stage.dispatcher.phase);
        try std.testing.expectEqual(2, runner.state.left_guest.count());
    }
};
