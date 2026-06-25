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
        left_guests: std.BufSet,

        const Self = @This();

        pub fn create(allocator: std.mem.Allocator, guests: *std.BufSet) !Self {
            return .{
                .left_guests = try guests.cloneWithAllocator(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            self.left_guests.deinit();
        }

        pub fn handle(self: *Self, stage: *HostRunner, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
            switch (entry.event) {
                .launching => {
                    const ep = stage.setting.base.endpoints;

                    try stage.log(.debug, "Launched", .{});

                    dump_setting: {
                        try stage.log(.debug, "CLI: Req/Rep Channel = {s}", .{ep.req_rep});
                        try stage.log(.debug, "CLI: Pub/Sub Channel = {s}", .{ep.pub_sub});
                        try stage.log(.debug, "CLI: Push/Pull Channel = {s}", .{ep.push_pull});
                        //  TODO:
                        // stage.dispatcher.log(.debug, app_context, "CLI: Watch mode = {}", .{stage.setting.command.watchModeEnabled()});
                        break :dump_setting;
                    }

                    try stage.sendProbeHeartbeat(.probe, .launching, 1);
                },
                .failed_launching => {
                    try stage.log(.warn, "Launching failed/guest: {s}", .{entry.from_stage});
                    try stage.log(.warn, "Stopping launch process", .{});
                    try stage.transitPhase(.terminating, .pending);
                },
                .launched => {
                    if (! self.left_guests.contains(entry.from_stage)) return;

                    self.left_guests.remove(entry.from_stage);

                    try stage.log(.info, "Guest accepted/name: {s} (left: {})", .{entry.from_stage, self.left_guests.count()});

                    if (self.left_guests.count() == 0) {
                        try stage.log(.info, "All guests launched", .{});
                        try stage.transitPhase(.request, .pending);
                    }
                },
                .heartbeat => |payload| {
                    if (self.left_guests.count() > 0) {
                        stage.sendProbeHeartbeat(payload.event_type, .launching, payload.count) catch |err| switch (err) {
                            error.DiscardProbe => {
                                dirty.* = .unhandled;
                            },
                            else => return err,
                        };
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
            try self.state.handle(stage, entry, dirty);
        }
    };

    test "booting" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var tmpDir: std.testing.TmpDir = try test_support.createTmpDir();
        defer tmpDir.cleanup();
        const host_ep: types.Endpoints = try test_support.createEndpoint(tmpDir, .{});
        defer test_support.releaseEndpoint(host_ep);

        const guest_ep1: types.Endpoints = try test_support.createEndpoint(tmpDir, .{.worker_endpoint = "inproc://guest-worker1"});
        defer test_support.releaseEndpoint(guest_ep1);

        const guest_ep2: types.Endpoints = try test_support.createEndpoint(tmpDir, .{.worker_endpoint = "inproc://guest-worker2"});
        defer test_support.releaseEndpoint(guest_ep2);
        
        defer test_support.cleanup();

        const guest_names: []const types.Symbol = &.{ "guest-a", "guest-b" };
        var guests = std.BufSet.init(allocator);
        defer guests.deinit();
        for (guest_names) |name| { try guests.insert(name); }

        var connection = try Connection.create(io, allocator , guests.count(), host_ep);
        defer connection.deinit();
        try connection.bind();

        var runner: PhaseTestHarness = .{
            .stage = try TestStage.init(&connection, host_ep, .unlimited),
            .state = try BootPhaseState(TestStage).create(allocator, &guests),
        };
        defer runner.stage.deinit();
        defer runner.state.deinit();

        var client1 = try Client("guest-a").create(io, allocator , guest_ep1);
        defer client1.deinit();
        try client1.connect();

        var client2 = try Client("guest-b").create(io, allocator , guest_ep2);
        defer client2.deinit();
        try client2.connect();

        var tasks: std.Io.Group = .init;
        try tasks.concurrent(io, test_support.sendMessage, .{ try client1.requestChannel(), .launched });
        try tasks.concurrent(io, test_support.sendMessage, .{ try client2.requestChannel(), .launched });
        try tasks.concurrent(io, PhaseTestHarness.run, .{ &runner });
        try tasks.await(io);

        try std.testing.expectEqual(null, runner.stage.err);
        try std.testing.expectEqual(.quitting, runner.stage.dispatcher.phase.kind);
        try std.testing.expectEqual(0, runner.state.left_guests.count());
    }

    test "boot no response" {
        var tmpDir: std.testing.TmpDir = try test_support.createTmpDir();
        defer tmpDir.cleanup();
        const host_ep: types.Endpoints = try test_support.createEndpoint(tmpDir, .{});
        defer test_support.releaseEndpoint(host_ep);
        defer test_support.cleanup();

        const io = std.testing.io;
        const allocator = std.testing.allocator;

        const guest_names: []const types.Symbol = &.{ "guest-a", "guest-b" };
        var guests = std.BufSet.init(allocator);
        defer guests.deinit();
        for (guest_names) |name| { try guests.insert(name); }

        var connection = try Connection.create(io, allocator , guests.count(), host_ep);
        defer connection.deinit();
        try connection.bind();

        var runner: PhaseTestHarness = .{
            .stage = try TestStage.init(&connection, host_ep, .{ .count = 1 }),
            .state = try BootPhaseState(TestStage).create(allocator, &guests),
        };
        defer runner.stage.deinit();
        defer runner.state.deinit();

        var tasks: std.Io.Group = .init;
        try tasks.concurrent(io, PhaseTestHarness.run, .{ &runner });
        try tasks.await(io);

        try std.testing.expectEqual(error.Timeout, runner.stage.err.?);
        try std.testing.expectEqual(.launching, runner.stage.dispatcher.phase.kind);
        try std.testing.expectEqual(2, runner.state.left_guests.count());
    }

    test "guest launch failed" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var tmpDir: std.testing.TmpDir = try test_support.createTmpDir();
        defer tmpDir.cleanup();
        const host_ep: types.Endpoints = try test_support.createEndpoint(tmpDir, .{});
        defer test_support.releaseEndpoint(host_ep);

        const guest_ep: types.Endpoints = .{
            .req_rep = try allocator.dupe(u8, host_ep.req_rep),
            .pub_sub = try allocator.dupe(u8, host_ep.pub_sub),
            .push_pull = try allocator.dupe(u8, host_ep.push_pull),
            .worker = "inproc://guest-worker1"
        };
        defer test_support.releaseEndpoint(guest_ep);
        
        defer test_support.cleanup();

        const guest_names: []const types.Symbol = &.{ "guest-a", "guest-b" };
        var guests = std.BufSet.init(allocator);
        defer guests.deinit();
        for (guest_names) |name| { try guests.insert(name); }

        var connection = try Connection.create(io, allocator , guests.count(), host_ep);
        defer connection.deinit();
        try connection.bind();

        var client1 = try Client("guest-a").create(io, allocator , guest_ep);
        defer client1.deinit();
        try client1.connect();

        var runner: PhaseTestHarness = .{
            .stage = try TestStage.init(&connection, host_ep, .{ .count = 1 }),
            .state = try BootPhaseState(TestStage).create(allocator, &guests),
        };
        defer runner.stage.deinit();
        defer runner.state.deinit();

        var tasks: std.Io.Group = .init;
        try tasks.concurrent(io, test_support.sendMessage, .{ try client1.requestChannel(), .failed_launching });
        try tasks.concurrent(io, PhaseTestHarness.run, .{ &runner });
        try tasks.await(io);

        try std.testing.expectEqual(null, runner.stage.err);
        try std.testing.expectEqual(.quitting, runner.stage.dispatcher.phase.kind);
        try std.testing.expectEqual(2, runner.state.left_guests.count());
    }
};
