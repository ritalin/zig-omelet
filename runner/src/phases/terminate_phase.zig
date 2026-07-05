const std = @import("std");
const core = @import("core");

const types = core.types;

const EventDispatcher = core.sockets.EventDispatcher;
const ReceiveEntry = core.sockets.ReceiveEntry;

const HeartbeatTask = @import("../tasks/HeartbeatTask.zig");

pub fn TerminatePhaseState(comptime HostRunner: type) type {
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
                .quit => {
                    if (! self.left_guests.contains(entry.from_stage)) return;
                    self.left_guests.remove(entry.from_stage);

                    try stage.log(.info, "Shutdown accepted/guest: {s}", .{entry.from_stage});

                    if (self.left_guests.count() == 0) {
                        try stage.log(.info, "All guests Exited", .{});
                        try stage.transitPhase(.quitting, .confirmed);
                    }
                },
                .heartbeat => |payload| {
                    if (self.left_guests.count() > 0) {
                        stage.sendProbeHeartbeat(.terminating, payload.count) catch |err| switch (err) {
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

test "terminate phase" {
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
        state: TerminatePhaseState(TestStage),

        pub fn run(self: *PhaseTestHarness) !void {
            self.stage.run(PhaseTestHarness.doDispatch);
        }

        fn doDispatch(dispatcher: *EventDispatcher.Sized(test_support.POLLER_SIZE), entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) anyerror!void {
            const stage: *TestStage = @alignCast(@fieldParentPtr("dispatcher", dispatcher));
            const self: *PhaseTestHarness = @alignCast(@fieldParentPtr("stage", stage));
            try self.state.handle(stage, entry, dirty);
        }

        fn doDefault(stage: *TestStage, entry: ReceiveEntry) !void {
            if (entry.event == .launching) {
                try stage.sendProbeHeartbeat(.terminating, 1);
            }
        }
    };
    
    test "terminating" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        const tmpDir: std.testing.TmpDir = try test_support.createTmpDir();
        var host_ep_config: core.configs.Endpoint.Config = try test_support.testEndpointConfig(io, &tmpDir, .{});
        var host_ep = try core.configs.Endpoint.runtimeIpc(allocator, host_ep_config);
        defer test_support.releaseEndpoint(io, &host_ep, &host_ep_config);

        var guest_ep_config1: core.configs.Endpoint.Config = try test_support.testEndpointConfig(io, &tmpDir, .{.worker_endpoint = "inproc://guest-worker1"});
        var guest_ep1 = try core.configs.Endpoint.runtimeIpc(allocator, guest_ep_config1);
        defer test_support.releaseEndpoint(io, &guest_ep1, &guest_ep_config1);

        var guest_ep_config2: core.configs.Endpoint.Config = try test_support.testEndpointConfig(io, &tmpDir, .{.worker_endpoint = "inproc://guest-worker2"});
        var guest_ep2 = try core.configs.Endpoint.runtimeIpc(allocator, guest_ep_config2);
        defer test_support.releaseEndpoint(io, &guest_ep2, &guest_ep_config2);

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
            .state = try TerminatePhaseState(TestStage).create(allocator, &guests),
        };
        defer runner.state.deinit();
        defer runner.stage.deinit();
        runner.stage.on_default = PhaseTestHarness.doDefault;

        var client1 = try Client("guest-a").create(io, allocator , guest_ep1);
        defer client1.deinit();
        try client1.connect();

        var client2 = try Client("guest-b").create(io, allocator , guest_ep2);
        defer client2.deinit();
        try client2.connect();

        var tasks: std.Io.Group = .init;
        try tasks.concurrent(io, test_support.sendMessage, .{ try client1.requestChannel(), .quit });
        try tasks.concurrent(io, test_support.sendMessage, .{ try client2.requestChannel(), .quit });
        try tasks.concurrent(io, PhaseTestHarness.run, .{ &runner });
        try tasks.await(io);

        try std.testing.expectEqual(null, runner.stage.err);
        try std.testing.expectEqual(.quit_done, runner.stage.dispatcher.phase.kind);
        try std.testing.expectEqual(0, runner.state.left_guests.count());
    }
};