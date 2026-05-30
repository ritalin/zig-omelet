const std = @import("std");
const core = @import("core");

const types = core.types;

const EventDispatcher = core.sockets.EventDispatcher;
const ReceiveEntry = core.sockets.ReceiveEntry;

const HeartbeatTask = @import("../tasks/HeartbeatTask.zig");

pub fn TerminatePhaseState(comptime HostRunner: type) type {
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

        pub fn handle(self: *Self, stage: *HostRunner, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
            switch (entry.event) {
                .quit => {
                    if (! self.guests.contains(entry.from_stage)) {
                        try stage.log(.warn, "Unexpected shut down/guest: {s}", .{entry.from_stage});
                        return;
                    }

                    self.left_guest.remove(entry.from_stage);

                    try stage.log(.info, "Shutdown accepted/guest: {s}", .{entry.from_stage});

                    if (self.left_guest.count() == 0) {
                        try stage.log(.info, "All guests Exited", .{});
                        try stage.transitPhase(.quitting);
                    }
                },
                .heartbeat => |payload| {
                    if (self.left_guest.count() > 0) {
                        switch (payload.event_type) {
                            .quit_all => {
                                try stage.sendProbe(.quit_all, payload.count, self.limit);
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
            const self: *PhaseTestHarness = @alignCast(@fieldParentPtr("stage", stage));
            if (entry.event == .launching) {
                try stage.sendProbe(.quit_all, 1, self.state.limit);
            }
        }
    };
    
    test "terminating" {
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
            .state = try TerminatePhaseState(TestStage).create(allocator, guest_names, .unlimited),
        };
        defer runner.state.deinit();
        defer runner.stage.deinit();
        runner.stage.on_default = PhaseTestHarness.doDefault;

        var client1 = try Client("guest-a").create(io, allocator , ep);
        defer client1.deinit();
        try client1.connect();

        var client2 = try Client("guest-b").create(io, allocator , ep);
        defer client2.deinit();
        try client2.connect();

        var tasks: std.Io.Group = .init;
        try tasks.concurrent(io, test_support.sendMessage, .{ try client1.requestChannel(), .quit });
        try tasks.concurrent(io, test_support.sendMessage, .{ try client2.requestChannel(), .quit });
        try tasks.concurrent(io, PhaseTestHarness.run, .{ &runner });
        try tasks.await(io);

        try std.testing.expectEqual(null, runner.stage.err);
        try std.testing.expectEqual(.quitting, runner.stage.dispatcher.phase);
        try std.testing.expectEqual(0, runner.state.left_guest.count());
    }
};