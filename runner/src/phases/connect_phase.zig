const std = @import("std");
const core = @import("core");

const types = core.types;
const events = core.events;

const EventDispatcher = core.sockets.EventDispatcher;
const ReceiveEntry = core.sockets.ReceiveEntry;

const Setting = @import("../settings/Setting.zig");
const HeartbeatTask = @import("../tasks/HeartbeatTask.zig");

pub fn ConnectPhaseState(comptime HostRunner: type) type {
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
                        stage.sendProbeHeartbeat(.connecting, payload.count) catch |err| switch (err) {
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

test "connect phase" {
    std.testing.refAllDecls(@This());
}

pub const tests = struct {
    const task_support = @import("../supports/task_support.zig");
    const test_support = @import("../supports/test_support.zig");

    const ClientConnection = core.sockets.Connection.Client;
    const ServerConnection = test_support.Connection;

    // const TestStage = test_support.TestStage;
    const Config = @import("../configs/Config.zig");
    const TestStage = @import("../Runner.zig");
    const GuestTestStage = test_support.GuestStage;

    const PhaseTestHarness = struct {
        host_stage: TestStage,
        guest_stages: []const *GuestTestStage,

        pub fn deinit(self: *PhaseTestHarness, allocator: std.mem.Allocator) void {
            defer self.host_stage.deinit();

            for (self.guest_stages) |guest| {
                defer guest.deinit(allocator);
            }
        }
    };

    test "booting" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var tmpDir: std.testing.TmpDir = try test_support.createTmpDir();
        defer tmpDir.cleanup();
        var host_ep_config: core.configs.Endpoint.Config = try test_support.testEndpointConfig(io, &tmpDir, .{});
        var host_ep = try core.configs.Endpoint.runtimeIpc(allocator, host_ep_config);
        defer test_support.releaseEndpoint(io, &host_ep, &host_ep_config);

        var guest_ep_config1: core.configs.Endpoint.Config = try test_support.testEndpointConfig(io, &tmpDir, .{.worker_endpoint = "inproc://guest-worker1"});
        var guest_ep1 = try core.configs.Endpoint.runtimeIpc(allocator, guest_ep_config1);
        defer test_support.releaseEndpoint(io, &guest_ep1, &guest_ep_config1);

        var guest_ep_confg2: core.configs.Endpoint.Config = try test_support.testEndpointConfig(io, &tmpDir, .{.worker_endpoint = "inproc://guest-worker2"});
        var guest_ep2 = try core.configs.Endpoint.runtimeIpc(allocator, guest_ep_confg2);
        defer test_support.releaseEndpoint(io, &guest_ep2, &guest_ep_confg2);
        
        defer test_support.cleanup();

        var setting: Setting = test_support.initSetting(host_ep);
        var config: Config = try test_support.initConfig(allocator, &.{ "guest-a", "guest-b" }, &.{.init, .init});
        defer config.deinit(allocator);

        var connection = try TestStage.Connection.create(io, allocator , config.guests.len, host_ep);
        defer connection.deinit();

        var harness: PhaseTestHarness = .{
            .host_stage = try TestStage.create(io, allocator, &connection, &config, &setting),
            .guest_stages = &.{
                try GuestTestStage.init(io, allocator, "guest-a", guest_ep1),
                try GuestTestStage.init(io, allocator, "guest-b", guest_ep2),
            },
        };
        defer harness.deinit(allocator);

        host_launch: {
            try harness.host_stage.transitPhase(.connecting, .pending);
            break:host_launch;
        }
        guest_iter: {
            try harness.guest_stages[0].request(.launched);
            try harness.guest_stages[0].iteration(.{.no_poll = true});
            break:guest_iter;
        }
        host_iter: {
            try std.testing.expectEqual(.connecting, harness.host_stage.dispatcher.phase.kind);
            try std.testing.expectEqual(.connecting, std.meta.activeTag(harness.host_stage.state));
            try std.testing.expectEqual(2, harness.host_stage.state.connecting.left_guests.count());
            iter: {
                try harness.host_stage.iteration(.{.handle_all = true, .prev_status = .pending_poll});
                try std.testing.expectEqual(.connecting, harness.host_stage.dispatcher.phase.kind);
                try std.testing.expectEqual(.connecting, std.meta.activeTag(harness.host_stage.state));
                try std.testing.expectEqual(1, harness.host_stage.state.connecting.left_guests.count());
                break:iter;
            }
            break:host_iter;
        }
        guest_iter: {
            try harness.guest_stages[1].request(.launched);
            try harness.guest_stages[1].iteration(.{.no_poll = true});
            break:guest_iter;
        }
        host_iter: {
            try std.testing.expectEqual(.connecting, harness.host_stage.dispatcher.phase.kind);
            iter: {
                try harness.host_stage.iteration(.{.handle_all = true, .prev_status = .pending_poll});
                try std.testing.expectEqual(.request, harness.host_stage.dispatcher.phase.kind);
                try std.testing.expectEqual(.request, std.meta.activeTag(harness.host_stage.state));
                break:iter;
            }
            break:host_iter;
        }
    }

    test "guest launch failed" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var tmpDir: std.testing.TmpDir = try test_support.createTmpDir();
        defer tmpDir.cleanup();

        var host_ep_config: core.configs.Endpoint.Config = try test_support.testEndpointConfig(io, &tmpDir, .{});
        var host_ep = try core.configs.Endpoint.runtimeIpc(allocator, host_ep_config);
        defer test_support.releaseEndpoint(io, &host_ep, &host_ep_config);

        var guest_ep_config: core.configs.Endpoint.Config = try test_support.testEndpointConfig(io, &tmpDir, .{.worker_endpoint = "inproc://guest-worker1"});
        var guest_ep = try core.configs.Endpoint.runtimeIpc(allocator, guest_ep_config);
        defer test_support.releaseEndpoint(io, &guest_ep, &guest_ep_config);
        
        defer test_support.cleanup();

        var setting: Setting = test_support.initSetting(host_ep);
        var config: Config = try test_support.initConfig(allocator, &.{ "guest-a" }, &.{.init});
        defer config.deinit(allocator);

        var connection = try TestStage.Connection.create(io, allocator , config.guests.len, host_ep);
        defer connection.deinit();

        var harness: PhaseTestHarness = .{
            .host_stage = try TestStage.create(io, allocator, &connection, &config, &setting),
            .guest_stages = &.{
                try GuestTestStage.init(io, allocator, "guest-a", guest_ep),
            },
        };
        defer harness.deinit(allocator);

        host_launch: {
            try harness.host_stage.transitPhase(.connecting, .pending);
            break:host_launch;
        }
        guest_iter: {
            try harness.guest_stages[0].request(.failed_launching);
            try harness.guest_stages[0].iteration(.{.no_poll = true});
            break:guest_iter;
        }
        host_iter: {
            try std.testing.expectEqual(.connecting, harness.host_stage.dispatcher.phase.kind);
            try std.testing.expectEqual(.connecting, std.meta.activeTag(harness.host_stage.state));
            try std.testing.expectEqual(1, harness.host_stage.state.connecting.left_guests.count());
            iter: {
                try harness.host_stage.iteration(.{.handle_all = true, .prev_status = .pending_poll});
                try std.testing.expectEqual(.terminating, harness.host_stage.dispatcher.phase.kind);
                try std.testing.expectEqual(.terminating, std.meta.activeTag(harness.host_stage.state));
                break:iter;
            }
            break:host_iter;
        }
    }
};
