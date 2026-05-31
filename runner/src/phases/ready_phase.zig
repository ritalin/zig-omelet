const std = @import("std");
const core = @import("core");

const types = core.types;

const EventDispatcher = core.sockets.EventDispatcher;
const ReceiveEntry = core.sockets.ReceiveEntry;

const HeartbeatTask = @import("../tasks/HeartbeatTask.zig");
const CacheManager = @import("../cache_manager.zig").CacheManager;

pub fn ReadyPhaseState(comptime HostRunner: type) type {
    return struct {
        allocator: std.mem.Allocator,
        topics: CacheManager.TopicsMap,
        guest_statuses: std.StringHashMap(Status),
        limit: HeartbeatTask.Limit,

        const Self = @This();

        pub fn create(allocator: std.mem.Allocator, guest_names: *std.BufSet, topics: CacheManager.TopicsMap, heartbeat_limit: HeartbeatTask.Limit) !Self {
            var guest_statuses = std.StringHashMap(Status).init(allocator);
            var iter = guest_names.iterator();
            while (iter.next()) |name| {
                try guest_statuses.put(try allocator.dupe(u8, name.*), .preparing);
            }

            return .{
                .allocator = allocator,
                .topics = topics,
                .guest_statuses = guest_statuses,
                .limit = heartbeat_limit,
            };
        }

        pub fn deinit(self: *Self) void {
            var iter = self.guest_statuses.keyIterator();
            while (iter.next()) |name| {
                self.allocator.free(name.*);
            }
            self.guest_statuses.deinit();
            self.topics.deinit();
        }

        pub fn handle(self: *Self, stage: *HostRunner, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
            switch (entry.event) {
                .ready => {
                    if (! self.guest_statuses.contains(entry.from_stage)) {
                        try stage.log(.debug, "External guest ready/name: {s}", .{entry.from_stage});
                        return;
                    }

                    if (self.guest_statuses.getPtr(entry.from_stage)) |status| {
                        try stage.log(.debug, "Guest ready/guest: {s}", .{entry.from_stage});
                        status.* = .ready;
                    }

                    // TODO: stub impl
                    if (self.checkStatus(.ready)) {
                        // All guests is ready
                        try stage.transitPhase(.terminating);
                    }
                },
                .heartbeat => |payload| {
                    if (!self.checkStatus(.ready)) {
                        switch (payload.event_type) {
                            .probe_ready => {
                                const interval = HostRunner.nextInterval(payload.count);
                                try stage.sendProbe(.probe_ready, payload.count, self.limit, interval);
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

        fn checkStatus(self: *const Self, needle: Status) bool {
            var iter = self.guest_statuses.iterator();
            while (iter.next()) |e| {
                if (e.value_ptr.* != needle) return false;
            }

            return true;
        }
    };
}

const Status = enum {
    preparing,
    ready,
    finished,
};