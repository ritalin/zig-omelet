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
        cache: CacheManager,

        const Self = @This();

        pub fn create(allocator: std.mem.Allocator, guest_names: *std.BufSet, topics: CacheManager.TopicsMap) !Self {
            var guest_statuses = std.StringHashMap(Status).init(allocator);
            var iter = guest_names.iterator();
            while (iter.next()) |name| {
                try guest_statuses.put(try allocator.dupe(u8, name.*), .preparing);
            }

            return .{
                .allocator = allocator,
                .topics = topics,
                .guest_statuses = guest_statuses,
                .cache = .empty,
            };
        }

        pub fn deinit(self: *Self) void {
            var iter = self.guest_statuses.keyIterator();
            while (iter.next()) |name| {
                self.allocator.free(name.*);
            }
            self.guest_statuses.deinit();
            self.topics.deinit();
            self.cache.deinit(self.allocator);
        }

        pub fn handle(self: *Self, stage: *HostRunner, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
            switch (entry.event) {
                .ready => {
                    if (self.guest_statuses.getPtr(entry.from_stage)) |status| {
                        try stage.log(.debug, "Guest ready/guest: {s}", .{entry.from_stage});
                        status.* = .ready;
                    }

                    if (self.checkStatus(.ready)) {
                        // All guests is ready
                        try stage.transitPhase(.ready, .confirmed);
                        try stage.sendProgressHeartbeat();

                        // TODO: interactive mode
                        try stage.dispatcher.queue.post(.ready_source_path, try stage.connection.commandChannel());
                    }
                },
                .heartbeat => |payload| {
                    switch (payload.event_type) {
                        .probe => {
                            if (!self.checkStatus(.ready)) {
                                stage.sendProbeHeartbeat(payload.event_type, .ready, payload.count) catch |err| switch (err) {
                                    error.DiscardProbe => {
                                        dirty.* = .unhandled;
                                    },
                                    else => return err,
                                };
                            }
                        },
                        .ready_progress => {
                            try stage.dispatcher.queue.post(.ready_progress, try stage.connection.commandChannel());
                            try stage.sendProgressHeartbeat();
                        },
                        else => {},
                    }
                },
                .source_path => |payload| {
                    try self.cache.register(stage.allocator, &payload, &self.topics);
                    try stage.dispatcher.queue.post(.{ .source_path = payload }, try stage.connection.commandChannel());
                },
                .finish_source_path => {
                    if (self.guest_statuses.getPtr(entry.from_stage)) |status| {
                        try stage.log(.debug, "Guest finish ready/guest: {s}", .{entry.from_stage});
                        status.* = .finish;
                    }

                    // TODO: stub impl
                    try stage.transitPhase(.terminating, .pending);
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
    finish,
};