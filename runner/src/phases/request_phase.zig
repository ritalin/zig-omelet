const std = @import("std");
const core = @import("core");

const types = core.types;

const EventDispatcher = core.sockets.EventDispatcher;
const ReceiveEntry = core.sockets.ReceiveEntry;

const HeartbeatTask = @import("../tasks/HeartbeatTask.zig");

pub fn RequestPhaseState(comptime HostRunner: type) type {
    const TopicsMap = @import("../cache_manager.zig").CacheManager.TopicsMap;

    return struct {
        left_guests: std.BufSet,
        topics: TopicsMap,

        const Self = @This();

        pub fn create(allocator: std.mem.Allocator, guests: *std.BufSet) !Self {
            return .{
                .left_guests = try guests.cloneWithAllocator(allocator),
                .topics = TopicsMap.init(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            self.left_guests.deinit();
            self.topics.deinit();
        }

        pub fn handle(self: *Self, stage: *HostRunner, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
            switch (entry.event) {
                .topic => |payload| {
                    try self.topics.addTopics(payload);
                },
                .finish_topic => {
                    if (! self.left_guests.contains(entry.from_stage)) return;

                    self.left_guests.remove(entry.from_stage);

                    if (self.left_guests.count() == 0) {
                        if (core.Logger.accepted(.debug)) {
                            var iter = self.topics.iterator();
                            while (iter.next()) |e| {
                                try stage.log(.debug, "Topic/category: {s}, topic: {s}", .{@tagName(e.category), e.topic});
                            }
                        }

                        try stage.transitPhase(.ready, .pending);
                    }
                },
                .heartbeat => |payload| {
                    if (self.left_guests.count() > 0) {
                        stage.sendProbeHeartbeat(payload.event_type, .request, payload.count) catch |err| switch (err) {
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

        pub fn drainTopics(self: *Self, allocator: std.mem.Allocator) !TopicsMap {
            const topics = self.topics;
            self.topics = TopicsMap.init(allocator);

            return topics;
        }
    };
}