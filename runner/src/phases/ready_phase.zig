const std = @import("std");
const core = @import("core");

const types = core.types;
const events = core.events;

const EventDispatcher = core.sockets.EventDispatcher;
const ReceiveEntry = core.sockets.ReceiveEntry;

const HeartbeatTask = @import("../tasks/HeartbeatTask.zig");
const CacheManager = @import("../cache_manager.zig").CacheManager;
const GuestConfig = @import("../configs/Config.zig").Guest;

pub fn ReadyPhaseState(comptime HostRunner: type) type {
    return struct {
        allocator: std.mem.Allocator,
        left_guests: std.BufSet,
        watch_guests: std.BufSet,
        cache: CacheManager,

        const Self = @This();

        pub fn create(allocator: std.mem.Allocator, stages: *const std.BufSet, guest_configs: *const std.MultiArrayList(GuestConfig), topics: CacheManager.TopicsMap) !Self {
            var watch_guests = std.BufSet.init(allocator);
            var generate_guests = std.BufSet.init(allocator);

            for (guest_configs.items(.kind), guest_configs.items(.name)) |kind, name| {
                switch (kind) {
                    .watch => try watch_guests.insert(name),
                    .generate => try generate_guests.insert(name),
                    else => {},
                }
            }

            return .{
                .allocator = allocator,
                .left_guests = try stages.cloneWithAllocator(allocator),
                .watch_guests = watch_guests,
                .cache = .init(topics, generate_guests),
            };
        }

        pub fn deinit(self: *Self) void {
            self.left_guests.deinit();
            self.watch_guests.deinit();
            self.cache.deinit(self.allocator);
        }

        pub fn handle(self: *Self, stage: *HostRunner, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
            switch (entry.event) {
                .ready => {
                    if (! self.left_guests.contains(entry.from_stage)) return;

                    try stage.log(.debug, "Guest ready/guest: {s}", .{entry.from_stage});

                    self.left_guests.remove(entry.from_stage);

                    if (self.left_guests.count() == 0) {
                        // All guests is ready
                        try stage.transitPhase(.ready, .confirmed);
                        try stage.sendProgressHeartbeat();

                        // TODO: Do not send in interactive mode
                        try stage.dispatcher.queue.post(.ready_source_path, try stage.connection.commandChannel());
                    }
                },
                .heartbeat => |payload| {
                    switch (payload.event_type) {
                        .probe => {
                            if (! self.isCompleted()) {
                                stage.sendProbeHeartbeat(payload.event_type, .ready, payload.count) catch |err| switch (err) {
                                    error.DiscardProbe => {
                                        try stage.defaultHandler(entry, dirty);
                                    },
                                    else => return err,
                                };
                            }
                        },
                        .ready_progress => {
                            if ((self.cache.header_entries.count() > 0) and (self.cache.body_entries.count() == 0)) {
                                const names: DescNames = .{.iter = self.cache.header_entries.keyIterator()};
                                try stage.log(.debug, "Unreceived source(s)/{f}", .{ names });
                            }
                            try stage.dispatcher.queue.post(.ready_progress, try stage.connection.commandChannel());
                            try stage.sendProgressHeartbeat();
                        },
                        else => {
                            try stage.defaultHandler(entry, dirty);
                        },
                    }
                },
                .source_path => |payload| {
                    try stage.log(.debug, "Source received/name: {s}, dialect: {s}, path: {s}", .{payload.name, payload.dialect, payload.path});
                    try self.cache.register(stage.allocator, &payload);
                    try stage.dispatcher.queue.post(.{ .source_path = payload }, try stage.connection.commandChannel());
                },
                .finish_source_path => {
                    // TODO: it does not remove in interactive mode
                    self.watch_guests.remove(entry.from_stage);
                },
                .ready_topic_body => |payload| {
                    try stage.log(.debug, "TopicBody extracted/name: {s}, offset: {}, dialect: {s}", .{payload.desc.name, payload.desc.offset, payload.desc.dialect});
                    switch (try self.cache.update(self.allocator, entry.from_stage, payload)) {
                        .progress => {},
                        .expired => {
                            try stage.log(.info, "Source expired/name: {s}, dialect: {s}", .{payload.desc.name, payload.desc.dialect});
                        },
                        .already_sent => {
                            try stage.log(.debug, "Skipped (Already sent)/name: {s}, offset: {}, dialect: {s}", .{payload.desc.name, payload.desc.offset, payload.desc.dialect});
                        },
                        .completed => {
                            try stage.log(.debug, "Skipped (Already completed)/name: {s}, offset: {}, dialect: {s}", .{payload.desc.name, payload.desc.offset, payload.desc.dialect});                            
                        },
                        .skipped => {
                            try stage.log(.info, "Skipped/name: {s}, offset: {}, dialect: {s}", .{payload.desc.name, payload.desc.offset, payload.desc.dialect});

                            if (self.isCompleted()) {
                                try stage.transitPhase(.terminating, .pending);
                            }
                        },
                        .ready => {
                            const body = self.cache.makeTopicBody(payload.desc);
                            
                            try stage.dispatcher.queue.post(.{ .topic_body = body }, try stage.connection.commandChannel());
                        },
                    }
                },
                .finish_generate => |payload| {
                    try stage.log(.info, "{s} {s}", .{toGnetatedMark(payload.status), payload.message});
                    self.cache.finishBodyEntry(self.allocator, entry.from_stage, payload.desc);

                    if (self.isCompleted()) {
                        try stage.transitPhase(.terminating, .pending);
                    }
                },
                else => {
                    try stage.defaultHandler(entry, dirty);
                }
            }
        }

        fn isCompleted(self: *const Self) bool {
            return (self.cache.header_entries.count() == 0) and (self.watch_guests.count() == 0);
        }
    };
}

fn toGnetatedMark(status: events.Event.Payload.GenerateResponse.Status) types.Symbol {
    return switch (status) {
        .new_file => "✨", 
        .update_file => "✏️", 
        .generate_failed => "❌"
    };
}

const Status = enum {
    preparing,
    ready,
    finish,
};

const DescNames = struct {
    iter: CacheManager.HeaderMap.KeyIterator,

    pub fn format(self: DescNames, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        var iter = self.iter;
        while (iter.next()) |desc| {
            try writer.print("{s}, ", .{desc.name});
        }
    }
};