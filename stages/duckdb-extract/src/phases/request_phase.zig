const std = @import("std");
const core = @import("core");
const c = @import("c");

const ReceiveEntry = core.sockets.ReceiveEntry;
const EventDispatcher = core.sockets.EventDispatcher;
const StructView = core.events.StructView;
const Event = core.events.Event;

pub fn RequestTopicPhaseState(comptime GuestStage: type) type {
    return struct {
        const Self = @This();

        pub const create: Self = .{};
        pub fn deinit(_: *Self) void {}

        pub fn handle(self: *Self, stage: *GuestStage, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
            _ = self;

            switch (entry.event) {
                .probe_request => {
                    topics: {
                        const topic: StructView(Event.Payload.Topic) = .{
                            .source,
                            &.{
                                c.topic_query, 
                                c.topic_placeholder, 
                                c.topic_placeholder_order, 
                                c.topic_select_list, 
                                c.topic_bound_user_type, 
                                c.topic_anon_user_type,
                            },
                        };
                        var channel = try stage.connection.dataChannel();
                        try channel.encode(.{.topic = Event.Payload.Topic.init(topic)});
                        try stage.dispatcher.queue.post(channel);
                        break :topics;
                    }
                    topics: {
                        const topic: StructView(Event.Payload.Topic) = .{
                            .schema,
                            &.{ c.topic_user_type },
                        };
                        var channel = try stage.connection.dataChannel();
                        try channel.encode(.{.topic = Event.Payload.Topic.init(topic)});
                        try stage.dispatcher.queue.post(channel);
                        break :topics;
                    }
                    finish_topic: {
                        var channel = try stage.connection.dataChannel();
                        try channel.encode(.finish_topic);
                        try stage.dispatcher.queue.post(channel);
                        try stage.transitPhase(.ready);
                        break:finish_topic;
                    }
                },
                else => {
                    try stage.defaultHandler(entry, dirty);
                }
            }
        }
    };
}