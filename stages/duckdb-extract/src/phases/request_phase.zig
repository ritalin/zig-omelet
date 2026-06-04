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
                .probe => |phase| {
                    if ((phase == .request) and (std.meta.eql(stage.dispatcher.phase, .{.kind = .request, .agreement = .pending}))) {
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
                            try stage.dispatcher.queue.post(
                                .{.topic = Event.Payload.Topic.init(topic)}, 
                                try stage.connection.dataChannel()
                            );
                            break :topics;
                        }
                        topics: {
                            const topic: StructView(Event.Payload.Topic) = .{
                                .schema,
                                &.{ c.topic_user_type },
                            };
                            try stage.dispatcher.queue.post(
                                .{.topic = Event.Payload.Topic.init(topic)},
                                try stage.connection.dataChannel()
                            );
                            break :topics;
                        }
                        finish_topic: {
                            try stage.dispatcher.queue.post(.finish_topic, try stage.connection.dataChannel());
                            try stage.transitPhase(.ready, .pending);
                            break:finish_topic;
                        }
                        return;
                    }
                },
                else => {}
            }
            try stage.defaultHandler(entry, dirty);
        }
    };
}