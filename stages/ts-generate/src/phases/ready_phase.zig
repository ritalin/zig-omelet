const std = @import("std");
const core = @import("core");

const ReceiveEntry = core.sockets.ReceiveEntry;
const EventDispatcher = core.sockets.EventDispatcher;
const StructView = core.events.StructView;
const Event = core.events.Event;

pub fn ReadyPhaseState(comptime GuestStage: type) type {
    return struct {
        const Self = @This();

        pub fn create() Self {
            return .{};
        }

        pub fn deinit(self: *Self) void {
            _ = self;
        }

        pub fn handle(self: *Self, stage: *GuestStage, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
            // TODO:
            // var lookup = std.StringHashMap(core.Event.Payload.SourcePath).init(self.allocator);
            // defer lookup.deinit();

            _ = self;

            switch (entry.event) {
                .probe_ready => {
                    var channel = try stage.connection.requestChannel();
                    try channel.submit(stage.connection.context.io, .ready, .{});
                },
                else => {
                    try stage.defaultHandler(entry, dirty);
                }
            }
        }
    };
}