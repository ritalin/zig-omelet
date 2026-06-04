const std = @import("std");
const root = @import("../root.zig");

const types = root.types;

const ReceiveEntry = root.sockets.ReceiveEntry;
const EventDispatcher = root.sockets.EventDispatcher;

pub fn BootPhaseState(comptime GuestStage: type) type {
    return struct {
        const Self = @This();

        pub const init: Self = .{};
        pub fn deinit(_: *Self) void {}

        pub fn handle(self: *const Self, stage: *GuestStage, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
            switch (entry.event) {
                .launching => {
                    try self.bootLog(stage);
                },
                .probe => |phase| {
                    if ((phase == .launching) and (std.meta.eql(stage.dispatcher.phase, .{.kind = .launching, .agreement = .pending}))) {
                        var channel = try stage.connection.requestChannel();
                        channel.submit(stage.connection.context.io, .launched, .{}) catch {
                            try stage.dispatcher.queue.post(.failed_launching, try stage.connection.dataChannel());
                            return;
                        };
                        try stage.transitPhase(.request, .pending);
                    }
                    else {
                        try stage.defaultHandler(entry, dirty);
                    }
                },
                else => {
                    try stage.defaultHandler(entry, dirty);
                }
            }
        }

        fn bootLog(self: *const Self, stage: *GuestStage) !void {
            _ = self;
            const ep = stage.setting.endpoints;

            try stage.log(.debug, "Beginning...", .{});

            dump_subscription: {
                var arena = std.heap.ArenaAllocator.init(stage.allocator);
                defer arena.deinit();

                try stage.log(.debug, "Subscriber filters: {s}", .{try stage.connection.listSubscriptions(arena.allocator())});
                break:dump_subscription;
            }
            dump_setting: {
                try stage.log(.debug, "CLI: Req/Rep Channel = {s}", .{ep.req_rep});
                try stage.log(.debug, "CLI: Pub/Sub Channel = {s}", .{ep.pub_sub});
                try stage.log(.debug, "CLI: Push/pull Channel = {s}", .{ep.push_pull});
                break :dump_setting;
            }
        }
    };
}