const std = @import("std");
const root = @import("../root.zig");

const types = root.types;

const ReceiveEntry = root.sockets.ReceiveEntry;
const EventDispatcher = root.sockets.EventDispatcher;

pub fn BootPhaseState(comptime GuestStage: type, comptime stage_name: types.StageName) type {
    const Connection = root.sockets.Connection.Client(stage_name);

    return struct {
        const Self = @This();

        pub const init: Self = .{};
        pub fn deinit(_: *Self) void {}

        pub fn handle(self: *const Self, stage: *GuestStage, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
            switch (entry.event) {
                .launching => {
                    try self.bootLog(stage);
                },
                .probe_launching => {
                    var channel = try stage.connection.requestChannel();
                    channel.submit(stage.connection.context.io, .launched, .{}) catch {
                        var push_channel = try stage.connection.dataChannel();
                        try push_channel.encode(.failed_launching);
                        try stage.dispatcher.queue.post(push_channel);
                        return;
                    };
                    try stage.transitPhase(.request);
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

        fn handshake(conn: *Connection, retry_count: usize) !void {
            // TODO:
            // Retrying itself
            var i: usize = 0;
            while (i < retry_count) {
                var channel = try conn.requestChannel();
                defer channel.deinit();
                try channel.encode(.launched);

                return (channel.submit(conn.context.io)) catch { i += 1; };
            }

            return error.LaunchFailed;
        }
    };
}