const std = @import("std");
const core = @import("core");

const types = core.types;
const events = core.events;

const EventDispatcher = core.sockets.EventDispatcher;
const ReceiveEntry = core.sockets.ReceiveEntry;

pub fn BootPhaseState(comptime HostRunner: type) type {
    return struct {
        const Self = @This();

        pub const init: Self = .{};
        pub fn deinit(_: *Self) void {}

        pub fn handle(self: *Self, stage: *HostRunner, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
            _ = self;

            switch (entry.event) {
                .launching => {
                    const ep = stage.setting.base.endpoints;

                    try stage.log(.debug, "Launched", .{});

                    dump_setting: {
                        try stage.log(.debug, "CLI: Req/Rep Channel = {s}", .{ep.req_rep});
                        try stage.log(.debug, "CLI: Pub/Sub Channel = {s}", .{ep.pub_sub});
                        try stage.log(.debug, "CLI: Push/Pull Channel = {s}", .{ep.push_pull});
                        break :dump_setting;
                    }

                    try stage.transitPhase(.connecting, .pending);
                },
                else => {
                    try stage.defaultHandler(entry, dirty);
                }
            }
        }
    };
}
