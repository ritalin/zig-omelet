const std = @import("std");
const root = @import("../root.zig");

const types = root.types;

const ReceiveEntry = root.sockets.ReceiveEntry;
const EventDispatcher = root.sockets.EventDispatcher;

pub fn BootPhaseState(comptime GuestStage: type) type {
    const VTable = @import("./phase_handler.zig").VTable(GuestStage);

    return struct {
        vtable: VTable,

        const Self = @This();

        pub const init: Self = .{ .vtable = .{} };
        pub fn deinit(_: *Self) void {}

        pub fn handle(self: *const Self, stage: *GuestStage, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
            switch (entry.event) {
                .launching => {
                    if (self.vtable.on_prepare) |on_prepare| {
                        on_prepare(stage) catch {
                            try stage.dispatcher.queue.post(.failed_launching, try stage.connection.dataChannel());
                            try stage.transitPhase(.terminating, .pending);
                            return;
                        };
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