const std = @import("std");
const core = @import("core");

const EventType = core.events.EventType;
const Event = core.events.Event;
const CancelationToken = @import("./CancelationToken.zig");

cancel_token: ?*CancelationToken = null,

const Self = @This();

pub fn spawn(self: Self, io: std.Io, channel: core.sockets.SendChannel, event_type: EventType, count: usize, interval: std.Io.Duration) std.Io.Cancelable!void {
    for (0..count) |_| {
        if (self.cancel_token.?.isCanceled()) return error.Canceled;
        // std.debug.print("heartbet ({f}): {} of {}\n", .{interval, i, count});
        try io.sleep(interval, .real);
    }
    
    var channel_mut = channel;
    const g = channel_mut.sender.lock();
    defer g.unlock();
    channel_mut.encode(.{ .heartbeat = .{ .event_type = event_type, .count = count + 1 } }) catch return error.Canceled;
    channel_mut.submit(.{}) catch return error.Canceled;
}

pub const Limit = union(enum) {
    unlimited: void,
    count: u64,
};