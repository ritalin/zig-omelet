const std = @import("std");
const core = @import("core");

const EventType = core.events.EventType;
const Event = core.events.Event;

pub fn spawn(io: std.Io, channel: core.sockets.SendChannel, event_type: EventType, count: usize, unit: std.Io.Duration) std.Io.Cancelable!void {
    const interval: std.Io.Duration = .fromNanoseconds(unit.nanoseconds * count);
    try io.sleep(interval, .real);
    
    var channel_mut = channel;

    sendHeartbeatInternal(&channel_mut, .{ .event_type = event_type, .count = count }) catch {
        return error.Canceled;
    };
}

fn sendHeartbeatInternal(channel: *core.sockets.SendChannel, heartbeat: Event.Payload.Heartbeat) !void {
    try channel.encode(.{ .heartbeat = heartbeat });
    try channel.sender.submit(channel.msg, .{});
}
