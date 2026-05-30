const std = @import("std");
const core = @import("core");
const TaskReaper = @import("./TaskReaper.zig");
const HeartbeatTask = @import("../tasks/HeartbeatTask.zig");

const Event = core.events.Event;
const StageName = core.types.StageName;
const Connection = core.sockets.Connection.Server;
const Dispatcher = core.sockets.EventDispatcher;

pub fn sendProbe(
    io: std.Io,
    reapers: *TaskReaper, 
    comptime stage_name: StageName, connection: *Connection(stage_name), 
    comptime poller_size: comptime_int, dispatcher: *Dispatcher.Sized(poller_size),
    event: Event, 
    count: usize, limit: HeartbeatTask.Limit, 
    interval: std.Io.Duration) !void 
{
    if (std.meta.activeTag(limit) == .count) {
        if (limit.count < count) return error.Timeout;
    }

    var channel = try connection.commandChannel();
    try channel.encode(event);
    try dispatcher.queue.post(channel);

    const args = .{
        io, 
        try connection.dataChannel(),
        std.meta.activeTag(event),
        count,
        interval,
    };
    try reapers.detach(HeartbeatTask.spawn, args);
}
