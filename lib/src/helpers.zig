const std = @import("std");
const zmq = @import("zmq");

const types = @import("./types.zig");
const WAIT_TIME: u64 = 25_000; //nsec

/// make temporary folder for ipc socket
pub fn makeIpcChannelRoot() !std.fs.AtomicFile {
    return try std.fs.AtomicFile.init(std.fs.path.stem(types.CHANNEL_ROOT), std.fs.File.default_mode, try std.fs.openDirAbsolute(std.fs.path.dirname(types.CHANNEL_ROOT).?, .{}), true);
}

pub fn addSubscriberFilters(socket: *zmq.ZSocket, events: types.EventTypes) !void {
    var it = std.enums.EnumSet(types.EventType).init(events).iterator();

    while (it.next()) |ev| {
        const filter: []const u8 = @tagName(ev);
        try socket.setSocketOption(.{ .Subscribe = filter });
    }
}

/// Send event type only
pub fn sendEvent(allocator: std.mem.Allocator, socket: *zmq.ZSocket, ev: types.Event) !void {
    const event_tag = std.meta.activeTag(ev);

    event_only: {
        switch (ev) {
            .ack => break :event_only,
            .nack => break :event_only,
            .begin_topic => break :event_only,
            .end_topic => break :event_only,
            .begin_session => break :event_only,
            .next_generate => break :event_only,
            .end_generate => break :event_only,
            .finished => break :event_only,
            .quit_all => break :event_only,
            .quit => break :event_only,

            .launched => |payload| {
                return sendEventWithPayload(allocator, socket, event_tag, &.{
                    payload.stage_name, "",
                });
            },
            .topic => |payload| {
                return sendEventWithPayload(allocator, socket, event_tag, &.{
                    payload.name, "",
                });
            },
            .source => |payload| {
                return sendEventWithPayload(allocator, socket, event_tag, &.{
                    payload.name, payload.path, payload.hash, "",
                });
            },
            .topic_payload => |payload| {
                return sendEventWithPayload(allocator, socket, event_tag, &.{
                    payload.topic, payload.content, "",
                });
            },
            .quit_accept => |payload| {
                return sendEventWithPayload(allocator, socket, event_tag, &.{
                    payload.stage_name, "",
                });
            },
            .log => |payload| {
                return sendEventWithPayload(allocator, socket, event_tag, &.{
                    @tagName(payload.level), payload.content, "",
                });
            },
        }
    }

    return sendEventWithPayload(allocator, socket, event_tag, &.{
        ""
    });

    // std.time.sleep(WAIT_TIME);
}

fn sendEventInternal(allocator: std.mem.Allocator, socket: *zmq.ZSocket, ev: types.EventType, has_more: bool) !void {
    // std.debug.print("[DEBUG] Sending event: '{}' (more: {})\n", .{ev, has_more});
    var msg = try zmq.ZMessage.init(allocator, @tagName(ev));
    defer {
        // std.debug.print("[DEBUG] Begin dealoc sending event\n", .{});
        std.time.sleep(WAIT_TIME);
        msg.deinit();
        // std.debug.print("[DEBUG] End dealoc sending event\n", .{});
    }
    try socket.send(&msg, .{.more = has_more});
}

fn sendPayloadInternal(allocator: std.mem.Allocator, socket: *zmq.ZSocket, payload: types.Symbol, has_more: bool) !void {
    // std.debug.print("[DEBUG] Sending payload: '{s}' (more: {})\n", .{payload, has_more});
    var msg = try zmq.ZMessage.init(allocator, payload);
    defer {
        // std.debug.print("[DEBUG] Begin dealoc sending payload\n", .{});
        std.time.sleep(WAIT_TIME);
        msg.deinit();
        // std.debug.print("[DEBUG] End dealoc sending payload\n", .{});
    }
    try socket.send(&msg, .{.more = has_more});
}

/// Send event type + payload(s)
pub fn sendEventWithPayload(allocator: std.mem.Allocator, socket: *zmq.ZSocket, ev: types.EventType, payloads: []const types.Symbol) !void {    
    try sendEventInternal(allocator, socket, ev, payloads.len > 0);
    
    for (payloads, 1..) |payload, i| {
        try sendPayloadInternal(allocator, socket, payload, i < payloads.len);
    }
    // std.time.sleep(WAIT_TIME);
}

/// Receive event type only
pub fn receiveEventType(socket: *zmq.ZSocket) !types.EventType {
    var frame = try socket.receive(.{});
    defer frame.deinit();

    const msg = try frame.data();

    const event_type = std.meta.stringToEnum(types.EventType, msg);

    if (event_type == null) {
        std.debug.print("[DEBUG] Received unexpected raw event: {s}\n", .{msg});
        
        // TODO 連続データを捨てる・・・できる？
        var f2 = try socket.receive(.{});
        defer f2.deinit();
        const msg2 = try f2.data();
        _ = msg2;

        return error.InvalidResponse;
    }

    // std.debug.print("[DEBUG] Received raw event: {s}\n", .{msg});
    return event_type.?;
}

fn receivePayload(allocator: std.mem.Allocator, socket: *zmq.ZSocket) !types.Symbol {
    var frame = try socket.receive(.{});
    defer frame.deinit();

    return try allocator.dupe(u8, try frame.data());
}

/// Receive event + payload
pub fn receiveEventWithPayload(allocator: std.mem.Allocator, socket: *zmq.ZSocket) !types.Event {    
    const event_type = try receiveEventType(socket);

    const event: types.Event = event: {
        switch (event_type) {
            .ack => break :event .ack,
            .nack => break :event .nack,
            .begin_topic => break :event .begin_topic,
            .end_topic => break :event .end_topic,
            .begin_session => break :event .begin_session,
            .next_generate => break :event .next_generate,
            .end_generate => break :event .end_generate,
            .finished => break :event .finished,
            .quit_all => break :event .quit_all,
            .quit => break :event .quit,

            .launched => {
                const stage_name = try receivePayload(allocator, socket);

                break :event .{
                    .launched = .{
                        .stage_name = stage_name,
                    }
                };
            },
            .topic => {
                const payload = try receivePayload(allocator, socket);

                break :event .{ .topic = .{ 
                    .name = payload,
                } };
            },
            .source => {
                const name = try receivePayload(allocator, socket);
                const path = try receivePayload(allocator, socket);
                const hash = try receivePayload(allocator, socket);

                break :event .{
                    .source = .{
                        .name = name,
                        .path = path,
                        .hash = hash,
                    }
                };
            },
            .topic_payload => {
                const topic = try receivePayload(allocator, socket);
                const content = try receivePayload(allocator, socket);

                break :event .{
                    .topic_payload = .{
                        .topic = topic,
                        .content = content,
                    },
                };
            },
            .quit_accept => {
                const stage_name = try receivePayload(allocator, socket);

                break :event .{
                    .quit_accept = .{
                        .stage_name = stage_name,
                    }
                };
            },
            .log => {
                const level = try receivePayload(allocator, socket);
                const content = try receivePayload(allocator, socket);
                const log_level = std.meta.stringToEnum(types.LogLevel, level);

                if (log_level == null) {
                    std.debug.print("[DEBUG] Received unexpected raw log-level: {s}\n", .{level});
                }

                break :event .{
                    .log = .{
                        .level = log_level.?,
                        .content = content,
                    }
                };
            },
        }
    };

    // receive dummy payload
    _ = try receivePayload(allocator, socket);

    return event;
}