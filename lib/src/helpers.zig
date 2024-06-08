const std = @import("std");
const zmq = @import("zmq");

const types = @import("./types.zig");

/// make temporary folder for ipc socket
pub fn makeIpcChannelRoot() !std.fs.AtomicFile {
    return try std.fs.AtomicFile.init(std.fs.path.stem(types.CHANNEL_ROOT), std.fs.File.default_mode, try std.fs.openDirAbsolute(std.fs.path.dirname(types.CHANNEL_ROOT).?, .{}), true);
}

pub fn addSubscriberFilters(socket: *zmq.ZSocket, events: types.EventTypes) !void {
    var it = std.enums.EnumSet(types.EventType).init(events).iterator();

    while (it.next()) |ev| {
        const filter: []const u8 = @tagName(ev);
        std.debug.print("[DEBUG] SUB filter: {s}\n", .{filter});
        try socket.setSocketOption(.{ .Subscribe = filter });
    }
}

/// Send event type only
pub fn sendEvent(allocator: std.mem.Allocator, socket: *zmq.ZSocket, ev: types.EventType) !void {
    try sendEventInternal(allocator, socket, ev, false);
}

fn sendEventInternal(allocator: std.mem.Allocator, socket: *zmq.ZSocket, ev: types.EventType, has_more: bool) !void {
    var msg = try zmq.ZMessage.init(allocator, @tagName(ev));
    defer msg.deinit();
    try socket.send(&msg, .{.more = has_more});
}

pub fn sendPayload(allocator: std.mem.Allocator, socket: *zmq.ZSocket, payload: types.Symbol) !void {
    return try sendPayloadInternal(allocator, socket, payload, false);
}

fn sendPayloadInternal(allocator: std.mem.Allocator, socket: *zmq.ZSocket, payload: types.Symbol, has_more: bool) !void {
    var msg = try zmq.ZMessage.init(allocator, payload);
    defer msg.deinit();
    try socket.send(&msg, .{.more = has_more});
}

/// Send event type + payload(s)
pub fn sendEventWithPayload(allocator: std.mem.Allocator, socket: *zmq.ZSocket, ev: types.EventType, payloads: []const types.Symbol) !void {    
    try sendEventInternal(allocator, socket, ev, payloads.len > 0);
    
    for (payloads, 1..) |payload, i| {
        try sendPayloadInternal(allocator, socket, payload, i <= payloads.len);
    }
}

/// Receive event type only
pub fn receiveEventType(socket: *zmq.ZSocket) !types.EventType {
    var frame = try socket.receive(.{});
    defer frame.deinit();

    const msg = try frame.data();

    return std.meta.stringToEnum(types.EventType, msg).?;
}

fn readEventType(msg: zmq.ZMessage) !types.EventType {
    const data = try msg.data();

    return @enumFromInt(data[0]);
}

fn receivePayload(allocator: std.mem.Allocator, socket: *zmq.ZSocket) !types.Symbol {
    var frame = try socket.receive(.{});
    defer frame.deinit();

    return try allocator.dupe(u8, try frame.data());
}

fn readPayload(allocator: std.mem.Allocator, msg: zmq.ZMessage) !types.Symbol {
    return try allocator.dupe(u8, try msg.data());
}

/// Receive event + payload
pub fn receiveEventWithPayload(allocator: std.mem.Allocator, socket: *zmq.ZSocket) !types.Event {    
    const ev = try receiveEventType(socket);

    return event: {
        switch (ev) {
            .launched => return .launched,
            .begin_topic => return .begin_topic,
            .end_topic => return .end_topic,
            .begin_session => return .begin_session,
            .next_generate => return .next_generate,
            .end_generate => return .end_generate,
            .finished => return .finished,
            .finished_accept => return .finished_accept,
            .quit_all => return .quit_all,
            .quit => return .quit,
            .quit_accept => return .quit_accept,

            .topic => {
                const payload = try receivePayload(allocator, socket);
                break :event .{ .topic = .{ .name = payload } };
            },
            .source => {
                const path = try receivePayload(allocator, socket);
                const content = try receivePayload(allocator, socket);
                const hash = try receivePayload(allocator, socket);
                
                break :event .{
                    .source = .{
                        .path = try allocator.dupe(u8, path),
                        .content = try allocator.dupe(u8, content),
                        .hash = try allocator.dupe(u8, hash),
                    }
                };
            },
            .topic_payload => {
                unreachable;
            },
        }
    };
}