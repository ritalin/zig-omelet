const std = @import("std");
const zmq = @import("zmq");

const types = @import("./types.zig");
const cbor = @import("./decode_event_cbor.zig");
const encodeEvent = cbor.encodeEvent;
const decodeEvent = cbor.decodeEvent;

const WAIT_TIME: u64 = 25_000; //nsec

/// make temporary folder for ipc socket
pub fn makeIpcChannelRoot() !void {
    return std.fs.makeDirAbsolute(types.CHANNEL_ROOT) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };
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
    const data = try encodeEvent(allocator, ev);
    defer allocator.free(data);
    
    try sendEventTypeInternal(allocator, socket, std.meta.activeTag(ev), true);
    try sendPayloadInternal(allocator, socket, data, true);
    try sendPayloadInternal(allocator, socket, "", false);

    // std.time.sleep(WAIT_TIME);
}

fn sendEventTypeInternal(allocator: std.mem.Allocator, socket: *zmq.ZSocket, ev: types.EventType, has_more: bool) !void {
    // std.debug.print("[DEBUG] Sending event: '{}' (more: {})\n", .{ev, has_more});
    _ = allocator;
    var msg = try zmq.ZMessage.init(std.heap.c_allocator, @tagName(ev));
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

// /// Send event type + payload(s)
// pub fn sendEventWithPayload(allocator: std.mem.Allocator, socket: *zmq.ZSocket, ev: types.EventType, payloads: []const types.Symbol) !void {    
//     try sendEventInternal(allocator, socket, ev, payloads.len > 0);
    
//     for (payloads, 1..) |payload, i| {
//         try sendPayloadInternal(allocator, socket, payload, i < payloads.len);
//     }
//     // std.time.sleep(WAIT_TIME);
// }

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

/// Receive event
pub fn receiveEventWithPayload(allocator: std.mem.Allocator, socket: *zmq.ZSocket) !types.Event {    
    const event_type = try receiveEventType(socket);
    _ = event_type;
    const data = try receivePayload(allocator, socket);
    _ = try receivePayload(allocator, socket);

    return decodeEvent(allocator, data);
}