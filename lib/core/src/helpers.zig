const std = @import("std");
const zmq = @import("zmq");

const types = @import("./types.zig");
const cbor = @import("./decode_event_cbor.zig");
const encodeEvent = cbor.encodeEvent;
const decodeEvent = cbor.decodeEvent;

const StageName = types.Symbol;
const WAIT_TIME: u64 = 25_000; //nsec

/// make temporary folder for ipc socket
pub fn makeIpcChannelRoot(endpoints: types.Endpoints) !void {
    try makeIpcChannelRootInternal(endpoints.req_rep);
    try makeIpcChannelRootInternal(endpoints.pub_sub);
}

fn makeIpcChannelRootInternal(channel: types.Symbol) !void {
    if (ipcChannelRootPath(channel)) |path| {
        try std.fs.cwd().makePath(path);
    }
}

pub fn cleanupIpcChannelRoot(endpoints: types.Endpoints) void {
    cleanupIpcChannelRootInternal(endpoints.req_rep);
    cleanupIpcChannelRootInternal(endpoints.pub_sub);
}

fn cleanupIpcChannelRootInternal(channel: types.Symbol) void {
    if (ipcChannelRootPath(channel)) |path| {
        if (std.fs.path.dirname(path)) |parent_dir_path| {
            var parent_dir = std.fs.cwd().openDir(parent_dir_path, .{}) catch {
                return;
            };
            defer parent_dir.close();

            const stem_path = std.fs.path.stem(path);
            parent_dir.deleteTree(stem_path) catch {};
        }
    }
}

fn ipcChannelRootPath(channel: types.Symbol) ?types.FilePath {
    if (std.mem.startsWith(u8, channel, types.IPC_PROTOCOL)) {
        return std.fs.path.dirname(channel[types.IPC_PROTOCOL.len..]);
    }
    return null;       
}

const AVAILABLE_PROROCOLS = std.StaticStringMap(void).initComptime(.{
    .{"tcp://"}, .{"udp://"}
});

pub fn resolveIPCConnectPort(allocator: std.mem.Allocator, channel_root: types.Symbol, port_name: types.Symbol) !types.Symbol {
    return resolveConnectPortInternal(try std.fmt.allocPrint(allocator, "{s}/{s}", .{channel_root, port_name}));
}

pub fn resolveConnectPort(allocator: std.mem.Allocator, channel: types.Symbol) !types.Symbol {
    return resolveConnectPortInternal(try allocator.dupe(u8, channel));
}

fn resolveConnectPortInternal(channel: types.Symbol) !types.Symbol {
    if (std.mem.startsWith(u8, channel, "ipc://")) {
        return channel;
    }

    for (AVAILABLE_PROROCOLS.keys()) |k| {
        if (std.mem.startsWith(u8, channel, k)) {
            const port_index = std.mem.lastIndexOf(u8, channel, ":");
            if (port_index == null) return error.ChannelPort;

            return channel;
        }
    }

    return error.ChannelProtocl;
}

pub fn resolveIPCBindPort(allocator: std.mem.Allocator, channel_root: types.Symbol, port_name: types.Symbol) !types.Symbol {
    return resolveBindPortInternal(
        allocator, 
        try std.fmt.allocPrint(allocator, "{s}/{s}", .{channel_root, port_name})
    );
}

pub fn resolveBindPort(allocator: std.mem.Allocator, channel: types.Symbol) !types.Symbol {
    return resolveBindPortInternal(allocator, try allocator.dupe(u8, channel));
}
fn resolveBindPortInternal(allocator: std.mem.Allocator, channel: types.Symbol) !types.Symbol {
    if (std.mem.startsWith(u8, channel, "ipc://")) {
        return channel;
    }

    for (AVAILABLE_PROROCOLS.keys()) |k| {
        if (std.mem.startsWith(u8, channel, k)) {
            const port_index = std.mem.lastIndexOf(u8, channel, ":");
            if (port_index == null) return error.ChannelPort;

            defer allocator.free(channel);
            return try std.fmt.allocPrint(allocator, "{s}*{s}", .{k, channel[port_index.?..]});
        }
    }

    return error.ChannelProtocl;
}

pub fn bytesToHexAlloc(allocator: std.mem.Allocator, input: []const u8) ![]const u8 {
    var result = try allocator.alloc(u8, input.len * 2);
    if (input.len == 0) return result;

    const charset = "0123456789" ++ "abcdef";

    for (input, 0..) |b, i| {
        result[i * 2 + 0] = charset[b >> 4];
        result[i * 2 + 1] = charset[b & 15];
    }
    return result;
}

pub fn addSubscriberFilters(socket: *zmq.ZSocket, events: types.EventTypes) !void {
    var it = std.enums.EnumSet(types.EventType).init(events).iterator();

    while (it.next()) |ev| {
        const filter: []const u8 = @tagName(ev);
        try socket.setSocketOption(.{ .Subscribe = filter });
    }
}

/// Send event type only
pub fn sendEvent(allocator: std.mem.Allocator, socket: *zmq.ZSocket, from: StageName, ev: types.Event) !void {
    const data = try encodeEvent(allocator, ev);
    defer allocator.free(data);
    
    try sendEventTypeInternal(allocator, socket, ev.tag(), true);
    try sendPayloadInternal(allocator, socket, from, true);
    try sendPayloadInternal(allocator, socket, data, true);
    try sendPayloadInternal(allocator, socket, "", false);
}

fn sendEventTypeInternal(allocator: std.mem.Allocator, socket: *zmq.ZSocket, ev: types.EventType, has_more: bool) !void {
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

/// Receive event type only
fn receiveEventType(socket: *zmq.ZSocket) !types.EventType {
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
pub fn receiveEventWithPayload(allocator: std.mem.Allocator, socket: *zmq.ZSocket) !struct{StageName, types.Event} {    
    // event type
    const event_type = try receiveEventType(socket);
    // from
    const from = try receivePayload(allocator, socket);
    // payload
    const data = try receivePayload(allocator, socket);
    defer allocator.free(data);
    // empty
    const term = try receivePayload(allocator, socket);
    defer allocator.free(term);

    return .{from, try decodeEvent(allocator, event_type, data)};
}

const SocketTypes = std.enums.EnumFieldStruct(zmq.ZSocketType, bool, false);
const SocketTypeSet = std.enums.EnumSet(zmq.ZSocketType);

fn hasSocketType(socket: *zmq.ZSocket, socket_types: SocketTypes) !bool {
    var opt: zmq.ZSocketOption = .{.SocketType = undefined};
    try socket.getSocketOption(&opt);

    return SocketTypeSet.init(socket_types).contains(opt.SocketType);
}
