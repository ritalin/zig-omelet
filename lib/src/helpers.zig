const std = @import("std");
const zmq = @import("zmq");

const types = @import("./types.zig");

/// make temporary folder for ipc socket
pub fn makeIpcChannelRoot() !std.fs.AtomicFile {
    return std.fs.AtomicFile.init(std.fs.path.stem(types.CHANNEL_ROOT), std.fs.File.default_mode, try std.fs.openDirAbsolute(std.fs.path.dirname(types.CHANNEL_ROOT).?, .{}), true);
}

/// Send event type only
pub fn sendEvent(allocator: std.mem.Allocator, socket: *zmq.ZSocket, ev: types.EventType) !void {
    var msg = try zmq.ZMessage.init(allocator, &[_]u8{@intFromEnum(ev)});
    defer msg.deinit();
    return socket.send(&msg, .{});
}

/// Receive event type only
pub fn receiveEvent(socket: *zmq.ZSocket) !types.EventType {
    var frame = try socket.receive(.{});
    defer frame.deinit();

    const msg = try frame.data();
    
    return @enumFromInt(msg[0]);
}