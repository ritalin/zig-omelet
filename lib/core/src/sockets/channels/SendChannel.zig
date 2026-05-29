const std = @import("std");
const nnng = @import("nnng");

const root = @import("../../root.zig");
const encodeToCbor = @import("../../events/encoder.zig").encodeToCbor;
const events = root.events;
const types = root.types;


const SendChannel = @This();

inner: InnerChannel,

pub fn init(allocator: std.mem.Allocator, pipe_id: u64, stage_name: types.StageName, sender: nnng.PipeSender) !SendChannel {
    return .{
        .inner = try InnerChannel.init(allocator, pipe_id, stage_name, sender),
    };
}

pub fn deinit(self: *SendChannel) void {
    self.inner.deinit();
}

pub fn encode(self: *SendChannel, event: events.Event) !void {
    std.log.scoped(.app).debug("SendQueue/event: {s}", .{@tagName(std.meta.activeTag(event))});
    return self.inner.encode(event);
}

const InnerChannel = struct {
    pipe_id: u64,
    buffer: std.Io.Writer.Allocating,
    msg: nnng.Message,
    sender: nnng.PipeSender,
    stage: types.StageName,

    pub fn init(allocator: std.mem.Allocator, pipe_id: u64, stage_name: types.StageName, sender: nnng.PipeSender) !InnerChannel {
        return .{
            .pipe_id = pipe_id,
            .buffer = std.Io.Writer.Allocating.init(allocator),
            .msg = try nnng.Message.create(),
            .sender = sender,
            .stage = stage_name,
        };
    }

    pub fn deinit(self: *InnerChannel) void {
        self.buffer.deinit();
    }

    pub fn encode(self: *InnerChannel, event: events.Event) !void {
        try encodeToCbor(&self.msg.writer, .{
            .header = events.EventHeader.fromEvent(event) ,
            .stage_name = self.stage,
            .event = event,
        });
    }
};

const Log = struct {
    inner: InnerChannel,

    pub fn init(allocator: std.mem.Allocator, pipe_id: u64, stage_name: types.StageName, sender: nnng.PipeSender) !SendChannel.Log {
        return .{
            .inner = try InnerChannel.init(allocator, pipe_id, stage_name, sender),
        };
    }

    pub fn deinit(self: *SendChannel.Log) void {
        self.inner.deinit();
    }

    pub fn encode(self: *SendChannel.Log, event: events.Event) !void {
        return self.inner.encode(event);
    }
};
