const std = @import("std");
const nnng = @import("nnng");

const root = @import("../../root.zig");
const encodeToCbor = @import("../../events/encoder.zig").encodeToCbor;
const events = root.events;
const types = root.types;

pub const SendChannel = struct {
    pipe_id: u64,
    buffer: std.Io.Writer.Allocating,
    msg: nnng.Message,
    sender: nnng.PipeSender,
    stage: types.StageName,

    pub fn init(allocator: std.mem.Allocator, pipe_id: u64, stage_name: types.StageName, sender: nnng.PipeSender) !SendChannel {
        return .{
            .pipe_id = pipe_id,
            .buffer = std.Io.Writer.Allocating.init(allocator),
            .msg = try nnng.Message.create(),
            .sender = sender,
            .stage = stage_name,
        };
    }

    pub fn deinit(self: *SendChannel) void {
        self.buffer.deinit();
    }

    pub fn encode(self: *SendChannel, event: events.Event) !void {
        try encodeToCbor(&self.msg.writer, .{
            .header = events.EventHeader.fromEvent(event) ,
            .stage_name = self.stage,
            .event = event,
        });
    }

    pub fn submit(self: *const SendChannel, options: nnng.PipeSender.Options) nnng.SendError!void {
        var sender = self.sender;
        return sender.submit(self.msg, options);
    }
};
