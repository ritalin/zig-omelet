const std = @import("std");
const nnng = @import("nnng");

const root = @import("../../root.zig");
const encodeToCbor = @import("../../events/encoder.zig").encodeToCbor;
const events = root.events;
const types = root.types;


const Self = @This();

buffer: std.Io.Writer.Allocating,
msg: nnng.Message,
sender: nnng.PipeSender,
stage: types.StageName,

pub fn init(allocator: std.mem.Allocator, stage_name: types.StageName, sender: nnng.PipeSender) !Self {
    return .{
        .buffer = std.Io.Writer.Allocating.init(allocator),
        .msg = try nnng.Message.create(),
        .sender = sender,
        .stage = stage_name,
    };
}

pub fn deinit(self: *Self) void {
    self.buffer.deinit();
}

pub fn encode(self: *Self, event: events.Event) !void {
    try encodeToCbor(&self.msg.writer, .{
        .header = events.EventHeader.fromEvent(event) ,
        .stage_name = self.stage,
        .event = event,
    });
}
