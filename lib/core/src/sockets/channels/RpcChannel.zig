const std = @import("std");
const nnng = @import("nnng");

const root = @import("../../root.zig");
const encodeToCbor = @import("../../events/encoder.zig").encodeToCbor;
const events = root.events;
const types = root.types;


const Self = @This();

rpc: nnng.Rpc,
sender: nnng.PipeSender,
receiver: nnng.PipeReceiver,
stage: types.StageName,

pub fn init(stage_name: types.StageName, sender: nnng.PipeSender, receiver: nnng.PipeReceiver) !Self {
    return .{
        .rpc = try nnng.Rpc.create(),
        .sender = sender,
        .receiver = receiver,
        .stage = stage_name,
    };
}

pub fn deinit(self: *Self) void {
    self.rpc.deinit();
}

pub fn encode(self: *Self, event: events.Event) !void {
    var msg = self.rpc.msg.?;

    try encodeToCbor(&msg.writer, .{
        .header = events.EventHeader.fromEvent(event) ,
        .stage_name = self.stage,
        .event = event,
    });
}

pub fn submit(self: *Self, io: std.Io) !void {
    var f = self.rpc.submit(io, self.sender, self.receiver);
    try f.await(io);
}