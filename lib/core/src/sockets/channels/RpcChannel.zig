const std = @import("std");
const nnng = @import("nnng");

const root = @import("../../root.zig");
const encodeToCbor = @import("../../events/encoder.zig").encodeToCbor;
const events = root.events;
const types = root.types;


const Self = @This();

sender: nnng.PipeSender,
receiver: nnng.PipeReceiver,
stage: types.StageName,

pub fn init(stage_name: types.StageName, sender: nnng.PipeSender, receiver: nnng.PipeReceiver) !Self {
    return .{
        .sender = sender,
        .receiver = receiver,
        .stage = stage_name,
    };
}

pub fn submit(self: *const Self, io: std.Io, event: events.Event, options: Self.Options) !void {
    const total_count = options.retry_count + 1;

    for (0..total_count) |i| {
        var rpc = try nnng.Rpc.create();
        defer rpc.deinit();
        
        try encodeToCbor(&rpc.msg.?.writer, .{
            .header = events.EventHeader.fromEvent(event) ,
            .stage_name = self.stage,
            .event = event,
        });
        
        var f = rpc.submit(io, self.sender, self.receiver);

        if (f.await(io)) {
            return;
        }
        else |err| {
            if (i > options.retry_count) return err;
        }
    }
}

const DEFAULT_RETRY: u64 = 3;

pub const Options = struct {
    retry_count: u64 = DEFAULT_RETRY,
};