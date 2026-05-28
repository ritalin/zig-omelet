const std = @import("std");
const nnng = @import("nnng");
const root = @import("../../root.zig");

const StageName = root.types.StageName;
const Event = root.events.Event;

const decodeFromCbor = @import("../../events/decoder.zig").decodeFromCbor;

pipe_id: u64,
event: Event,
from_stage: StageName,
buffer: []const u8,
msg: nnng.Message,
features: nnng.Pipe.Features,

const Self = @This();

pub fn create(allocator: std.mem.Allocator, pipe_id: u64, msg: nnng.Message, features: nnng.Pipe.Features) !Self {
    const buffer = if (features.replyable) try allocator.dupe(u8, msg.bytes()) else msg.bytes();
    errdefer if (features.replyable) allocator.free(buffer);

    const packet = try decodeFromCbor(allocator, buffer);

    return .{
        .pipe_id = pipe_id,
        .event = packet.event,
        .from_stage = packet.stage_name,
        .buffer = buffer,
        .msg = msg,
        .features = features,
    };
}

pub fn booting(stage_name: StageName) !Self {
    const msg = try nnng.Message.withCapacity(0);

    return .{
        .pipe_id = 0,
        .event = .launching,
        .from_stage = stage_name,
        .buffer = &.{},
        .msg = msg,
        .features = .{ .last_msg_owner = true },
    };
}

pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
    if (self.features.last_msg_owner) {
        self.msg.deinit();
    }
    if (self.features.replyable) {
        allocator.free(self.buffer);
    }
    self.event.deinit(allocator);
}
