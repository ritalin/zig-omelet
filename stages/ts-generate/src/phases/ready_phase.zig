const std = @import("std");
const core = @import("core");

const ReceiveEntry = core.sockets.ReceiveEntry;
const EventDispatcher = core.sockets.EventDispatcher;
const StructView = core.events.StructView;
const Event = core.events.Event;

const GenerateWorker = @import("../GenerateWorker.zig");

pub fn ReadyPhaseState(comptime GuestStage: type) type {
    return struct {
        t: *std.Io.Threaded,
        io: std.Io,

        const Self = @This();

        pub fn create(allocator: std.mem.Allocator) !Self {
            const t = try allocator.create(std.Io.Threaded);
            t.* = std.Io.Threaded.init(allocator, .{
                .concurrent_limit = std.Io.Limit.limited(try std.Thread.getCpuCount()),
            });
            
            return .{
                .t = t,
                .io = t.io(),
            };
        }

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            self.t.deinit();
            allocator.destroy(self.t);
        }

        pub fn handle(self: *Self, stage: *GuestStage, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
            switch (entry.event) {
                .probe => |phase| {
                    if ((phase == .ready) and (std.meta.eql(stage.dispatcher.phase, .{.kind = .ready, .agreement = .pending}))) {
                        var channel = try stage.connection.requestChannel();
                        try channel.submit(stage.connection.context.io, .ready, .{});
                        try stage.transitPhase(.ready, .confirmed);
                        return;
                    }
                },
                .topic_body => |payload| {
                    if (self.t.concurrent_limit.toInt()) |limit| {
                        if (self.t.busy_count >= limit) {
                            try stage.log(.trace, "Worker pool is full", .{});
                            // will process latter
                            dirty.* = .delayed;
                            return;
                        }
                    }
                    try stage.log(.debug, "TopicBody received/name: {s}, dialect: {s}, offset: {}", .{ payload.desc.name, payload.desc.dialect, payload.desc.offset });

                    const worker = try GenerateWorker.init(self.io, std.heap.c_allocator, &payload, stage.setting.output_dir_path);
                    _ = try self.io.concurrent(GenerateWorker.run, .{worker, self.io, stage.connection.push_worker_socket.pipe});
                    try stage.log(.trace, "Begin worker process/name: {s}, dialect: {s}", .{payload.desc.name, payload.desc.dialect});
                    return;
                },
                else => {}
            }
            try stage.defaultHandler(entry, dirty);
        }
    };
}