const std = @import("std");
const root = @import("../root.zig");

const StageName = root.types.StageName;
const Event = root.events.Event;
const EventType = root.events.EventType;

pub const EventPacket = struct {
    header: EventHeader,
    stage_name: StageName,
    event: Event,
};

const ExceptStructView = std.StaticStringMap(void).initComptime(.{
    .{@typeName(std.mem.Allocator)},
    .{@typeName(*std.heap.ArenaAllocator)},
});

pub fn StructView(comptime T: type) type {
    comptime std.debug.assert(@typeInfo(T) == .@"struct");

    const fields = std.meta.fields(T);
    comptime var i: usize = 0;
    comptime var types: [fields.len]type = undefined;
    inline for (fields) |field| {
        if (comptime (!ExceptStructView.has(@typeName(field.type)))) {
            defer i += 1;
            types[i] = field.type;
        }
    }

    return @Tuple(types[0..i]);
}

pub const EventHeader = union(EventType) {
    // Response events
    ack,
    nack,
    // periodically heartbeat
    heartbeat,
    probe,
    // Boot phase event
    launching,
    launched,
    failed_launching,
    // Request phase event
    topic,
    finish_topic,
    // Ready phase event
    ready,
    // watch event
    request_watch_path,

    // finish_watch_path,

    // Source path event
    ready_source_path,
    source_path,
    pending_finish_source_path,
    finish_source_path,
    // Topic body event
    ready_topic_body,
    topic_body,
    skip_topic_body,
    pending_finish_topic_body,
    finish_topic_body,
    // Generate event
    ready_generate,
    finish_generate,
    // Worker event
    worker_response,
    // Other event
    quit,
    log,
    report_fatal,
    pending_fatal_quit,

    pub const fromEvent = headerFromEvent;
};

fn headerFromEvent(tag: EventType) EventHeader {
    inline for (std.meta.fields(Event)) |f| {
        if (@field(EventHeader, f.name) == tag) {
            return @unionInit(EventHeader, f.name, {});
        }
    }
    unreachable;
}
