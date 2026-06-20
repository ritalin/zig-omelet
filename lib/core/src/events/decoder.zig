const std = @import("std");
const root = @import("../root.zig");

const types = root.types;
const events = root.events;
const impl = @import("./event_impl.zig");

const EventType = root.events.EventType;
const Event = root.events.Event;
const CborStream = @import("cbor").CborStream;
const StructView = impl.StructView;
const EventHeader = impl.EventHeader;
const EventPacket = impl.EventPacket;

pub fn decodeFromCbor(allocator: std.mem.Allocator, data: []const u8) !EventPacket {
    var reader = CborStream.Reader.createFromSlice(data);
    return decodeFromCborInternal(allocator, &reader);
}

pub fn decodeFromCborInternal(allocator: std.mem.Allocator, reader: *CborStream.Reader) !EventPacket {
    const header = try decodeEventHeader(reader);
    const stage = try reader.readString();
    const event = try decodeEventInternal(allocator, header, reader);

    return .{
        .header = header,
        .stage_name = stage,
        .event = event,
    };
}

fn decodeEventInternal(allocator: std.mem.Allocator, event_type: EventType, reader: *CborStream.Reader) !Event {
    switch (event_type) {
        // Response events
        .ack => return .ack,
        .nack => return .nack,
        // periodically heartbeat
        .heartbeat => {
            const view = try reader.readTupleWithAllocator(allocator, StructView(Event.Payload.Heartbeat));
            return .{ .heartbeat = Event.Payload.Heartbeat.init(view) };
        },
        .probe => {
            const phase = try reader.readEnum(events.EventPhase.Kind);
            return .{.probe = phase};
        },
        // Boot phase event
        .launching => return .launching,
        .launched => return .launched,
        .failed_launching => return .failed_launching,
        // Topic request phase event
        .topic => {
            const view = try reader.readTupleWithAllocator(allocator, StructView(Event.Payload.Topic));

            return .{
                .topic = Event.Payload.Topic.init(view),
            };
        },
        .finish_topic => return .finish_topic,
        // Ready phase event
        .ready => return .ready,
        .ready_progress => return .ready_progress,
        // .finish_watch_path => return .finish_watch_path, // TODO:deprecated

        // Source path event
        .ready_source_path => return .ready_source_path,
        .source_path => {
            const path = try reader.readTuple(StructView(Event.Payload.SourcePath));

            return .{
                .source_path = Event.Payload.SourcePath.init(path),
            };
        },
        .finish_source_path => return .finish_source_path,
        // Topic body event
        .ready_topic_body => {
            const desc: Event.Payload.SourceDescriptor = .init(try reader.readTuple(StructView(Event.Payload.SourceDescriptor)));
            const hash = try reader.readString();
            const name_alt = try reader.readOptional(types.Symbol);
            const response: Event.Payload.TopicBodyResponse.Result = response: {
                switch (try reader.readEnum(events.ResponseTag)) {
                    .progress => {
                        break:response .{.progress = try reader.readUInt(usize) };
                    },
                    .success => {
                        const len =  try reader.readSliceHeader();
                        const topics = try allocator.alloc( Event.Payload.TopicBody.Encoded, len);
                        for (0..len) |i| {
                            topics[i] = .init(try reader.readTuple(StructView(Event.Payload.TopicBody.Encoded)));
                        }
                        break:response .{.success = topics };
                    },
                    .skipped => break:response .skipped,
                }
            };
            const payload: Event.Payload.TopicBodyResponse = .{ 
                .desc = desc, 
                .hash = hash, 
                .name_alt = name_alt, 
                .response = response 
            };

            return .{.ready_topic_body = payload};
        },
        .topic_body => {
            const desc = try reader.readTuple(StructView(Event.Payload.SourceDescriptor));
            const name_alt = try reader.readOptional(types.Symbol);

            const bodies = try reader.readSlice(allocator, StructView(Event.Payload.TopicBody.Encoded));
            defer allocator.free(bodies);

            const payload: Event.Payload.TopicBody = .{
                .desc = .init(desc),
                .name_alt = name_alt,
                .bodies = try Event.Payload.TopicBody.Encoded.fromValuesSet(allocator, bodies),
            };

            return .{ .topic_body = payload };
        },
        .skip_topic_body => {
            const desc = try reader.readTuple(StructView(Event.Payload.SourceDescriptor));

            return .{ .skip_topic_body = .init(desc) };
        },
        .finish_topic_body => {
            const desc = try reader.readTuple(StructView(Event.Payload.SourceDescriptor));

            return .{ .finish_topic_body = .init(desc) };
        },
        .pending_finish_topic_body => return .pending_finish_topic_body,
        // Generation event
        .ready_generate => return .ready_generate,
        .finish_generate => {
            const desc = try reader.readTuple(StructView(Event.Payload.SourceDescriptor));
            const status = try reader.readEnum(Event.Payload.GenerateResponse.Status);
            const message = try reader.readString();

            const payload: Event.Payload.GenerateResponse = .{
                .desc = .init(desc),
                .status = status,
                .message = message,
            };
            return .{.finish_generate = payload};
        },
        // Worker event
        .worker_response, => {
            const content = try reader.readString();

            return .{
                .worker_response = try Event.Payload.WorkerResponse.init(allocator, .{content}),
            };
        },
        // Other event
        .quit => return .quit,
        .log => {
            const log = try reader.readTuple(StructView(Event.Payload.Log));

            return .{
                .log = Event.Payload.Log.init(log),
            };
        },
        .report_fatal => {
            const log = try reader.readTuple(StructView(Event.Payload.Log));

            return .{
                .report_fatal = Event.Payload.Log.init(log),
            };
        },
        .pending_fatal_quit => return .pending_fatal_quit,
    }
}

pub fn decodeSubscription(data: types.BinaryData) !events.EventHeader {
    var reader = CborStream.Reader.createFromSlice(data);
    return decodeEventHeader(&reader);
}

fn decodeEventHeader(reader: *CborStream.Reader) !events.EventHeader {
    const tag: events.EventType = try reader.readEnum(events.EventType);
    
    // TODO:EventHeader variant
    switch (tag) {
        else => {
            inline for (std.meta.fields(events.EventType)) |field| {
                if (field.value == @intFromEnum(tag)) {
                    return @unionInit(events.EventHeader, field.name, {});
                }
            }
        }
    }
    unreachable; 
}

test "encoder/decoder" {
    std.testing.refAllDecls(@This());
}

pub const tests = struct {
    const encodeToCbor = @import("./encoder.zig").encodeToCbor;
    const test_context = "test-lib-core";

    test "Encode/Decode event" {
        const allocator = std.testing.allocator;

        var buffer = std.Io.Writer.Allocating.init(allocator);
        defer buffer.deinit();

        const heartbeat = Event.Payload.Heartbeat.init(.{ .probe, 1 });
        const topic = Event.Payload.Topic.init(.{ .source, &.{"topic_a", "topic_b", "topic_c"} });
        var source_path = Event.Payload.SourcePath.init(.{ .source, "Some-name", "Some-path", "duckdb", "Some-content", 1 });
        defer source_path.deinit(allocator);
        const desc = Event.Payload.SourceDescriptor.init(.{ .source, "path/to/name", "duckdb", 0 });
        const topic_body_response_progress: Event.Payload.TopicBodyResponse = .{
            .desc = desc,
            .hash = "deadbeaf",
            .name_alt = null,
            .response = .{
                .progress = 42,
            },
        };
        const topic_body_response_success: Event.Payload.TopicBodyResponse = .{
            .desc = desc,
            .hash = "deadbeaf",
            .name_alt = null,
            .response = .{
                .success = &.{
                    Event.Payload.TopicBody.Encoded.init(.{"topic_a", "0x123456789"}), 
                    Event.Payload.TopicBody.Encoded.init(.{"topic_b", "0x987654321"}),
                }
            },
        };
        const topic_body_response_skip: Event.Payload.TopicBodyResponse = .{
            .desc = desc,
            .hash = "deadbeaf",
            .name_alt = null,
            .response = .skipped,
        };
        const topic_body: Event.Payload.TopicBody = .{
            .desc = desc,
            .name_alt = "alt",
            .bodies = &.{
                Event.Payload.TopicBody.Encoded.init(.{ "topic_a", "topic_a_content" }),
                Event.Payload.TopicBody.Encoded.init(.{ "topic_b", "topic_b_content" }),
                Event.Payload.TopicBody.Encoded.init(.{ "topic_c", "topic_c_content" }),
            },
        };
        const generate_response: Event.Payload.GenerateResponse = .{
            .desc = desc,
            .status = .new_file,
            .message = "Success!!"
        };
        const worker_response = try Event.Payload.WorkerResponse.init(allocator, .{"some-worker-text"});
        defer worker_response.deinit();
        var log = Event.Payload.Log.init(.{.debug, "Test message😃"});
        defer log.deinit();

        try encodeToCbor(&buffer.writer, EventPacket{ .header = EventHeader.fromEvent(.ack), .stage_name = test_context, .event = .ack });
        try encodeToCbor(&buffer.writer, EventPacket{ .header = EventHeader.fromEvent(.nack), .stage_name = test_context, .event = .nack });
        try encodeToCbor(&buffer.writer, EventPacket{ .header = EventHeader.fromEvent(.heartbeat), .stage_name = test_context, .event = .{.heartbeat = heartbeat} });
        try encodeToCbor(&buffer.writer, EventPacket{ .header = EventHeader.fromEvent(.probe), .stage_name = test_context, .event = .{.probe = .launching} });
        try encodeToCbor(&buffer.writer, EventPacket{ .header = EventHeader.fromEvent(.launching), .stage_name = test_context, .event = .launching });
        try encodeToCbor(&buffer.writer, EventPacket{ .header = EventHeader.fromEvent(.launched), .stage_name = test_context, .event = .launched });
        try encodeToCbor(&buffer.writer, EventPacket{ .header = EventHeader.fromEvent(.failed_launching), .stage_name = test_context, .event = .failed_launching });
        try encodeToCbor(&buffer.writer, EventPacket{ .header = EventHeader.fromEvent(.topic), .stage_name = test_context, .event = .{.topic = topic} });
        try encodeToCbor(&buffer.writer, EventPacket{ .header = EventHeader.fromEvent(.finish_topic), .stage_name = test_context, .event = .finish_topic });
        try encodeToCbor(&buffer.writer, EventPacket{ .header = EventHeader.fromEvent(.ready), .stage_name = test_context, .event = .ready });
        try encodeToCbor(&buffer.writer, EventPacket{ .header = EventHeader.fromEvent(.ready_progress), .stage_name = test_context, .event = .ready_progress });
        try encodeToCbor(&buffer.writer, EventPacket{ .header = EventHeader.fromEvent(.ready_source_path), .stage_name = test_context, .event = .ready_source_path });
        try encodeToCbor(&buffer.writer, EventPacket{ .header = EventHeader.fromEvent(.source_path), .stage_name = test_context, .event = .{.source_path = source_path} });
        try encodeToCbor(&buffer.writer, EventPacket{ .header = EventHeader.fromEvent(.finish_source_path), .stage_name = test_context, .event = .finish_source_path });
        try encodeToCbor(&buffer.writer, EventPacket{ .header = EventHeader.fromEvent(.ready_topic_body), .stage_name = test_context, .event = .{.ready_topic_body = topic_body_response_progress} });
        try encodeToCbor(&buffer.writer, EventPacket{ .header = EventHeader.fromEvent(.ready_topic_body), .stage_name = test_context, .event = .{.ready_topic_body = topic_body_response_success} });
        try encodeToCbor(&buffer.writer, EventPacket{ .header = EventHeader.fromEvent(.ready_topic_body), .stage_name = test_context, .event = .{.ready_topic_body = topic_body_response_skip} });
        try encodeToCbor(&buffer.writer, EventPacket{ .header = EventHeader.fromEvent(.topic_body), .stage_name = test_context, .event = .{.topic_body = topic_body} });
        try encodeToCbor(&buffer.writer, EventPacket{ .header = EventHeader.fromEvent(.skip_topic_body), .stage_name = test_context, .event = .{.skip_topic_body = desc} });
        try encodeToCbor(&buffer.writer, EventPacket{ .header = EventHeader.fromEvent(.finish_topic_body), .stage_name = test_context, .event = .{.finish_topic_body = desc} });
        try encodeToCbor(&buffer.writer, EventPacket{ .header = EventHeader.fromEvent(.pending_finish_topic_body), .stage_name = test_context, .event = .pending_finish_topic_body });
        try encodeToCbor(&buffer.writer, EventPacket{ .header = EventHeader.fromEvent(.ready_generate), .stage_name = test_context, .event = .ready_generate });
        try encodeToCbor(&buffer.writer, EventPacket{ .header = EventHeader.fromEvent(.finish_generate), .stage_name = test_context, .event = .{.finish_generate = generate_response} });
        try encodeToCbor(&buffer.writer, EventPacket{ .header = EventHeader.fromEvent(.worker_response), .stage_name = test_context, .event = .{.worker_response = worker_response} });
        try encodeToCbor(&buffer.writer, EventPacket{ .header = EventHeader.fromEvent(.quit), .stage_name = test_context, .event = .quit });
        try encodeToCbor(&buffer.writer, EventPacket{ .header = EventHeader.fromEvent(.log), .stage_name = test_context, .event = .{.log = log} });

        var reader = CborStream.Reader.createFromSlice(buffer.written());

        ack: {
            var packet = try decodeFromCborInternal(allocator, &reader);
            defer packet.event.deinit(allocator);
            try std.testing.expectEqual(.ack, packet.header.tag());
            try std.testing.expectEqual({}, packet.header.ack);
            try std.testing.expectEqualStrings(test_context, packet.stage_name);
            try std.testing.expectEqualDeep({}, packet.event.ack);
            break:ack;
        }
        nack: {
            var packet = try decodeFromCborInternal(allocator, &reader);
            defer packet.event.deinit(allocator);
            try std.testing.expectEqual(.nack, packet.header.tag());
            try std.testing.expectEqual({}, packet.header.nack);
            try std.testing.expectEqualStrings(test_context, packet.stage_name);
            try std.testing.expectEqualDeep({}, packet.event.nack);
            break:nack;
        }
        heartbeat: {
            var packet = try decodeFromCborInternal(allocator, &reader);
            defer packet.event.deinit(allocator);
            try std.testing.expectEqual(.heartbeat, packet.header.tag());
            try std.testing.expectEqual({}, packet.header.heartbeat);
            try std.testing.expectEqualStrings(test_context, packet.stage_name);
            try std.testing.expectEqualDeep(heartbeat, packet.event.heartbeat);
            break:heartbeat;
        }
        probe_launching: {
            var packet = try decodeFromCborInternal(allocator, &reader);
            defer packet.event.deinit(allocator);
            try std.testing.expectEqual(.probe, packet.header.tag());
            try std.testing.expectEqual({}, packet.header.probe);
            try std.testing.expectEqualStrings(test_context, packet.stage_name);
            try std.testing.expectEqualDeep(.launching, packet.event.probe);
            break:probe_launching;
        }
        launching: {
            var packet = try decodeFromCborInternal(allocator, &reader);
            defer packet.event.deinit(allocator);
            try std.testing.expectEqual(.launching, packet.header.tag());
            try std.testing.expectEqual({}, packet.header.launching);
            try std.testing.expectEqualStrings(test_context, packet.stage_name);
            try std.testing.expectEqualDeep({}, packet.event.launching);
            break:launching;
        }
        launched: {
            var packet = try decodeFromCborInternal(allocator, &reader);
            defer packet.event.deinit(allocator);
            try std.testing.expectEqual(.launched, packet.header.tag());
            try std.testing.expectEqual({}, packet.header.launched);
            try std.testing.expectEqualStrings(test_context, packet.stage_name);
            try std.testing.expectEqualDeep({}, packet.event.launched);
            break:launched;
        }
        failure_launching: {
            var packet = try decodeFromCborInternal(allocator, &reader);
            defer packet.event.deinit(allocator);
            try std.testing.expectEqual(.failed_launching, packet.header.tag());
            try std.testing.expectEqual({}, packet.header.failed_launching);
            try std.testing.expectEqualStrings(test_context, packet.stage_name);
            try std.testing.expectEqualDeep({}, packet.event.failed_launching);
            break:failure_launching;
        }
        topic: {
            var packet = try decodeFromCborInternal(allocator, &reader);
            defer packet.event.deinit(allocator);
            try std.testing.expectEqual(.topic, packet.header.tag());
            try std.testing.expectEqual({}, packet.header.topic);
            try std.testing.expectEqualStrings(test_context, packet.stage_name);
            try std.testing.expectEqualDeep(topic, packet.event.topic);
            break:topic;
        }
        finish_topic: {
            var packet = try decodeFromCborInternal(allocator, &reader);
            defer packet.event.deinit(allocator);
            try std.testing.expectEqual(.finish_topic, packet.header.tag());
            try std.testing.expectEqual({}, packet.header.finish_topic);
            try std.testing.expectEqualStrings(test_context, packet.stage_name);
            try std.testing.expectEqualDeep({}, packet.event.finish_topic);
            break:finish_topic;
        }
        ready: {
            var packet = try decodeFromCborInternal(allocator, &reader);
            defer packet.event.deinit(allocator);
            try std.testing.expectEqual(.ready, packet.header.tag());
            try std.testing.expectEqual({}, packet.header.ready);
            try std.testing.expectEqualStrings(test_context, packet.stage_name);
            try std.testing.expectEqualDeep({}, packet.event.ready);
            break:ready;
        }
        ready_progress: {
            var packet = try decodeFromCborInternal(allocator, &reader);
            defer packet.event.deinit(allocator);
            try std.testing.expectEqual(.ready_progress, packet.header.tag());
            try std.testing.expectEqual({}, packet.header.ready_progress);
            try std.testing.expectEqualStrings(test_context, packet.stage_name);
            try std.testing.expectEqualDeep({}, packet.event.ready_progress);
            break:ready_progress;
        }
        ready_source_path: {
            var packet = try decodeFromCborInternal(allocator, &reader);
            defer packet.event.deinit(allocator);
            try std.testing.expectEqual(.ready_source_path, packet.header.tag());
            try std.testing.expectEqual({}, packet.header.ready_source_path);
            try std.testing.expectEqualStrings(test_context, packet.stage_name);
            try std.testing.expectEqualDeep({}, packet.event.ready_source_path);
            break:ready_source_path;
        }
        source_path: {
            var packet = try decodeFromCborInternal(allocator, &reader);
            defer packet.event.deinit(allocator);
            try std.testing.expectEqual(.source_path, packet.header.tag());
            try std.testing.expectEqual({}, packet.header.source_path);
            try std.testing.expectEqualStrings(test_context, packet.stage_name);
            try std.testing.expectEqualDeep(source_path, packet.event.source_path);
            break:source_path;
        }
        finish_source_path: {
            var packet = try decodeFromCborInternal(allocator, &reader);
            defer packet.event.deinit(allocator);
            try std.testing.expectEqual(.finish_source_path, packet.header.tag());
            try std.testing.expectEqual({}, packet.header.finish_source_path);
            try std.testing.expectEqualStrings(test_context, packet.stage_name);
            try std.testing.expectEqualDeep({}, packet.event.finish_source_path);
            break:finish_source_path;
        }
        ready_topic_body_progress: {
            var packet = try decodeFromCborInternal(allocator, &reader);
            defer packet.event.deinit(allocator);
            try std.testing.expectEqual(.ready_topic_body, packet.header.tag());
            try std.testing.expectEqual({}, packet.header.ready_topic_body);
            try std.testing.expectEqualStrings(test_context, packet.stage_name);
            try std.testing.expectEqualDeep(topic_body_response_progress, packet.event.ready_topic_body);
            break:ready_topic_body_progress;
        }
        ready_topic_body_success: {
            var packet = try decodeFromCborInternal(allocator, &reader);
            defer packet.event.deinit(allocator);
            try std.testing.expectEqual(.ready_topic_body, packet.header.tag());
            try std.testing.expectEqual({}, packet.header.ready_topic_body);
            try std.testing.expectEqualStrings(test_context, packet.stage_name);
            try std.testing.expectEqualDeep(topic_body_response_success, packet.event.ready_topic_body);
            break:ready_topic_body_success;
        }
        ready_topic_body_skip: {
            var packet = try decodeFromCborInternal(allocator, &reader);
            defer packet.event.deinit(allocator);
            try std.testing.expectEqual(.ready_topic_body, packet.header.tag());
            try std.testing.expectEqual({}, packet.header.ready_topic_body);
            try std.testing.expectEqualStrings(test_context, packet.stage_name);
            try std.testing.expectEqualDeep(topic_body_response_skip, packet.event.ready_topic_body);
            break:ready_topic_body_skip;
        }
        topic_body: {
            var packet = try decodeFromCborInternal(allocator, &reader);
            defer packet.event.deinit(allocator);
            try std.testing.expectEqual(.topic_body, packet.header.tag());
            try std.testing.expectEqual({}, packet.header.topic_body);
            try std.testing.expectEqualStrings(test_context, packet.stage_name);
            try std.testing.expectEqualDeep(topic_body, packet.event.topic_body);
            break:topic_body;
        }
        skip_topic_body: {
            var packet = try decodeFromCborInternal(allocator, &reader);
            defer packet.event.deinit(allocator);
            try std.testing.expectEqual(.skip_topic_body, packet.header.tag());
            try std.testing.expectEqual({}, packet.header.skip_topic_body);
            try std.testing.expectEqualStrings(test_context, packet.stage_name);
            try std.testing.expectEqualDeep(desc, packet.event.skip_topic_body);
            break:skip_topic_body;
        }
        finish_topic_body: {
            var packet = try decodeFromCborInternal(allocator, &reader);
            defer packet.event.deinit(allocator);
            try std.testing.expectEqual(.finish_topic_body, packet.header.tag());
            try std.testing.expectEqual({}, packet.header.finish_topic_body);
            try std.testing.expectEqualStrings(test_context, packet.stage_name);
            try std.testing.expectEqualDeep(desc, packet.event.finish_topic_body);
            break:finish_topic_body;
        }
        pending_finish_topic_body: {
            var packet = try decodeFromCborInternal(allocator, &reader);
            defer packet.event.deinit(allocator);
            try std.testing.expectEqual(.pending_finish_topic_body, packet.header.tag());
            try std.testing.expectEqual({}, packet.header.pending_finish_topic_body);
            try std.testing.expectEqualStrings(test_context, packet.stage_name);
            try std.testing.expectEqualDeep({}, packet.event.pending_finish_topic_body);
            break:pending_finish_topic_body;
        }
        ready_generate: {
            var packet = try decodeFromCborInternal(allocator, &reader);
            defer packet.event.deinit(allocator);
            try std.testing.expectEqual(.ready_generate, packet.header.tag());
            try std.testing.expectEqual({}, packet.header.ready_generate);
            try std.testing.expectEqualStrings(test_context, packet.stage_name);
            try std.testing.expectEqualDeep({}, packet.event.ready_generate);
            break:ready_generate;
        }
        finish_generate: {
            var packet = try decodeFromCborInternal(allocator, &reader);
            defer packet.event.deinit(allocator);
            try std.testing.expectEqual(.finish_generate, packet.header.tag());
            try std.testing.expectEqual({}, packet.header.finish_generate);
            try std.testing.expectEqualStrings(test_context, packet.stage_name);
            try std.testing.expectEqualDeep(generate_response, packet.event.finish_generate);
            break:finish_generate;
        }
        worker_response: {
            var packet = try decodeFromCborInternal(allocator, &reader);
            defer packet.event.deinit(allocator);
            try std.testing.expectEqual(.worker_response, packet.header.tag());
            try std.testing.expectEqual({}, packet.header.worker_response);
            try std.testing.expectEqualStrings(test_context, packet.stage_name);
            try std.testing.expectEqualDeep(worker_response, packet.event.worker_response);
            break:worker_response;
        }
        quit: {
            var packet = try decodeFromCborInternal(allocator, &reader);
            defer packet.event.deinit(allocator);
            try std.testing.expectEqual(.quit, packet.header.tag());
            try std.testing.expectEqual({}, packet.header.quit);
            try std.testing.expectEqualStrings(test_context, packet.stage_name);
            try std.testing.expectEqualDeep({}, packet.event.quit);
            break:quit;
        }
        log: {
            var packet = try decodeFromCborInternal(allocator, &reader);
            defer packet.event.deinit(allocator);
            try std.testing.expectEqual(.log, packet.header.tag());
            try std.testing.expectEqual({}, packet.header.log);
            try std.testing.expectEqualStrings(test_context, packet.stage_name);
            try std.testing.expectEqualDeep(log, packet.event.log);
            break:log;
        }
    }
};
