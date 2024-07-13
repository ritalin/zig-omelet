const std = @import("std");

const c = @cImport({
    @cInclude("cbor/cbor.h");
});

const types = @import("./types.zig");
const StructView = types.StructView;
const Symbol = types.Symbol;
const Event = types.Event;
const CborStream = @import("./CborStream.zig");

pub fn encodeEvent(allocator: std.mem.Allocator, event: Event) ![]const u8 {
    var writer = try CborStream.Writer.init(allocator);
    defer writer.deinit();

    try encodeEventInternal(allocator, &writer, event);

    return writer.buffer.toOwnedSlice();
}

fn encodeEventInternal(allocator: std.mem.Allocator, writer: *CborStream.Writer, event: Event) !void {
    _ = allocator;

    switch (event) {
        .ack => {},
        .nack => {},
        .launched, .failed_launching => {},
        .request_topic => {},
        .topic => |payload| {
            _ = try writer.writeSlice(Symbol, payload.names);
        },
        // Watch event
        .ready_watch_path => {},
        .finish_watch_path => {},
        // Source path event
        .ready_source_path => {},
        .source_path => |payload| {
            _ = try writer.writeTuple(StructView(Event.Payload.SourcePath), payload.values());
        },
        .pending_finish_source_path => {},
        .finish_source_path => {},
        // Topic body event
        .ready_topic_body => {},
        .topic_body => |payload| { 
            _ = try writer.writeTuple(StructView(Event.Payload.SourcePath), payload.header.values());
            _ = try writer.writeUInt(usize, payload.index); 

            _ = try writer.writeSliceHeader(payload.bodies.len);

            for (payload.bodies) |item| {
                _ = try writer.writeTuple(StructView(Event.Payload.TopicBody.Item), item.values());
            }
        },
        .invalid_topic_body => |payload| {
            _ = try writer.writeTuple(StructView(Event.Payload.SourcePath), payload.header.values());
            _ = try writer.writeTuple(StructView(Event.Payload.Log), payload.log.values());
        },
        .pending_finish_topic_body => {},
        .finish_topic_body => {},
        // Generate event
        .ready_generate => {},
        .finish_generate => {},
        // Other event
        .worker_result => |payload| {
            _ = try writer.writeString(payload.content);
        },
        .quit_all => {},
        .quit_accept => {},
        .quit => {},
        .log => |payload| {
            _ = try writer.writeTuple(StructView(Event.Payload.Log), payload.values());
        }
    }
}

pub fn decodeEvent(allocator: std.mem.Allocator, event_type: types.EventType, data: []const u8) !types.Event {
    var reader = CborStream.Reader.init(data);
    return decodeEventInternal(allocator, event_type, &reader);
}

fn decodeEventInternal(allocator: std.mem.Allocator, event_type: types.EventType, reader: *CborStream.Reader) !types.Event {
    switch (event_type) {
        // Response events
        .ack => return .ack,
        .nack => return .nack,
        // Boot events
        .launched => return .launched,
        .failed_launching => return .failed_launching,
        .request_topic => return .request_topic,
        .topic => {
            const topic_names = try reader.readSlice(allocator, Symbol);
            defer allocator.free(topic_names);

            return .{
                .topic = try Event.Payload.Topic.init(allocator, topic_names),
            };
        },
        // Watch event
        .ready_watch_path => return .ready_watch_path,
        .finish_watch_path => return .finish_watch_path,
        // Source path event
        .ready_source_path => return .ready_source_path,
        .source_path => {
            const path = try reader.readTuple(StructView(Event.Payload.SourcePath));

            return .{
                .source_path = try Event.Payload.SourcePath.init(allocator, path),
            };
        },
        .pending_finish_source_path => return .pending_finish_source_path,
        .finish_source_path => return .finish_source_path,
        // Topic body event
        .ready_topic_body => return .ready_topic_body,
        .topic_body => { 
            const header = try reader.readTuple(StructView(Event.Payload.SourcePath));
            const item_index = try reader.readUInt(usize);

            const bodies = try reader.readSlice(allocator, StructView(Event.Payload.TopicBody.Item));
            defer allocator.free(bodies);
            
            var payload = try Event.Payload.TopicBody.init(allocator, header, bodies);

            return .{
                .topic_body = payload.withNewIndex(item_index, payload.header.item_count),
            };
        },
        .invalid_topic_body => {
            const header = try reader.readTuple(StructView(Event.Payload.SourcePath));
            const log = try reader.readTuple(StructView(Event.Payload.Log));

            return .{
                .invalid_topic_body = try Event.Payload.InvalidTopicBody.init(allocator, header, log),
            };
        },
        .pending_finish_topic_body => return .pending_finish_topic_body,
        .finish_topic_body => return .finish_topic_body,
        // Generation event
        .ready_generate => return .ready_generate,
        .finish_generate => return .finish_generate,
        // Other event
        .worker_result => {
            const content = try reader.readString();

            return .{
                .worker_result = try Event.Payload.WorkerResult.init(allocator, .{content}),
            };
        },
        .quit_all => return .quit_all,
        .quit => return .quit,
        .quit_accept => return .quit_accept,
        .log => {
            const log = try reader.readTuple(StructView(Event.Payload.Log));

            return .{
                .log = try Event.Payload.Log.init(allocator, log),
            };
        }
    }
}

pub const __encodeEventInternal = encodeEventInternal;
pub const __decodeEventInternal = decodeEventInternal;

const test_context = "test-lib-core";

test "Encode/Decode event" {
    const allocator = std.testing.allocator;

    var writer = try CborStream.Writer.init(allocator);
    defer writer.deinit();

    const topic = try Event.Payload.Topic.init(allocator, &.{"topic_a", "topic_b", "topic_c"});
    defer topic.deinit();
    const source_path = try Event.Payload.SourcePath.init(allocator, .{"Some-name", "Some-path", "Some-content", 1});
    defer source_path.deinit();
    const topic_body = try Event.Payload.TopicBody.init(allocator, 
        .{ source_path.name, source_path.path, source_path.hash, 2 }, 
        &.{
            .{ "topic_a", "topic_a_content" },
            .{ "topic_b", "topic_b_content" },
            .{ "topic_c", "topic_c_content" },
        }
    );
    defer topic_body.deinit();
    const invalid_topic_body = try Event.Payload.InvalidTopicBody.init(allocator,
        .{ source_path.name, source_path.path, source_path.hash, 3 }, 
        .{.err, "SQL syntax error"}
    );
    defer invalid_topic_body.deinit();
    const worker_result = try Event.Payload.WorkerResult.init(allocator, .{"some-result-text"});
    defer worker_result.deinit();
    const log = try Event.Payload.Log.init(allocator, .{.debug, "Test message😃"});
    defer log.deinit();

    try encodeEventInternal(allocator, &writer, .ack);
    try encodeEventInternal(allocator, &writer, .nack);
    try encodeEventInternal(allocator, &writer, .launched);
    try encodeEventInternal(allocator, &writer, .failed_launching);
    try encodeEventInternal(allocator, &writer, .request_topic);
    try encodeEventInternal(allocator, &writer, .{.topic = topic});
    try encodeEventInternal(allocator, &writer, .ready_watch_path);
    try encodeEventInternal(allocator, &writer, .finish_watch_path);
    try encodeEventInternal(allocator, &writer, .ready_source_path);
    try encodeEventInternal(allocator, &writer, .{.source_path = source_path});
    try encodeEventInternal(allocator, &writer, .pending_finish_source_path);
    try encodeEventInternal(allocator, &writer, .finish_source_path);
    try encodeEventInternal(allocator, &writer, .ready_topic_body);
    try encodeEventInternal(allocator, &writer, .{.topic_body = topic_body});
    try encodeEventInternal(allocator, &writer, .{.invalid_topic_body = invalid_topic_body});
    try encodeEventInternal(allocator, &writer, .finish_topic_body);
    try encodeEventInternal(allocator, &writer, .pending_finish_topic_body);
    try encodeEventInternal(allocator, &writer, .ready_generate);
    try encodeEventInternal(allocator, &writer, .finish_generate);
    try encodeEventInternal(allocator, &writer, .{.worker_result = worker_result});
    try encodeEventInternal(allocator, &writer, .quit_all);
    try encodeEventInternal(allocator, &writer, .quit);
    try encodeEventInternal(allocator, &writer, .quit_accept);
    try encodeEventInternal(allocator, &writer, .{.log = log});

    const encoded = try writer.buffer.toOwnedSlice();
    defer allocator.free(encoded);

    var reader = CborStream.Reader.init(encoded);

    ack: {
        const event = try decodeEventInternal(allocator, .ack, &reader);
        defer event.deinit();
        try std.testing.expectEqual(.ack, event);
        break:ack;
    }
    nack: {
        const event = try decodeEventInternal(allocator, .nack, &reader);
        defer event.deinit();
        try std.testing.expectEqual(.nack, event);
        break:nack;
    }
    launched: {
        const event = try decodeEventInternal(allocator, .launched, &reader);
        defer event.deinit();
        try std.testing.expectEqual(.launched, event);
        break:launched;
    }
    failure_launching: {
        const event = try decodeEventInternal(allocator, .failed_launching, &reader);
        defer event.deinit();
        try std.testing.expectEqual(.failed_launching, event);
        break:failure_launching;
    }
    request_topic: {
        const event = try decodeEventInternal(allocator, .request_topic, &reader);
        defer event.deinit();
        try std.testing.expectEqual(.request_topic, event);
        break:request_topic;
    }
    topic: {
        const event = try decodeEventInternal(allocator, .topic, &reader);
        defer event.deinit();
        try std.testing.expectEqualDeep(topic, event.topic);
        break:topic;
    }
    ready_watch_path: {
        const event = try decodeEventInternal(allocator, .ready_watch_path, &reader);
        defer event.deinit();
        try std.testing.expectEqual(.ready_watch_path, event);
        break:ready_watch_path;
    }
    finish_watch_path: {
        const event = try decodeEventInternal(allocator, .finish_watch_path, &reader);
        defer event.deinit();
        try std.testing.expectEqual(.finish_watch_path, event);
        break:finish_watch_path;
    }
    ready_source_path: {
        const event = try decodeEventInternal(allocator, .ready_source_path, &reader);
        defer event.deinit();
        try std.testing.expectEqual(.ready_source_path, event);
        break:ready_source_path;
    }
    source_path: {
        const event = try decodeEventInternal(allocator, .source_path, &reader);
        defer event.deinit();
        try std.testing.expectEqualDeep(source_path, event.source_path);
        break:source_path;
    }
    finish_source_path: {
        const event = try decodeEventInternal(allocator, .finish_source_path, &reader);
        defer event.deinit();
        try std.testing.expectEqual(.finish_source_path, event);
        break:finish_source_path;
    }
    pending_finish_source_path: {
        const event = try decodeEventInternal(allocator, .pending_finish_source_path, &reader);
        defer event.deinit();
        try std.testing.expectEqual(.pending_finish_source_path, event);
        break:pending_finish_source_path;
    }
    ready_topic_body: {
        const event = try decodeEventInternal(allocator, .ready_topic_body, &reader);
        defer event.deinit();
        try std.testing.expectEqual(.ready_topic_body, event);
        break:ready_topic_body;
    }
    topic_body: {
        const event = try decodeEventInternal(allocator, .topic_body, &reader);
        defer event.deinit();
        try std.testing.expectEqualDeep(topic_body.values(), event.topic_body.values());
        break:topic_body;
    }
    invalid_topic_body: {
        const event = try decodeEventInternal(allocator, .invalid_topic_body, &reader);
        defer event.deinit();
        try std.testing.expectEqualDeep(invalid_topic_body.values(), event.invalid_topic_body.values());
        break:invalid_topic_body;
    }
    finish_topic_body: {
        const event = try decodeEventInternal(allocator, .finish_topic_body, &reader);
        defer event.deinit();
        try std.testing.expectEqual(.finish_topic_body, event);
        break:finish_topic_body;
    }
    pending_finish_topic_body: {
        const event = try decodeEventInternal(allocator, .pending_finish_topic_body, &reader);
        defer event.deinit();
        try std.testing.expectEqual(.pending_finish_topic_body, event);
        break:pending_finish_topic_body;
    }
    ready_generate: {
        const event = try decodeEventInternal(allocator, .ready_generate, &reader);
        defer event.deinit();
        try std.testing.expectEqual(.ready_generate, event);
        break:ready_generate;
    }
    finish_generate: {
        const event = try decodeEventInternal(allocator, .finish_generate, &reader);
        defer event.deinit();
        try std.testing.expectEqual(.finish_generate, event);
        break:finish_generate;
    }
    worker_result: {
        const event = try decodeEventInternal(allocator, .worker_result, &reader);
        defer event.deinit();
        try std.testing.expectEqualDeep(worker_result.values(), event.worker_result.values());
        break:worker_result;
    }
    quit_all: {
        const event = try decodeEventInternal(allocator, .quit_all, &reader);
        defer event.deinit();
        try std.testing.expectEqual(.quit_all, event);
        break:quit_all;
    }
    quit: {
        const event = try decodeEventInternal(allocator, .quit, &reader);
        defer event.deinit();
        try std.testing.expectEqual(.quit, event);
        break:quit;
    }
    quit_accept: {
        const event = try decodeEventInternal(allocator, .quit_accept, &reader);
        defer event.deinit();
        try std.testing.expectEqual(.quit_accept, event);
        break:quit_accept;
    }
    log: {
        const event = try decodeEventInternal(allocator, .log, &reader);
        defer event.deinit();
        try std.testing.expectEqualDeep(log.values(), event.log.values());
        break:log;
    }
}