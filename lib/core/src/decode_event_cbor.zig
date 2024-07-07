const std = @import("std");

const c = @cImport({
    @cInclude("cbor/cbor.h");
});

const types = @import("./types.zig");
const Symbol = types.Symbol;
const CborStream = @import("./CborStream.zig");

pub fn encodeEvent(allocator: std.mem.Allocator, event: types.Event) ![]const u8 {
    var writer = try CborStream.Writer.init(allocator);
    defer writer.deinit();

    try encodeEventInternal(allocator, &writer, event);

    return writer.buffer.toOwnedSlice();
}

fn encodeEventInternal(allocator: std.mem.Allocator, writer: *CborStream.Writer, event: types.Event) !void {
    // _ = try writer.writeEnum(types.EventType, std.meta.activeTag(event));

    switch (event) {
        .ack => {},
        .nack => {},
        .launched => |payload| {
            _ = try writer.writeString(payload.stage_name);
        },
        .request_topic => {},
        .topic => |payload| {
            _ = try writer.writeSlice(Symbol, payload.names);
        },
        .begin_watch_path => {},
        .source_path => |payload| {
            _ = try writer.writeString(payload.name);
            _ = try writer.writeString(payload.path);
            _ = try writer.writeString(payload.hash);
            _ = try writer.writeUInt(usize, payload.item_count); 
        },
        .end_watch_path => {},
        // Topic body event
        .topic_body => |payload| { 
            _ = try writer.writeString(payload.header.name);
            _ = try writer.writeString(payload.header.path);
            _ = try writer.writeString(payload.header.hash);
            _ = try writer.writeUInt(usize, payload.header.item_count); 
            _ = try writer.writeUInt(usize, payload.index); 

            const TopicBodyItem = types.EventPayload.TopicBody.Item.Values;
            var bodies = try std.ArrayList(TopicBodyItem).initCapacity(allocator, payload.bodies.len);
            defer bodies.deinit();
            for (payload.bodies) |item| {
                try bodies.append(item.asTuple());
            }
            _ = try writer.writeSlice(TopicBodyItem, bodies.items);
        },
        .invalid_topic_body => |payload| {
            _ = try writer.writeString(payload.header.name);
            _ = try writer.writeString(payload.header.path);
            _ = try writer.writeString(payload.header.hash);
            _ = try writer.writeEnum(types.LogLevel, payload.log_level);
            _ = try writer.writeString(payload.log_from);
            _ = try writer.writeString(payload.log_content);
        },
        .ready_topic_body => {},
        .finish_topic_body => {},
        // Generation event
        .ready_generate => {},
        .finish_generate => {},
        // worker event
        .worker_result => |payload| {
            _ = try writer.writeString(payload.content);
        },
        // Finish event
        .quit_all => {},
        .quit_accept => |payload| {
            _ = try writer.writeString(payload.stage_name);
        },
        .quit => {},
        .log => |payload| {
            _ = try writer.writeEnum(types.LogLevel, payload.level);
            _ = try writer.writeString(payload.from);
            _ = try writer.writeString(payload.content);
        }
    }
}

pub fn decodeEvent(allocator: std.mem.Allocator, event_type: types.EventType, data: []const u8) !types.Event {
    var reader = CborStream.Reader.init(data);
    return decodeEventInternal(allocator, event_type, &reader);
}

fn decodeEventInternal(allocator: std.mem.Allocator, event_type: types.EventType, reader: *CborStream.Reader) !types.Event {
    switch (event_type) {
        .ack => return .ack,
        .nack => return .nack,
        .launched => {
            const stage_name = try reader.readString();

            return .{
                .launched = try types.EventPayload.Stage.init(allocator, stage_name),
            };
        },
        .request_topic => return .request_topic,
        .topic => {
            const topic_names = try reader.readSlice(allocator, Symbol);
            defer allocator.free(topic_names);

            return .{
                .topic = try types.EventPayload.Topic.init(allocator, topic_names),
            };
        },
        .begin_watch_path => return .begin_watch_path,
        .source_path => {
            const name = try reader.readString();
            const path = try reader.readString();
            const hash = try reader.readString();
            const item_count = try reader.readUInt(usize);

            return .{
                .source_path = try types.EventPayload.SourcePath.init(
                    allocator, name, path, hash, item_count
                ),
            };
        },
        .end_watch_path => return .end_watch_path,
        // Topic body event
        .topic_body => { 
            const name = try reader.readString();
            const path = try reader.readString();
            const hash = try reader.readString();
            const item_count = try reader.readUInt(usize);
            const item_index = try reader.readUInt(usize);

            const TopicBodyItem = types.EventPayload.TopicBody.Item.Values;
            const bodies = try reader.readSlice(allocator, TopicBodyItem);
            defer allocator.free(bodies);
            
            var payload = try types.EventPayload.TopicBody.init(
                allocator, 
                name, path, hash, bodies
            );

            return .{
                .topic_body = payload.withNewIndex(item_index, item_count),
            };
        },
        .invalid_topic_body => {
            const name = try reader.readString();
            const path = try reader.readString();
            const hash = try reader.readString();
            const level = try reader.readEnum(types.LogLevel);
            const from = try reader.readString();
            const content = try reader.readString();

            return .{
                .invalid_topic_body = try types.EventPayload.InvalidTopicBody.init(
                    allocator,
                    name, path, hash,
                    level, from, content
                ),
            };
        },
        .ready_topic_body => return .ready_topic_body,
        .finish_topic_body => return .finish_topic_body,
        // worker event
        .worker_result => {
            const content = try reader.readString();

            return .{
                .worker_result = try types.EventPayload.WorkerResult.init(allocator, content),
            };
        },
        // Generation event
        .ready_generate => return .ready_generate,
        .finish_generate => return .finish_generate,
        // Finish event
        .quit_all => return .quit_all,
        .quit => return .quit,
        .quit_accept => {
            const stage_name = try reader.readString();

            return .{
                .quit_accept = try types.EventPayload.Stage.init(allocator, stage_name),
            };
        },
        .log => {
            const level = try reader.readEnum(types.LogLevel);
            const from = try reader.readString();
            const content = try reader.readString();

            return .{
                .log = try types.EventPayload.Log.init(
                    allocator, level, from, content
                ),
            };
        }
    }
}

pub const __encodeEventInternal = encodeEventInternal;
pub const __decodeEventInternal = decodeEventInternal;

const TopicBodyView = struct {
    header: types.EventPayload.SourcePath,
    bodies: []const types.EventPayload.TopicBody.Item,

    fn from(a: std.mem.Allocator, topic_bosy: types.EventPayload.TopicBody) !TopicBodyView {
        return .{
            .header = try topic_bosy.header.clone(a),
            .bodies = topic_bosy.bodies,
        };
    }
    fn deinit(self: TopicBodyView) void {
        self.header.deinit();
    }
};

const test_context = "test-lib-core";

test "Encode/Decode event" {
    const allocator = std.testing.allocator;

    var writer = try CborStream.Writer.init(allocator);
    defer writer.deinit();

    const launched = try types.EventPayload.Stage.init(allocator, "Xyz");
    defer launched.deinit();
    const topic = try types.EventPayload.Topic.init(allocator, &.{"topic_a", "topic_b", "topic_c"});
    defer topic.deinit();
    const source_path = try types.EventPayload.SourcePath.init(allocator, "Some-name", "Some-path", "Some-content", 1);
    defer source_path.deinit();
    const topic_body = try types.EventPayload.TopicBody.init(allocator, source_path.name, source_path.path, source_path.hash, &.{
        .{ "topic_a", "topic_a_content" },
        .{ "topic_b", "topic_b_content" },
        .{ "topic_c", "topic_c_content" },
    });
    defer topic_body.deinit();
    const quit_accept = try types.EventPayload.Stage.init(allocator, "Qwerty");
    defer quit_accept.deinit();
    const log = try types.EventPayload.Log.init(allocator, .debug, test_context, "Test message😃");
    defer log.deinit();

    try encodeEventInternal(allocator, &writer, .ack);
    try encodeEventInternal(allocator, &writer, .nack);
    try encodeEventInternal(allocator, &writer, .{.launched = launched});
    try encodeEventInternal(allocator, &writer, .request_topic);
    try encodeEventInternal(allocator, &writer, .{.topic = topic});
    try encodeEventInternal(allocator, &writer, .begin_watch_path);
    try encodeEventInternal(allocator, &writer, .{.source_path = source_path});
    try encodeEventInternal(allocator, &writer, .{.topic_body = topic_body});
    try encodeEventInternal(allocator, &writer, .ready_topic_body);
    try encodeEventInternal(allocator, &writer, .ready_generate);
    try encodeEventInternal(allocator, &writer, .end_watch_path);
    try encodeEventInternal(allocator, &writer, .quit_all);
    try encodeEventInternal(allocator, &writer, .quit);
    try encodeEventInternal(allocator, &writer, .{.quit_accept = quit_accept});
    try encodeEventInternal(allocator, &writer, .{.log = log});

    const encoded = try writer.buffer.toOwnedSlice();
    defer allocator.free(encoded);

    var reader = CborStream.Reader.init(encoded);

    ack: {
        const event = try decodeEventInternal(allocator, .ack, &reader);
        try std.testing.expectEqual(.ack, event);
        break:ack;
    }
    nack: {
        const event = try decodeEventInternal(allocator, .nack, &reader);
        try std.testing.expectEqual(.nack, event);
        break:nack;
    }
    launched: {
        const event = try decodeEventInternal(allocator, .launched, &reader);
        defer event.deinit();
        try std.testing.expectEqualStrings(launched.stage_name, event.launched.stage_name);
        break:launched;
    }
    request_topic: {
        const event = try decodeEventInternal(allocator, .request_topic, &reader);
        try std.testing.expectEqual(.request_topic, event);
        break:request_topic;
    }
    topic: {
        const event = try decodeEventInternal(allocator, .topic, &reader);
        defer event.deinit();
        try std.testing.expectEqualDeep(topic, event.topic);
        break:topic;
    }
    begin_watch_path: {
        const event = try decodeEventInternal(allocator, .begin_watch_path, &reader);
        try std.testing.expectEqual(.begin_watch_path, event);
        break:begin_watch_path;
    }
    source_path: {
        const event = try decodeEventInternal(allocator, .source_path, &reader);
        defer event.deinit();
        try std.testing.expectEqualDeep(source_path, event.source_path);
        break:source_path;
    }
    topic_body: {
        const event = try decodeEventInternal(allocator, .topic_body, &reader);
        defer event.deinit();
        const lhs = try TopicBodyView.from(allocator, topic_body);
        defer lhs.deinit();
        const rhs = try TopicBodyView.from(allocator, event.topic_body);
        defer rhs.deinit();
        try std.testing.expectEqualDeep(lhs, rhs);
        break:topic_body;
    }
    ready_topic_body: {
        const event = try decodeEventInternal(allocator, .ready_topic_body, &reader);
        try std.testing.expectEqual(.ready_topic_body, event);
        break:ready_topic_body;
    }
    ready_generate: {
        const event = try decodeEventInternal(allocator, .ready_generate, &reader);
        try std.testing.expectEqual(.ready_generate, event);
        break:ready_generate;
    }
    end_watch_path: {
        const event = try decodeEventInternal(allocator, .end_watch_path, &reader);
        try std.testing.expectEqual(.end_watch_path, event);
        break:end_watch_path;
    }
    quit_all: {
        const event = try decodeEventInternal(allocator, .quit_all, &reader);
        try std.testing.expectEqual(.quit_all, event);
        break:quit_all;
    }
    quit: {
        const event = try decodeEventInternal(allocator, .quit, &reader);
        try std.testing.expectEqual(.quit, event);
        break:quit;
    }
    quit_accept: {
        const event = try decodeEventInternal(allocator, .quit_accept, &reader);
        defer event.deinit();
        try std.testing.expectEqualStrings(quit_accept.stage_name, event.quit_accept.stage_name);
        break:quit_accept;
    }
    log: {
        const event = try decodeEventInternal(allocator, .log, &reader);
        defer event.deinit();
        try std.testing.expectEqualStrings(log.from, event.log.from);
        break:log;
    }
}