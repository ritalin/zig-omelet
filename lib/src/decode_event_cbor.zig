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
    _ = try writer.writeEnum(types.EventType, std.meta.activeTag(event));

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
        },
        .end_watch_path => {},
        // Topic body event
        .topic_body => |payload| { 
            _ = try writer.writeString(payload.header.name);
            _ = try writer.writeString(payload.header.path);
            _ = try writer.writeString(payload.header.hash);

            const TopicBodyItem = types.EventPayload.TopicBody.Item.Values;
            var bodies = try std.ArrayList(TopicBodyItem).initCapacity(allocator, payload.bodies.len);
            defer bodies.deinit();
            for (payload.bodies) |item| {
                try bodies.append(item.asTuple());
            }
            _ = try writer.writeSlice(TopicBodyItem, bodies.items);
        },
        .ready_topic_body => {},
        .finish_topic_body => {},
        // Generation event
        .ready_generate => {},
        .finish_generate => {},
        // Finish event
        .quit_all => {},
        .quit_accept => |payload| {
            _ = try writer.writeString(payload.stage_name);
        },
        .quit => {},
        .log => |payload| {
            _ = try writer.writeEnum(types.LogLevel, payload.level);
            _ = try writer.writeString(payload.content);
        }
    }
}

pub fn decodeEvent(allocator: std.mem.Allocator, data: []const u8) !types.Event {
    var reader = CborStream.Reader.init(data);
    return decodeEventInternal(allocator, &reader);
}

fn decodeEventInternal(allocator: std.mem.Allocator, reader: *CborStream.Reader) !types.Event {
    switch (try reader.readEnum(types.EventType)) {
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

            return .{
                .topic = try types.EventPayload.Topic.init(allocator, topic_names),
            };
        },
        .begin_watch_path => return .begin_watch_path,
        .source_path => {
            const name = try reader.readString();
            const path = try reader.readString();
            const hash = try reader.readString();

            return .{
                .source_path = try types.EventPayload.SourcePath.init(
                    allocator, name, path, hash
                ),
            };
        },
        .end_watch_path => return .end_watch_path,
        // Topic body event
        .topic_body => { 
            const name = try reader.readString();
            const path = try reader.readString();
            const hash = try reader.readString();

            const TopicBodyItem = types.EventPayload.TopicBody.Item.Values;
            const bodies = try reader.readSlice(allocator, TopicBodyItem);
            defer allocator.free(bodies);
            
            return .{
                .topic_body = try types.EventPayload.TopicBody.init(
                    allocator, 
                    try types.EventPayload.SourcePath.init(allocator, name, path, hash),
                    bodies,
                ),
            };
        },
        .ready_topic_body => return .ready_topic_body,
        .finish_topic_body => return .finish_topic_body,
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
            const content = try reader.readString();

            return .{
                .log = try types.EventPayload.Log.init(
                    allocator, level, content
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

test "Encode/Decode event" {
    const test_allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(test_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var writer = try CborStream.Writer.init(allocator);
    defer writer.deinit();

    const launched = try types.EventPayload.Stage.init(allocator, "Xyz");
    defer launched.deinit();
    const topic = try types.EventPayload.Topic.init(allocator, &.{"topic_a", "topic_b", "topic_c"});
    defer topic.deinit();
    const source_path = try types.EventPayload.SourcePath.init(allocator, "Some-name", "Some-path", "Some-content");
    defer source_path.deinit();
    const topic_body = try types.EventPayload.TopicBody.init(allocator, source_path, &.{
        .{ "topic_a", "topic_a_content" },
        .{ "topic_b", "topic_b_content" },
        .{ "topic_c", "topic_c_content" },
    });
    defer topic_body.deinit();
    const quit_accept = try types.EventPayload.Stage.init(allocator, "Qwerty");
    defer quit_accept.deinit();
    const log = try types.EventPayload.Log.init(allocator, .debug, "Test message😃");
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
    try encodeEventInternal(allocator, &writer, .finished);
    try encodeEventInternal(allocator, &writer, .quit_all);
    try encodeEventInternal(allocator, &writer, .quit);
    try encodeEventInternal(allocator, &writer, .{.quit_accept = quit_accept});
    try encodeEventInternal(allocator, &writer, .{.log = log});

    const encoded = try writer.buffer.toOwnedSlice();
    defer allocator.free(encoded);

    var reader = CborStream.Reader.init(encoded);
    var event: types.Event = undefined;

    event = try decodeEventInternal(allocator, &reader);
    try std.testing.expectEqual(.ack, event);

    event = try decodeEventInternal(allocator, &reader);
    try std.testing.expectEqual(.nack, event);

    event = try decodeEventInternal(allocator, &reader);
    try std.testing.expectEqualStrings(launched.stage_name, event.launched.stage_name);

    event = try decodeEventInternal(allocator, &reader);
    try std.testing.expectEqual(.request_topic, event);

    event = try decodeEventInternal(allocator, &reader);
    try std.testing.expectEqualDeep(topic, event.topic);

    event = try decodeEventInternal(allocator, &reader);
    try std.testing.expectEqual(.begin_watch_path, event);

    event = try decodeEventInternal(allocator, &reader);
    try std.testing.expectEqualDeep(source_path, event.source_path);

    event = try decodeEventInternal(allocator, &reader);
    try std.testing.expectEqualDeep(try TopicBodyView.from(allocator, topic_body), try TopicBodyView.from(allocator, event.topic_body));

    event = try decodeEventInternal(allocator, &reader);
    try std.testing.expectEqual(.ready_topic_body, event);

    event = try decodeEventInternal(allocator, &reader);
    try std.testing.expectEqual(.ready_generate, event);

    event = try decodeEventInternal(allocator, &reader);
    try std.testing.expectEqual(.end_watch_path, event);

    event = try decodeEventInternal(allocator, &reader);
    try std.testing.expectEqual(.finished, event);

    event = try decodeEventInternal(allocator, &reader);
    try std.testing.expectEqual(.quit_all, event);

    event = try decodeEventInternal(allocator, &reader);
    try std.testing.expectEqual(.quit, event);

    event = try decodeEventInternal(allocator, &reader);
    try std.testing.expectEqualStrings(quit_accept.stage_name, event.quit_accept.stage_name);

    event = try decodeEventInternal(allocator, &reader);
    try std.testing.expectEqualDeep(log, event.log);
}