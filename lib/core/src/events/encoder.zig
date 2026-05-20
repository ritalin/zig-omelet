const std = @import("std");
const root = @import("../root.zig");
const impl = @import("./event_impl.zig");

const types = root.types;
const events = root.events;

const CborStream = @import("cbor").CborStream;
const EventHeader = impl.EventHeader;
const EventPacket = impl.EventPacket;
const StructView = impl.StructView;

const Event = root.events.Event;
const EventType = root.events.EventType;

pub fn encodeToCbor(writer: *std.Io.Writer, packet: EventPacket) !void {
    var cbor_writer = try CborStream.Writer.init(writer);
    defer cbor_writer.deinit();

    // TODO: needs to write EventHeader variant
    _ = try cbor_writer.writeString(@tagName(packet.header));
    _ = try cbor_writer.writeString(packet.stage_name);

    try encodePayload(&cbor_writer, packet.event);

    try writer.flush();
}

pub fn encodeSubscription(writer: *std.Io.Writer, subscription: EventHeader) !types.Symbol {
    writer.end = 0;

    var cbor_writer = try CborStream.Writer.init(writer);
    defer cbor_writer.deinit();

    const size = try cbor_writer.writeString(@tagName(subscription));
    return writer.buffer[0..size];
}

fn encodePayload(writer: *CborStream.Writer, event: Event) !void {
    switch (event) {
        .ack => {},
        .nack => {},
        .launched, .failed_launching => {},
        .request_topic => {},
        .topic => |payload| {
            _ = try writer.writeTuple(StructView(Event.Payload.Topic), payload.values());
        },
        .finish_topic => {},
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
        .skip_topic_body => |payload| {
            _ = try writer.writeTuple(StructView(Event.Payload.SourcePath), payload.header.values());
            _ = try writer.writeUInt(usize, payload.index);
        },
        .pending_finish_topic_body => {},
        .finish_topic_body => {},
        // Generate event
        .ready_generate => {},
        .finish_generate => {},
        // Worker event
        .worker_response => |payload| {
            _ = try writer.writeString(payload.content);
        },
        // Other event
        .quit_all => {},
        .quit_accept => {},
        .quit => {},
        .log, .report_fatal => |payload| {
            _ = try writer.writeTuple(StructView(Event.Payload.Log), payload.values());
        },
        .pending_fatal_quit => {},
    }
}
