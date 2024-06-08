const std = @import("std");

//
// Channel endpoints
//

pub const CHANNEL_ROOT = "/tmp/duckdb-ext-ph";

/// (control) Client -> Server
pub const CMD_C2S_END_POINT = std.fmt.comptimePrint("ipc://{s}_cmd_c2s", .{CHANNEL_ROOT});
/// (control) Server -> Client
pub const CMD_S2C_END_POINT = std.fmt.comptimePrint("ipc://{s}_cmd_s2c", .{CHANNEL_ROOT});
/// (source) Client -> Server
pub const SRC_C2S_END_POINT = std.fmt.comptimePrint("ipc://{s}_src_c2s", .{CHANNEL_ROOT});
/// (source) Client -> Server
pub const SRC_S2C_END_POINT = std.fmt.comptimePrint("ipc://{s}_src_s2c", .{CHANNEL_ROOT});

/// ChannelType
pub const ChannelType = enum {
    channel_command,
    channel_source,
    channel_generate,
};

pub const Symbol = []const u8;

/// Event types
pub const EventType = enum (u8) {
    launched = 1,
    begin_topic,
    topic,
    end_topic,
    begin_session,
    source,
    topic_payload,
    next_generate,
    end_generate,
    finishd,
    quit,
    quit_accept,
};
/// Event type options
pub const EventTypes = std.enums.EnumFieldStruct(EventType, bool, false);

/// Events
pub const Event = union(EventType) {
    launched: void,
    begin_topic: void,
    topic: struct { name: Symbol },
    end_topic: void,
    begin_session: void,
    source: struct { path: Symbol, hash: Symbol },
    topic_payload: struct { topic: Symbol, payload: Symbol },
    begin_generate: void,
    end_generate: void,
    quit: void,
    quit_accept: void,
};