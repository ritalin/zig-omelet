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
pub const REQ_C2S_END_POINT = std.fmt.comptimePrint("ipc://{s}_req_c2s", .{CHANNEL_ROOT});

pub const LogScope = enum {
    trace, default,
};

pub const LogLevel = enum {
    err,
    warn,
    info,
    debug,
    trace,

    pub fn toStdLevel(self: LogLevel) std.log.Level {
        return switch (self) {
            .err => .err,
            .warn => .warn,
            .info => .info,
            .debug => .debug,
            .trace => .debug,
        };
    }

    pub fn ofScope(self: LogLevel) LogScope {
        return switch (self) {
            .trace => .trace,
            else => .default,
        };
    }
};

/// ChannelType
pub const ChannelType = enum {
    channel_command,
    channel_source,
    channel_generate,
};

pub const Symbol = []const u8;

/// Event types
pub const EventType = enum (u8) {
    ack = 1,
    nack,
    launched,
    begin_topic,
    topic,
    end_topic,
    begin_session,
    source,
    topic_payload,
    next_generate,
    end_generate,
    finished,
    quit_all,
    quit,
    quit_accept,
    log,
};
/// Event type options
pub const EventTypes = std.enums.EnumFieldStruct(EventType, bool, false);

/// Events
pub const Event = union(EventType) {
    ack,
    nack,
    launched: struct { stage_name: Symbol },
    begin_topic: void,
    topic: struct { name: Symbol },
    end_topic: void,
    begin_session: void,
    source: struct { name: Symbol, path: Symbol, hash: Symbol },
    topic_payload: struct { topic: Symbol, content: Symbol },
    next_generate: void,
    end_generate: void,
    finished: void,
    quit_all: void,
    quit: void,
    quit_accept: struct { stage_name: Symbol },
    log: struct { level: LogLevel, content: Symbol },

    pub fn deinit(self: Event, allocator: std.mem.Allocator) void {
        // std.debug.print("[DEBUG] Brgin dealloc event: {}\n", .{std.meta.activeTag(self)});
        switch (self) {
            .launched => |payload| {
                allocator.free(payload.stage_name);
            },
            .topic => |payload| {
                allocator.free(payload.name);
            },
            .source => |payload| {
                allocator.free(payload.name);
                allocator.free(payload.path);
                allocator.free(payload.hash);
            },
            .topic_payload => |payload| {
                allocator.free(payload.topic);
                allocator.free(payload.content);
            },
            .log => |payload| {
                allocator.free(payload.content);
            },
            .ack,
            .nack,
            .begin_topic, 
            .end_topic, 
            .begin_session, 
            .next_generate, 
            .end_generate, 
            .finished, 
            .quit_all, 
            .quit, 
            .quit_accept => {}
        }
        // std.debug.print("[DEBUG] End dealloc event\n", .{});
    }

    pub fn clone(self: Event, allocator: std.mem.Allocator) !Event {
        tag_only: {
            switch (self) {
                .launched => |payload| {
                    return .{
                        .launched = .{ 
                            .stage_name = try allocator.dupe(u8, payload.stage_name) 
                        }
                    };
                },
                .topic => |payload| {
                    return .{
                        .topic = .{ 
                            .name = try allocator.dupe(u8, payload.name)
                        }
                    };
                },
                .source => |payload| {
                    return .{
                        .source = .{ 
                            .name = try allocator.dupe(u8, payload.name),
                            .path = try allocator.dupe(u8, payload.path),
                            .hash = try allocator.dupe(u8, payload.hash),
                        }
                    };
                },
                .topic_payload => |payload| {
                    return .{
                        .topic_payload = .{ 
                            .topic = try allocator.dupe(u8, payload.topic),
                            .content = try allocator.dupe(u8, payload.content),
                        }
                    };
                },
                .log => |payload| {
                    return .{
                        .log = .{ 
                            .level = payload.level,
                            .content = try allocator.dupe(u8, payload.content),
                        }
                    };
                },
                .ack,
                .nack,
                .begin_topic, 
                .end_topic, 
                .begin_session, 
                .next_generate, 
                .end_generate, 
                .finished, 
                .quit_all, 
                .quit, 
                .quit_accept => break :tag_only,
            }   
        }

        return self; 
    }
};