const std = @import("std");

const core_types = @import("../types.zig");
const LogScope = core_types.LogScope;
const Symbol = core_types.Symbol;
const BinaryData = core_types.BinaryData;
const FilePath = core_types.FilePath;

const c = @import("omelet_c");

pub const StructView = @import("./event_impl.zig").StructView;
pub const EventPacket = @import("./event_impl.zig").EventPacket;

pub const LogLevel = enum(u8) {
    err = c.log_level_err,
    warn = c.log_level_warn,
    info = c.log_level_info,
    debug = c.log_level_debug,
    trace = c.log_level_trace,

    pub fn asText(self: LogLevel) Symbol {
        return switch (self) {
            .err => "ERROR",
            .warn => "WARN",
            .info => "INFO",
            .debug => "DEBUG",
            .trace => "TRACE",
        };
    }

    pub fn toStdLevel(self: LogLevel) std.log.Level {
        return switch (self) {
            .err => .err,
            .warn => .warn,
            .info => .info,
            .debug => .debug,
            .trace => .debug,
        };
    }

    pub fn resolveLogLevel(s: ?Symbol) ?LogLevel {
        if (s == null) return null;
        return std.meta.stringToEnum(LogLevel, s.?);
    }
};
pub const LogLevelSet = std.enums.EnumSet(LogLevel);

pub const UserTypeKind = enum(u8) {
    @"enum" = c.Enum,
    @"struct" = c.Struct,
    array = c.Array,
    primitive = c.Primitive,
    user = c.User,
    alias = c.Alias,
};

/// ChannelType
pub const ChannelType = enum {
    channel_command,
    channel_source,
    channel_generate,
};

pub const EventPhase = struct {
    kind: EventPhase.Kind,
    agreement: EventPhase.Agreement,

    pub const Kind = enum { launching, request, ready, terminating, quitting };
    pub const Agreement = enum { pending, confirmed };
};

/// Event types
pub const EventType = enum (u8) {
    // Response events
    ack = 1,
    nack,
    // periodically heartbeat
    heartbeat,
    probe,
    // Boot phase event
    launching,
    launched,
    failed_launching,
    // Topic request phase event
    topic,
    finish_topic,
    // Ready phase event
    ready,
    ready_progress,
    // Source path event
    ready_source_path,
    source_path,
    finish_source_path,
    // Topic body event
    ready_topic_body,
    topic_body,
    // Generate event
    finish_generate,
    // Other event
    quit,
    log,
    report_fatal,
    pending_fatal_quit,
    worker_response,
};

pub const EventHeader = @import("./event_impl.zig").EventHeader;

/// Event type options
pub const EventTypes = std.enums.EnumFieldStruct(EventType, bool, false);
pub const EventTypeSet = std.enums.EnumSet(EventType);

pub const TopicCategory = enum(u8) {
    source = c.category_source,
    schema = c.category_schema,
};

pub const ResponseTag = enum(u8) {
    progress = c.worker_progress,
    success = c.worker_result,
    skipped = c.worker_skipped,
};

const EventPayload = struct {
    pub const Heartbeat = struct {
        event_type: EventType,
        count: u64,

        pub fn init(view: StructView(Heartbeat)) Heartbeat {
            return .{
                .event_type = view[0],
                .count = view[1],
            };
        }
        pub fn deinit(_: *Heartbeat, _: std.mem.Allocator) void {}
        pub fn values(self: *const Heartbeat) StructView(Heartbeat) {
            return .{ self.event_type, self.count };
        }
    };

    pub const Topic = struct {
        category: TopicCategory,
        names: []const Symbol,

        pub fn init(view: StructView(Topic))Topic {
            return .{
                .category = view[0],
                .names = view[1],
            };
        }
        pub fn deinit(self: *Topic, allocator: std.mem.Allocator) void {
            allocator.free(self.names);
        }
        pub fn values(self: *const Topic) StructView(Topic) {
            return .{ self.category, self.names };
        }
    };

    pub const TopicBodyResponse = struct {
        desc: SourceDescriptor,
        hash: Symbol,
        name_alt: ?Symbol,
        response: Result,

        pub fn deinit(self: *TopicBodyResponse, allocator: std.mem.Allocator) void {
            self.response.deinit(allocator);
        }

        pub const Result = union(ResponseTag) {
            progress: usize,
            success: []const TopicBody.Encoded,
            skipped: void,

            pub fn deinit(self: *Result, allocator: std.mem.Allocator) void {
                switch (self.*) {
                    .success => |slice| allocator.free(slice),
                    .progress, .skipped => {},
                }
            }
        };
    };

    pub const TopicBody = struct {
        desc: SourceDescriptor,
        name_alt: ?Symbol,
        bodies: []const TopicBody.Encoded,

        pub fn deinit(self: *TopicBody, allocator: std.mem.Allocator) void {
            allocator.free(self.bodies);
        }

        pub const Encoded = struct {
            topic: Symbol,
            data: BinaryData,

            pub fn init(view: StructView(TopicBody.Encoded)) TopicBody.Encoded {
                return .{
                    .topic = view[0],
                    .data = view[1],
                };
            }

            pub fn values(self: *const Encoded) StructView(TopicBody.Encoded) {
                return .{ self.topic, self.data };
            }

            pub fn fromValuesSet(allocator: std.mem.Allocator, view: []const StructView(TopicBody.Encoded)) ![]const TopicBody.Encoded {
                const bodies = try allocator.alloc(TopicBody.Encoded, view.len);

                for (view, 0..) |v, i| {
                    bodies[i] = .init(v);
                }

                return bodies;
            }
        };

        pub const Support = struct {
            pub fn clone(allocator: std.mem.Allocator, self: *const TopicBody) !TopicBody {
                const desc: SourceDescriptor = .{
                    .category = self.desc.category,
                    .name = try allocator.dupe(u8, self.desc.name),
                    .dialect = try allocator.dupe(u8, self.desc.dialect),
                    .offset = self.desc.offset,
                }; 

                const bodies = try allocator.alloc(TopicBody.Encoded, self.bodies.len);
                for (self.bodies, 0..) |body, i| {
                    bodies[i] = .{
                        .topic = try allocator.dupe(u8, body.topic),
                        .data = try allocator.dupe(u8, body.data),
                    };
                }

                return .{
                    .desc = desc,
                    .name_alt = if (self.name_alt) |name| try allocator.dupe(u8, name) else null,
                    .bodies = bodies,
                };
            }

            pub fn release(allocator: std.mem.Allocator, self: * TopicBody) void {
                allocator.free(self.desc.name);
                allocator.free(self.desc.dialect);
                
                if (self.name_alt) |name| allocator.free(name);
                
                for (self.bodies) |body| {
                    allocator.free(body.topic);
                    allocator.free(body.data);
                }
                allocator.free(self.bodies);
            }
        };
    };

    pub const GenerateResponse = struct {
        desc: SourceDescriptor,
        status: GenerateResponse.Status,
        message: Symbol,

        pub const Status = enum { new_file, update_file, generate_failed };
    };

    pub const SourcePath = struct {
        category: TopicCategory,
        name: Symbol,
        path: FilePath,
        dialect: Symbol,
        hash: Symbol,

        pub fn init(view: StructView(SourcePath)) SourcePath {
            return .{
                .category = view[0],
                .name = view[1],
                .path = view[2],
                .dialect = view[3],
                .hash = view[4],
            };
        }
        pub fn deinit(_: *SourcePath, _: std.mem.Allocator) void {}

        pub fn values(self: *const SourcePath) StructView(SourcePath) {
            return .{ self.category, self.name, self.path, self.dialect, self.hash };
        }
    };

    pub const SourceDescriptor = struct {
        category: TopicCategory,
        name: Symbol,
        dialect: Symbol,
        offset: usize,

        pub fn init(view: StructView(SourceDescriptor)) SourceDescriptor {
            return .{
                .category = view[0],
                .name = view[1],
                .dialect = view[2],
                .offset = view[3],
            };
        }

        pub fn deinit(_: *SourceDescriptor) void {}

        pub fn values(self: *const SourceDescriptor) StructView(SourceDescriptor) {
            return .{ self.category, self.name, self.dialect, self.offset };
        }
    };

    pub const Log = struct {
        level: LogLevel,
        content: Symbol,

        pub fn init(view: StructView(Log)) Log {
            return .{
                .level = view[0],
                .content = view[1],
            };
        }
        pub fn deinit(_: *Log) void {}
        pub fn values(self: *const Log) StructView(Log) {
            return .{ self.level, self.content };
        }
    };
};

/// Event operation
pub const EventOperation = struct {
    pub const deinit = deinitEvent;
    // pub const clone = cloneEvent;
    pub fn tag(event: Event) std.meta.Tag(Event) {
        return std.meta.activeTag(event);
    }
};

/// Events
pub const Event = union(EventType) {
    // Response
    ack: void,
    nack: void,
    // periodically heartbeat
    heartbeat: Payload.Heartbeat,
    probe: EventPhase.Kind,
    // Boot phase event
    launching: void,
    launched: void,
    failed_launching: void,
    // Topic request phase event
    topic: Payload.Topic,
    finish_topic: void,
    // Ready phase event
    ready: void,
    ready_progress: void,
    // Source path event
    ready_source_path: void,
    source_path: Payload.SourcePath,
    finish_source_path: void,
    // Topic body events
    ready_topic_body: Payload.TopicBodyResponse,
    topic_body: Payload.TopicBody,
    // Generate events
    finish_generate: Payload.GenerateResponse,
    // Other event
    quit: void,
    log: Payload.Log,
    report_fatal: Payload.Log,
    pending_fatal_quit: void,
    worker_response: Symbol,

    pub const Payload = EventPayload;
    pub const deinit = deinitEvent;
    pub const tag = eventTag;
};

fn deinitEvent(event: *Event, allocator: std.mem.Allocator) void {
    switch (event.*) {
        // Response events
        .ack => {},
        .nack => {},
        // periodically heartbeat
        .heartbeat => {},
        .probe => {},
        // Boot phase event
        .launching => {},
        .launched => {},
        .failed_launching => {},
        // Topic request phase event
        .topic => |*data| data.deinit(allocator),
        .finish_topic => {},
        // Ready phase event
        .ready => {},
        .ready_progress => {},
        // Source path event
        .ready_source_path => {},
        .source_path => |*data| data.deinit(allocator),
        .finish_source_path => {},
        // Topic body events
        .ready_topic_body => |*data| data.deinit(allocator),
        .topic_body => |*data| data.deinit(allocator),
        // Generate events
        .finish_generate => {},
        // Other events
        .quit => {},
        .log => |*data| data.deinit(),
        .report_fatal => |*data| data.deinit(),
        .pending_fatal_quit => {},
        .worker_response => {},
    }
}

pub fn eventTag(event: *const Event) EventType {
    return std.meta.activeTag(event.*);
}
