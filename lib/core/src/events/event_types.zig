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

    // pub fn ofScope(self: LogLevel) LogScope {
    //     return switch (self) {
    //         .trace => .trace,
    //         else => .default,
    //     };
    // }
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
    pending_finish_source_path, //TODO:Deprecated?
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
};

pub const EventHeader = @import("./event_impl.zig").EventHeader;

/// Event type options
pub const EventTypes = std.enums.EnumFieldStruct(EventType, bool, false);
pub const EventTypeSet = std.enums.EnumSet(EventType);

pub const TopicCategory = enum {
    source,
    schema,
};

pub const ResponseTag = enum(u8) {
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
        response: Result,

        pub fn deinit(self: *TopicBodyResponse, allocator: std.mem.Allocator) void {
            self.response.deinit(allocator);
        }

        pub const Result = union(ResponseTag) {
            success: []const Encoded,
            skipped: void,

            pub fn deinit(self: *Result, allocator: std.mem.Allocator) void {
                switch (self.*) {
                    .success => |slice| allocator.free(slice),
                    .skipped => {},
                }
            }
        };

        pub const Encoded = struct {
            topic: Symbol,
            data: BinaryData,

            pub fn init(view: StructView(TopicBodyResponse.Encoded)) TopicBodyResponse.Encoded {
                return .{
                    .topic = view[0],
                    .data = view[1],
                };
            }

            pub fn values(self: *const Encoded) StructView(TopicBodyResponse.Encoded) {
                return .{ self.topic, self.data };
            }
        };
    };

    pub const TopicBody = struct {
        allocator: std.mem.Allocator,
        header: SourcePath,
        index: usize,
        bodies: []const Item,

        pub fn init(allocator: std.mem.Allocator, header: StructView(SourcePath), items: []const StructView(Item)) !@This() {
            const new_bodies = try allocator.alloc(Item, items.len);
            for (items, 0..) |item, i| {
                new_bodies[i] = try Item.init(item);
            }

            return .{
                .allocator = allocator,
                .header = SourcePath.init(header),
                .index = 0,
                .bodies = new_bodies,
            };
        }
        pub fn withNewIndex(self: *@This(), new_index: usize, new_count: usize) @This() {
            self.index = new_index;
            self.header.item_count = new_count;

            return self.*;
        }
        pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            self.header.deinit(allocator);
            allocator.free(self.bodies);
        }
        pub fn values(self: @This()) struct{StructView(SourcePath), usize, []const Item} {
            return .{ self.header.values(), self.index, self.bodies };
        }

        pub const Item = struct {
            topic: Symbol,
            content: Symbol,

            pub fn init(item: StructView(Item)) !@This() {
                return .{
                    .topic = item[0],
                    .content = item[1],
                };
            }
            pub fn deinit(_: @This(), _: std.mem.Allocator) void {}
            pub fn clone(self: @This(), allocator: std.mem.Allocator) !@This() {
                return Item.init(allocator, self.asTuple());
            }
            pub fn values(self: Item) StructView(@This()) {
                return .{self.topic, self.content};
            }
        };
    };

    // TODO: Deprecate
    pub const SkipTopicBody = struct {
        header: SourcePath,
        index: usize,

        pub fn init(header: StructView(SourcePath), index: usize) !@This() {
            return .{
                .header = SourcePath.init(header),
                .index = index,
            };
        }
        pub fn deinit(_: @This(), _: std.mem.Allocator) void {}
        pub fn clone(self: @This(), allocator: std.mem.Allocator) !@This() {
            return init(allocator, self.header.values(), self.index);
        }
        pub fn values(self: @This()) struct{StructView(SourcePath), usize} {
            return .{ self.header.values(), self.index };
        }
    };

    pub const SourcePath = struct {
        category: TopicCategory,
        name: Symbol,
        path: FilePath,
        dialect: Symbol,
        hash: Symbol,
        item_count: usize,

        pub fn init(view: StructView(SourcePath)) SourcePath {
            return .{
                .category = view[0],
                .name = view[1],
                .path = view[2],
                .dialect = view[3],
                .hash = view[4],
                .item_count = view[5],
            };
        }
        pub fn deinit(_: *SourcePath, _: std.mem.Allocator) void {}

        pub fn values(self: *const SourcePath) StructView(SourcePath) {
            return .{ self.category, self.name, self.path, self.dialect, self.hash, self.item_count };
        }
    };

    pub const SourceDescriptor = struct {
        name: Symbol,
        dialect: Symbol,
        offset: usize,

        pub fn init(view: StructView(SourceDescriptor)) SourceDescriptor {
            return .{
                .name = view[0],
                .dialect = view[1],
                .offset = view[2],
            };
        }

        pub fn values(self: *const SourceDescriptor) StructView(SourceDescriptor) {
            return .{ self.name, self.dialect, self.offset };
        }
    };

    // TODO: Deprecate
    pub const WorkerResponse = struct {
        allocator: std.mem.Allocator,
        content: Symbol,

        pub fn init(allocator: std.mem.Allocator, view: StructView(WorkerResponse)) !@This() {
            return .{
                .allocator = allocator,
                .content = try allocator.dupe(u8, view[0]),
            };
        }
        pub fn deinit(self: @This()) void {
            self.allocator.free(self.content);
        }
        pub fn clone(self: @This(), allocator: std.mem.Allocator) !@This() {
            return init(allocator, .{self.content});
        }
        pub fn values(self: @This()) StructView(@This()) {
            return .{ self.content };
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

    // finish_watch_path: void,
    // Source path event
    ready_source_path: void,
    source_path: Payload.SourcePath,
    pending_finish_source_path: void,
    finish_source_path: void,
    // Topic body events
    ready_topic_body: Payload.TopicBodyResponse,
    topic_body: Payload.TopicBody,
    skip_topic_body: Payload.SkipTopicBody,
    pending_finish_topic_body: void,
    finish_topic_body: void,
    // Generate events
    ready_generate: void,
    finish_generate: void,
    // Worker event
    worker_response: Payload.WorkerResponse,
    // Other event
    quit: void,
    log: Payload.Log,
    report_fatal: Payload.Log,
    pending_fatal_quit: void,

    pub const Payload = EventPayload;
    pub const deinit = deinitEvent;
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
        .pending_finish_source_path => {},
        .finish_source_path => {},

        // Topic body events
        .ready_topic_body => |*data| data.deinit(allocator),
        .topic_body => |*data| data.deinit(allocator),
        .skip_topic_body => |data| data.deinit(allocator),
        .pending_finish_topic_body => {},
        .finish_topic_body => {},
        // Generate events
        .ready_generate => {},
        .finish_generate => {},
        // Worker event
        .worker_response => |data| data.deinit(),
        // Other events
        .quit => {},
        .log => |*data| data.deinit(),
        .report_fatal => |*data| data.deinit(),
        .pending_fatal_quit => {},
    }
}

// TODO:
// pub fn cloneEvent(event: Event, allocator: std.mem.Allocator) !Event {
//     const cloned_event: Event = switch (event) {
//         // Response events
//         .ack => .ack,
//         .nack => .nack,
//         // periodically heartbeat
//         .heartbeat => .heartbeat,
//         // Boot phase event
//         .launching => .launching,
//         .probe_launching => .probe_launching,
//         .launched => .launched,
//         .failed_launching => .failed_launching,
//         // Request phase ebent
//         .request_topic => .request_topic,
//         .topic => |payload| .{.topic = try payload.clone(allocator)},
//         // Watch events
//         .ready_watch_path => .ready_watch_path,
//         .finish_watch_path => .finish_watch_path,
//         // Source path events
//         .ready_source_path => .ready_source_path,
//         .source_path => |payload| .{.source_path = try payload.clone(allocator)},
//         .pending_finish_source_path => .pending_finish_source_path,
//         .finish_source_path => .finish_source_path,
//         // Topic body events
//         .ready_topic_body => .ready_topic_body,
//         .topic_body => |payload| .{.topic_body = try payload.clone(allocator)},
//         .skip_topic_body => |payload| .{.skip_topic_body = try payload.clone(allocator)},
//         .pending_finish_topic_body => .pending_finish_topic_body,
//         .finish_topic_body => .finish_topic_body,
//         // Generate events
//         .ready_generate => .ready_generate,
//         .finish_generate => .finish_generate,
//         // Worker event
//         .worker_response => |payload| .{.worker_response = try payload.clone(allocator)},
//         // Other events
//         .quit => .quit,
//         .quit_all => .quit_all,
//         .quit_accept => .quit_accept,
//         .log => |payload| .{.log = try payload.clone(allocator)},
//         .report_fatal => |payload| .{.log = try payload.clone(allocator)},
//         .pending_fatal_quit => .pending_fatal_quit,
//     };

//     std.debug.assert(event.tag() == cloned_event.tag());

//     return cloned_event;
// }

// TODO:
// test "Clone events" {
//     const allocator = std.testing.allocator;
//     var buffer = std.Io.Writer.Allocating.init(allocator);
//     defer buffer.deinit();

//     topic: {
//         const expect_event: Event = .{
//             .topic = &.{
//                 .{ .category = .schema, .name = "foo" },
//                 .{ .category = .schema, .name = "bar" },
//                 .{ .category = .schema, .name = "baz" },
//             }
//         };
//         const event: Event = .{ .topic = try expect_event.topic.clone(&buffer.writer) };
//         defer event.deinit();
//         try std.testing.expectEqualDeep(expect_event, event);
//         break:topic;
//     }
//     source_path: {
//         const expect_event: Event = .{ .source_path = try Event.Payload.SourcePath.init(allocator, .{.schema, "name", "/path/to", "hash", 1}) };
//         defer expect_event.deinit();
//         const event = try expect_event.clone(std.heap.page_allocator);
//         defer event.deinit();
//         try std.testing.expectEqualDeep(expect_event.source_path.values(), event.source_path.values());
//         break:source_path;
//     }
//     topic_body: {
//         const expect_event: Event = .{ .topic_body = try Event.Payload.TopicBody.init(allocator,
//             .{.schema, "header/name", "header/path", "header/hash", 2},
//             &.{ .{"topic_1", "value_1"}, .{"topic_2", "value_3"}, .{"topic_99", "value_99"},  }
//         ) };
//         defer expect_event.deinit();
//         const event = try expect_event.clone(std.heap.page_allocator);
//         defer event.deinit();
//         try std.testing.expectEqualDeep(expect_event.topic_body.values(), event.topic_body.values());
//         break:topic_body;
//     }
//     skip_topic_body: {
//         const expect_event: Event = .{ .skip_topic_body = try Event.Payload.SkipTopicBody.init(allocator,
//             .{.schema, "header/name_i", "header/path_i", "header/hash_i", 3},
//             0,
//         ) };
//         defer expect_event.deinit();
//         const event = try expect_event.clone(std.heap.page_allocator);
//         defer event.deinit();
//         try std.testing.expectEqualDeep(expect_event.skip_topic_body.values(), event.skip_topic_body.values());
//         break:skip_topic_body;
//     }
//     worker_response: {
//         const expect_event: Event = .{ .worker_response = try Event.Payload.WorkerResponse.init(allocator, .{"some-worker-text"}) };
//         defer expect_event.deinit();
//         const event = try expect_event.clone(std.heap.page_allocator);
//         defer event.deinit();
//         try std.testing.expectEqualDeep(expect_event.worker_response.values(), event.worker_response.values());
//         break:worker_response;
//     }
//     log: {
//         const expect_event: Event = .{ .log = try Event.Payload.Log.init(allocator, .{.info, "log message"}) };
//         defer expect_event.deinit();
//         const event = try expect_event.clone(std.heap.page_allocator);
//         defer event.deinit();
//         try std.testing.expectEqualDeep(expect_event.log.values(), event.log.values());
//         break:log;
//     }
// }
