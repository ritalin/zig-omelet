const std = @import("std");

pub const IPC_PROTOCOL = "ipc://";
// IPC channel root directory
pub const CHANNEL_ROOT = "/tmp/duckdb-ext-ph";

//
// Channel endpoints
//
pub const REQ_PORT = "req_c2s";
pub const PUBSUB_PORT = "cmd_s2c";

pub const Endpoints = struct {
    req_rep: Symbol,
    pub_sub: Symbol,
};

// pub const StageState = enum {booting, ready, terminating};

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
pub const LogLevelSet = std.enums.EnumSet(LogLevel);

/// ChannelType
pub const ChannelType = enum {
    channel_command,
    channel_source,
    channel_generate,
};

pub const FilePath = []const u8;
pub const Symbol = []const u8;

/// Event types
pub const EventType = enum (u8) {
    // Response events
    ack = 1,
    nack,
    // Boot events
    launched,
    failed_launching,
    request_topic,
    topic,
    // watch event
    ready_watch_path,
    finish_watch_path,
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
    // Other event
    worker_result,
    quit_all,
    quit,
    quit_accept,
    log,
};
/// Event type options
pub const EventTypes = std.enums.EnumFieldStruct(EventType, bool, false);
pub const EventTypeSet = std.enums.EnumSet(EventType);

const ExceptStructView = std.StaticStringMap(void).initComptime(.{
    .{@typeName(std.mem.Allocator)},
    .{@typeName(*std.heap.ArenaAllocator)},
});

pub fn StructView(comptime T: type) type {
    comptime std.debug.assert(@typeInfo(T) == .Struct);

    const fields = std.meta.fields(T);
    comptime var i: usize = 0;
    comptime var types: [fields.len]type = undefined;
    inline for (fields) |field| {
        if (comptime (!ExceptStructView.has(@typeName(field.type)))) {
            defer i += 1;
            types[i] = field.type;
        }
    }

    return std.meta.Tuple(types[0..i]);
}

pub const TopicCategory = enum {
    source, 
    schema,
};

const EventPayload = struct {
    pub const Topic = struct {
        arena: *std.heap.ArenaAllocator,
        category: TopicCategory,
        names: []const Symbol,
        has_more: bool,

        pub fn init(allocator: std.mem.Allocator, category: TopicCategory, names: []const Symbol, has_more: bool) !@This() {
            const arena = try allocator.create(std.heap.ArenaAllocator);
            arena.* = std.heap.ArenaAllocator.init(allocator);
            const a = arena.allocator();
            
            const new_names = try a.alloc(Symbol, names.len);
            for (names, 0..) |name, i| {
                new_names[i] = try a.dupe(u8, name);
            }

            return .{
                .arena = arena,
                .category = category,
                .names = new_names,
                .has_more = has_more,
            };
        }
        pub fn deinit(self: @This()) void {
            self.arena.deinit();
            self.arena.child_allocator.destroy(self.arena);
        }
        pub fn clone(self: @This(), allocator: std.mem.Allocator) !@This() {
            return init(allocator, self.category, self.values(), self.has_more);
        }
        pub fn values(self: @This()) []const Symbol {
            return self.names;
        }
    };

    pub const TopicBody = struct {
        allocator: std.mem.Allocator,
        header: SourcePath, 
        index: usize,
        bodies: []const Item,

        pub fn init(allocator: std.mem.Allocator, header: StructView(SourcePath), items: []const StructView(Item)) !@This() {
            const new_bodies = try allocator.alloc(Item, items.len);
            for (items, 0..) |item, i| {
                new_bodies[i] = try Item.init(allocator, item);
            }
        
            return .{
                .allocator = allocator,
                .header = try SourcePath.init(allocator, header),
                .index = 1,
                .bodies = new_bodies,
            };
        }
        pub fn withNewIndex(self: *@This(), new_index: usize, new_count: usize) @This() {
            self.index = new_index;
            self.header.item_count = new_count;

            return self.*;
        }
        pub fn deinit(self: @This()) void {
            self.header.deinit();
            deinitBodies(self.bodies, self.allocator);
        }
        fn deinitBodies(bodies: []const Item, allocator: std.mem.Allocator) void {
            for (bodies) |item| {
                item.deinit(allocator);
            }
            allocator.free(bodies);
        }
        pub fn clone(self: @This(), allocator: std.mem.Allocator) !@This() {
            const new_bodies = try allocator.alloc(Item, self.bodies.len);
            for (self.bodies, 0..) |item, i| {
                new_bodies[i] = try Item.init(allocator, item.values());
            }
        
            return .{
                .allocator = allocator,
                .header = try SourcePath.init(allocator, self.header.values()),
                .index = 1,
                .bodies = new_bodies,
            };
        }
        pub fn values(self: @This()) struct{StructView(SourcePath), usize, []const Item} {
            return .{ self.header.values(), self.index, self.bodies };
        }

        pub const Item = struct {
            topic: Symbol, 
            content: Symbol,

            pub fn init(allocator: std.mem.Allocator, item: StructView(Item)) !@This() {
                return .{
                    .topic = try allocator.dupe(u8, item.@"0"),
                    .content = try allocator.dupe(u8, item.@"1"),
                };
            }
            pub fn deinit(self: @This(), allocator: std.mem.Allocator) void {
                allocator.free(self.topic);
                allocator.free(self.content);
            }
            pub fn clone(self: @This(), allocator: std.mem.Allocator) !@This() {
                return Item.init(allocator, self.asTuple());
            }
            pub fn values(self: Item) StructView(@This()) {
                return .{self.topic, self.content};
            }
        };
    };

    pub const SkipTopicBody = struct {
        header: SourcePath, 
        log: Log,

        pub fn init(allocator: std.mem.Allocator, header: StructView(SourcePath), log: StructView(Log)) !@This() {
            return .{
                .header = try SourcePath.init(allocator, header),
                .log = try Log.init(allocator, log),
            };
        }
        pub fn deinit(self: @This()) void {
            self.header.deinit();
            self.log.deinit();
        }
        pub fn clone(self: @This(), allocator: std.mem.Allocator) !@This() {
            return init(allocator, self.header.values(), self.log.values());
        }
        pub fn values(self: @This()) struct{StructView(SourcePath), StructView(Log)} {
            return .{ self.header.values(), self.log.values() };
        }
    };


    pub const SourcePath = struct {
        allocator: std.mem.Allocator,
        category: TopicCategory,
        name: Symbol, 
        path: FilePath, 
        hash: Symbol,
        item_count: usize,

        pub fn init(allocator: std.mem.Allocator, view: StructView(SourcePath)) !@This() {
            return .{
                .allocator = allocator,
                .category = view[0],
                .name = try allocator.dupe(u8, view[1]),
                .path = try allocator.dupe(u8, view[2]),
                .hash = try allocator.dupe(u8, view[3]),
                .item_count = view[4],
            };
        }
        pub fn deinit(self: @This()) void {
            self.allocator.free(self.name);
            self.allocator.free(self.path);
            self.allocator.free(self.hash);
        }
        pub fn clone(self: @This(), allocator: std.mem.Allocator) !@This() {
            return init(allocator, self.values());
        }
        pub fn values(self: @This()) StructView(@This()) {
            return .{ self.category, self.name, self.path, self.hash, self.item_count };
        }
    };

    pub const WorkerResult = struct {
        allocator: std.mem.Allocator,
        content: Symbol,

        pub fn init(allocator: std.mem.Allocator, view: StructView(WorkerResult)) !@This() {
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
        allocator: std.mem.Allocator,
        level: LogLevel,
        content: Symbol,

        pub fn init(allocator: std.mem.Allocator, view: StructView(Log)) !@This() {
            return .{
                .allocator = allocator,
                .level = view[0],
                .content = try allocator.dupe(u8, view[1]),
            };
        }
        pub fn deinit(self: @This()) void {
            self.allocator.free(self.content);
        }
        pub fn clone(self: @This(), allocator: std.mem.Allocator) !@This() {
            return init(allocator, self.values());
        }
        pub fn values(self: @This()) StructView(@This()) {
            return .{ self.level, self.content };
        }
    };
};

/// Event operation 
pub const EventOperation = struct {
    pub const deinit = deinitEvent;
    pub const clone = cloneEvent;
    pub fn tag(event: Event) std.meta.Tag(Event) {
        return std.meta.activeTag(event);
    }
};

/// Events
pub const Event = union(EventType) {
    // Response
    ack: void,
    nack: void,
    // Boot events
    launched: void,
    failed_launching: void,
    request_topic: void,
    topic: Payload.Topic,
    // Watch event
    ready_watch_path: void,
    finish_watch_path: void,
    // Source path event
    ready_source_path: void,
    source_path: Payload.SourcePath,
    pending_finish_source_path: void,
    finish_source_path: void,
    // Topic body events
    ready_topic_body: void,
    topic_body: Payload.TopicBody,
    skip_topic_body: Payload.SkipTopicBody,
    pending_finish_topic_body: void,
    finish_topic_body: void,
    // Generate events
    ready_generate: void,
    finish_generate: void,
    // Other event
    worker_result: Payload.WorkerResult,
    quit_all: void,
    quit: void,
    quit_accept: void,
    log: Payload.Log,

    pub const Payload = EventPayload;
    pub usingnamespace EventOperation;
};

fn deinitEvent(event: Event) void {
    switch (event) {
        // Response events
        .ack => {},
        .nack => {},
        // Boot events
        .launched => {},
        .failed_launching => {},
        .request_topic => {},
        .topic => |data| data.deinit(),
        // Watch event
        .ready_watch_path => {},
        .finish_watch_path => {},
        // Source path event
        .ready_source_path => {},
        .source_path => |data| data.deinit(),
        .pending_finish_source_path => {},
        .finish_source_path => {},
        // Topic body events
        .ready_topic_body => {},
        .topic_body => |data| data.deinit(),
        .skip_topic_body => |data| data.deinit(),
        .pending_finish_topic_body => {},
        .finish_topic_body => {},
        // Generate events
        .ready_generate => {},
        .finish_generate => {},
        // Other events
        .worker_result => |data| data.deinit(),
        .quit_all => {},
        .quit => {},
        .quit_accept => {},
        .log => |data| data.deinit(),
    }
}
pub fn cloneEvent(event: Event, allocator: std.mem.Allocator) !Event {
    const cloned_event: Event = switch (event) {
        // Response events
        .ack => .ack,
        .nack => .nack,
        // Boot events
        .launched => .launched,
        .failed_launching => .failed_launching,
        .request_topic => .request_topic,
        .topic => |payload| .{.topic = try payload.clone(allocator)},
        // Watch events
        .ready_watch_path => .ready_watch_path,
        .finish_watch_path => .finish_watch_path,
        // Source path events
        .ready_source_path => .ready_source_path,
        .source_path => |payload| .{.source_path = try payload.clone(allocator)},
        .pending_finish_source_path => .pending_finish_source_path,
        .finish_source_path => .finish_source_path,
        // Topic body events
        .ready_topic_body => .ready_topic_body,
        .topic_body => |payload| .{.topic_body = try payload.clone(allocator)},
        .skip_topic_body => |payload| .{.skip_topic_body = try payload.clone(allocator)},
        .pending_finish_topic_body => .pending_finish_topic_body,
        .finish_topic_body => .finish_topic_body,
        // Generate events
        .ready_generate => .ready_generate,
        .finish_generate => .finish_generate,
        // Other events
        .worker_result => |payload| .{.worker_result = try payload.clone(allocator)},
        .quit => .quit,
        .quit_all => .quit_all,
        .quit_accept => .quit_accept,
        .log => |payload| .{.log = try payload.clone(allocator)},
    };

    std.debug.assert(event.tag() == cloned_event.tag());

    return cloned_event; 
}

test "Clone events" {
    const allocator = std.testing.allocator;

    topic: {
        const expect_event: Event = .{ .topic = try Event.Payload.Topic.init(allocator, .schema, &.{"foo", "bar", "baz"}, false) };
        defer expect_event.deinit();
        const event = try expect_event.clone(std.heap.page_allocator);
        defer event.deinit();
        try std.testing.expectEqualDeep(expect_event.topic.values(), event.topic.values());
        break:topic;
    }
    source_path: {
        const expect_event: Event = .{ .source_path = try Event.Payload.SourcePath.init(allocator, .{.schema, "name", "/path/to", "hash", 1}) };
        defer expect_event.deinit();
        const event = try expect_event.clone(std.heap.page_allocator);
        defer event.deinit();
        try std.testing.expectEqualDeep(expect_event.source_path.values(), event.source_path.values());
        break:source_path;
    }
    topic_body: {
        const expect_event: Event = .{ .topic_body = try Event.Payload.TopicBody.init(allocator, 
            .{.schema, "header/name", "header/path", "header/hash", 2}, 
            &.{ .{"topic_1", "value_1"}, .{"topic_2", "value_3"}, .{"topic_99", "value_99"},  }
        ) };
        defer expect_event.deinit();
        const event = try expect_event.clone(std.heap.page_allocator);
        defer event.deinit();
        try std.testing.expectEqualDeep(expect_event.topic_body.values(), event.topic_body.values());
        break:topic_body;
    }
    skip_topic_body: {
        const expect_event: Event = .{ .skip_topic_body = try Event.Payload.SkipTopicBody.init(allocator, 
            .{.schema, "header/name_i", "header/path_i", "header/hash_i", 3},
            .{.err, "error message"}
        ) };
        defer expect_event.deinit();
        const event = try expect_event.clone(std.heap.page_allocator);
        defer event.deinit();
        try std.testing.expectEqualDeep(expect_event.skip_topic_body.values(), event.skip_topic_body.values());
        break:skip_topic_body;
    }
    worker_result: {
        const expect_event: Event = .{ .worker_result = try Event.Payload.WorkerResult.init(allocator, .{"some-result-text"}) };
        defer expect_event.deinit();
        const event = try expect_event.clone(std.heap.page_allocator);
        defer event.deinit();
        try std.testing.expectEqualDeep(expect_event.worker_result.values(), event.worker_result.values());
        break:worker_result;
    }
    log: {
        const expect_event: Event = .{ .log = try Event.Payload.Log.init(allocator, .{.info, "log message"}) };
        defer expect_event.deinit();
        const event = try expect_event.clone(std.heap.page_allocator);
        defer event.deinit();
        try std.testing.expectEqualDeep(expect_event.log.values(), event.log.values());
        break:log;
    }
} 
