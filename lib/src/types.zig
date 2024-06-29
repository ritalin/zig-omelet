const std = @import("std");

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

pub const StageState = enum {booting, ready, terminating};

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

pub const FilePath = []const u8;
pub const Symbol = []const u8;

/// Event types
pub const EventType = enum (u8) {
    ack = 1,
    nack,
    launched,
    request_topic,
    topic,
    // watch
    begin_watch_path,
    source_path,
    end_watch_path,
    // Topic body event
    topic_body,
    invalid_topic_body,
    ready_topic_body,
    finish_topic_body,
    // Generation event
    ready_generate,
    finish_generate,
    // worker event
    worker_result,
    // Finish event
    quit_all,
    quit,
    quit_accept,
    log,
};
/// Event type options
pub const EventTypes = std.enums.EnumFieldStruct(EventType, bool, false);

pub const EventPayload = struct {
    pub const Stage = struct {
        allocator: std.mem.Allocator,
        stage_name: Symbol,

        pub fn init(allocator: std.mem.Allocator, name: Symbol) !@This() {
            return .{
                .allocator = allocator,
                .stage_name = try allocator.dupe(u8, name),
            };
        }
        pub fn deinit(self: @This()) void {
            self.allocator.free(self.stage_name);
        }
        pub fn clone(self: @This(), allocator: std.mem.Allocator) !@This() {
            return init(allocator, self.stage_name);
        }
    };
    pub const Topic = struct {
        arena: *std.heap.ArenaAllocator,
        names: []const Symbol,

        pub fn init(allocator: std.mem.Allocator, names: []const Symbol) !@This() {
            const arena = try allocator.create(std.heap.ArenaAllocator);
            arena.* = std.heap.ArenaAllocator.init(allocator);
            const a = arena.allocator();
            
            const new_names = try a.alloc(Symbol, names.len);
            for (names, 0..) |name, i| {
                new_names[i] = try a.dupe(u8, name);
            }

            return .{
                .arena = arena,
                .names = new_names,
            };
        }
        pub fn deinit(self: @This()) void {
            self.arena.deinit();
            self.arena.child_allocator.destroy(self.arena);
        }
        pub fn clone(self: @This(), allocator: std.mem.Allocator) !@This() {
            return init(allocator, self.names);
        }
    };
    pub const TopicBody = struct {
        arena: *std.heap.ArenaAllocator,
        header: EventPayload.SourcePath, 
        index: usize,
        bodies: []const Item,

        pub fn init(allocator: std.mem.Allocator, name: Symbol, path: FilePath, hash: Symbol, items: []const Item.Values) !@This() {
            var arena = try allocator.create(std.heap.ArenaAllocator);
            arena.* = std.heap.ArenaAllocator.init(allocator);
            const a = arena.allocator();
                
            const new_bodies = try a.alloc(Item, items.len);
            for (items, 0..) |item, i| {
                new_bodies[i] = try Item.init(a, item);
            }
        
            return .{
                .arena = arena,
                .header = try SourcePath.init(a, name, path, hash, 1),
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
            self.arena.deinit();
            self.arena.child_allocator.destroy(self.arena);
        }
        pub fn clone(self: @This(), allocator: std.mem.Allocator) !@This() {
            var arena = try allocator.create(std.heap.ArenaAllocator);
            arena.* = std.heap.ArenaAllocator.init(allocator);
            const a = arena.allocator();
                
            const new_bodies = try a.alloc(Item, self.bodies.len);
            for (self.bodies, 0..) |item, i| {
                new_bodies[i] = try Item.init(a, item.asTuple());
            }
        
            return .{
                .arena = arena,
                .header = try self.header.clone(a),
                .index = self.index,
                .bodies = new_bodies,
            };                
        }

        pub const Item = struct {
            topic: Symbol, 
            content: Symbol,

            pub const Values = std.meta.Tuple(&.{Symbol, Symbol});

            pub fn init(allocator: std.mem.Allocator, item: Values) !@This() {
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
            pub fn asTuple(self: Item) Values {
                return .{self.topic, self.content};
            }
        };
    };
    pub const InvalidTopicBody = struct {
        allocator: std.mem.Allocator,
        header: EventPayload.SourcePath, 
        log_level: LogLevel,
        log_content: Symbol,

        pub fn init(allocator: std.mem.Allocator, name: Symbol, path: FilePath, hash: Symbol, log_level: LogLevel, log_content: Symbol) !@This() {
            return .{
                .allocator = allocator,
                .header = try SourcePath.init(allocator, name, path, hash, 1),
                .log_level = log_level,
                .log_content = try allocator.dupe(u8, log_content),
            };
        }
        pub fn deinit(self: @This()) void {
            self.header.deinit();
            self.allocator.free(self.log_content);
        }
        pub fn clone(self: @This(), allocator: std.mem.Allocator) !@This() {
            return init(
                allocator, 
                self.header.name, self.header.path, self.header.hash, 
                self.log_level, self.log_content
            );
        }
    };
    pub const SourcePath = struct {
        allocator: std.mem.Allocator,
        name: Symbol, 
        path: FilePath, 
        hash: Symbol,
        item_count: usize,

        pub fn init(allocator: std.mem.Allocator, name: Symbol, path: FilePath, hash: Symbol, item_count: usize) !@This() {
            return .{
                .allocator = allocator,
                .name = try allocator.dupe(u8, name),
                .path = try allocator.dupe(u8, path),
                .hash = try allocator.dupe(u8, hash),
                .item_count = item_count,
            };
        }
        pub fn deinit(self: @This()) void {
            self.allocator.free(self.name);
            self.allocator.free(self.path);
            self.allocator.free(self.hash);
        }
        pub fn clone(self: @This(), allocator: std.mem.Allocator) !@This() {
            return SourcePath.init(
                allocator, 
                self.name, self.path, self.hash, 
                self.item_count
            );
        }
    };
    pub const WorkerResult = struct {
        allocator: std.mem.Allocator,
        content: Symbol,

        pub fn init(allocator: std.mem.Allocator, content: Symbol) !@This() {
            return .{
                .allocator = allocator,
                .content = try allocator.dupe(u8, content),
            };
        }
        pub fn deinit(self: @This()) void {
            self.allocator.free(self.content);
        }
        pub fn clone(self: @This(), allocator: std.mem.Allocator) !@This() {
            return init(allocator, self.content);
        }
    };
    pub const Log = struct {
        allocator: std.mem.Allocator,
        level: LogLevel, 
        content: Symbol,

        pub fn init(allocator: std.mem.Allocator, level: LogLevel, content: Symbol) !@This() {
            return .{
                .allocator = allocator,
                .level = level,
                .content = try allocator.dupe(u8, content),
            };
        }
        pub fn deinit(self: @This()) void {
            self.allocator.free(self.content);
        }
        pub fn clone(self: @This(), allocator: std.mem.Allocator) !@This() {
            return init(allocator, self.level, self.content);
        }
    };
};

/// Events
pub const Event = union(EventType) {
    // Response
    ack,
    nack,
    // Booting events
    launched: EventPayload.Stage,
    request_topic: void,
    topic: EventPayload.Topic,
    // watch
    begin_watch_path: void,
    source_path: EventPayload.SourcePath,
    end_watch_path: void,
    // Topic body events
    topic_body: EventPayload.TopicBody,
    invalid_topic_body: EventPayload.InvalidTopicBody,
    ready_topic_body: void,
    finish_topic_body: void,
    // Generation events
    ready_generate: void,
    finish_generate: void,
    // worker event
    worker_result: EventPayload.WorkerResult,
    // Finish events
    quit_all: void,
    quit: void,
    quit_accept: EventPayload.Stage,
    log: EventPayload.Log,

    pub fn deinit(self: Event) void {
        // std.debug.print("[DEBUG] Brgin dealloc event: {}\n", .{std.meta.activeTag(self)});
        switch (self) {
            .launched => |payload| payload.deinit(),
            .topic => |payload| payload.deinit(),
            .source_path => |payload| payload.deinit(),
            .topic_body => |payload| payload.deinit(),
            .invalid_topic_body => |payload| payload.deinit(),
            .quit_accept => |payload| payload.deinit(),
            .worker_result => |payload| payload.deinit(),
            .log => |payload| payload.deinit(),
            .ack,
            .nack,
            .request_topic, 
            .begin_watch_path, 
            .end_watch_path, 
            .ready_topic_body,
            .finish_topic_body,
            .ready_generate,
            .finish_generate,
            .quit_all, 
            .quit => {},
        }
        // std.debug.print("[DEBUG] End dealloc event\n", .{});
    }

    pub fn clone(self: Event, allocator: std.mem.Allocator) !Event {
        tag_only: {
            switch (self) {
                .launched => |payload| {
                    return .{
                        .launched = try payload.clone(allocator),
                    };
                },
                .topic => |payload| {
                    return .{
                        .topic = try payload.clone(allocator), 
                    };
                },
                .source_path => |path| {
                    return .{
                        .source_path = try path.clone(allocator),
                    };
                },
                .topic_body => |payload| {
                    return .{
                        .topic_body = try payload.clone(allocator),
                    };
                },
                .invalid_topic_body => |payload| {
                    return .{
                        .invalid_topic_body = try payload.clone(allocator),
                    };
                },
                .worker_result => |payload| {
                    return .{
                        .worker_result = try payload.clone(allocator),
                    };
                },
                .quit_accept => |payload| {
                    return .{
                        .quit_accept = try payload.clone(allocator),
                    };
                },
                .log => |payload| {
                    return .{
                        .log = try payload.clone(allocator),
                    };
                },
                .ack,
                .nack,
                .request_topic, 
                .begin_watch_path, 
                .end_watch_path, 
                .ready_topic_body,
                .finish_topic_body,
                .ready_generate,
                .finish_generate, 
                .quit_all, 
                .quit => break :tag_only,
            }   
        }

        return self; 
    }

    pub fn tag(self: Event) EventType {
        return std.meta.activeTag(self);
    }
};