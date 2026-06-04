const std = @import("std");

const core = @import("core");

const Symbol = core.types.Symbol;

pub const CacheManager = struct {
    pub fn Payload(comptime app_context: Symbol) type {
        _ = app_context;

        return struct {
            arena: *std.heap.ArenaAllocator,
            cache: std.StringHashMap(*EntryGroup),
            topics_map: TopicsMap,
            ready_queue: core.Queue(core.Event.Payload.TopicBody),

            const Self = @This();

            pub const CacheStatus = enum {
                expired, missing, fulfil
            };

            pub fn init(allocator: std.mem.Allocator) !Self {
                const arena = try allocator.create(std.heap.ArenaAllocator);
                arena.* = std.heap.ArenaAllocator.init(allocator);

                const managed_allocator = arena.allocator();
                return .{
                    .arena = arena,
                    .topics_map = TopicsMap.init(managed_allocator),
                    .cache = std.StringHashMap(*EntryGroup).init(managed_allocator),
                    .ready_queue = core.Queue(core.Event.Payload.TopicBody).init(managed_allocator),
                };
            }

            pub fn deinit(self: *Self) void {
                const child = self.arena.child_allocator;

                self.ready_queue.deinit();
                self.topics_map.deinit();
                self.cache.deinit();
                self.arena.deinit();
                
                child.destroy(self.arena);
            }

            pub fn addNewEntryGroup(self: *Self, source: core.Event.Payload.SourcePath) !bool {
                const allocator = self.arena.allocator();

                const path = try allocator.dupe(u8, source.path);
                defer allocator.free(path);
                const entry = try self.cache.getOrPut(path);

                if (entry.found_existing) {
                    if (! entry.value_ptr.*.isExpired(source.hash)) return false;
                    entry.value_ptr.*.deinit();
                }
                
                entry.value_ptr.* = try EntryGroup.init(allocator, source, self.topics_map.get(source.category));
                entry.key_ptr.* = entry.value_ptr.*.source.path;

                return true;
            }

            pub fn update(self: *Self, topic_body: core.Event.Payload.TopicBody) !CacheStatus {
                if (self.cache.get(topic_body.header.path)) |group| {
                    if (group.isExpired(topic_body.header.hash)) return .expired;

                    const entry = try group.fetchEntry(topic_body.index, topic_body.header);
                    return entry.update(topic_body.bodies);
                }

                return .expired;
            }

            pub fn ready(self: *Self, path: core.Event.Payload.SourcePath, index: usize) !bool {
                if (self.cache.get(path.path)) |group| {
                    defer {
                        if (group.left_offsets.count() == 0) {
                            _ = self.cache.remove(path.path);
                            group.deinit();
                        }
                    }

                    if (group.popEntry(index)) |entry| {
                        defer entry.deinit(group.allocator);

                        const a = self.arena.allocator();

                        const bodies = try a.alloc(core.StructView(core.Event.Payload.TopicBody.Item), entry.contents.count());
                        defer a.free(bodies);
                        
                        var it = entry.contents.iterator();
                        var i: usize = 0;

                        while (it.next()) |content| {
                            bodies[i] = .{content.key_ptr.*, content.value_ptr.*};
                            i += 1;
                        }

                        try self.ready_queue.enqueue(
                            try core.Event.Payload.TopicBody.init(a, entry.header.values(), bodies)
                        );

                        return true;
                    }
                }

                return false;
            }

            pub fn dismiss(self: *Self, path: core.Event.Payload.SourcePath, index: usize) !void {
                if (self.cache.get(path.path)) |group| {
                    defer {
                        if (group.left_offsets.count() == 0) {
                            _ = self.cache.remove(path.path);
                            group.deinit();
                        }
                    }

                    try group.ensureExpand(path.item_count);

                    if (group.popEntry(index)) |entry| {
                        entry.deinit(group.allocator);   
                    }
                }
            }

            pub fn isEmpty(self: Self) bool {
                return (self.cache.count() == 0) and (self.ready_queue.count() == 0);
            }

            const EntryGroup = struct {
                allocator: std.mem.Allocator, 
                source: core.Event.Payload.SourcePath,
                topics: std.BufSet,
                entries: std.ArrayList(?*Entry),
                left_offsets: std.AutoHashMap(usize, bool),

                pub fn init(allocator: std.mem.Allocator, source: core.Event.Payload.SourcePath, topics: *std.BufSet) !*EntryGroup {
                    const self = try allocator.create(EntryGroup);
                    self.* =  .{
                        .allocator = allocator,
                        .source = try source.clone(allocator),
                        .topics = try topics.cloneWithAllocator(allocator),
                        .entries = std.ArrayList(?*Entry).init(allocator),
                        .left_offsets = std.AutoHashMap(usize, bool).init(allocator),
                    };

                    return self;
                }

                pub fn deinit(self: *EntryGroup) void {
                    for (self.entries.items) |entry| {
                        if (entry) |x| {
                            x.deinit(self.allocator);
                        }
                    }

                    self.entries.deinit();
                    self.left_offsets.deinit();
                    self.topics.deinit();
                    self.source.deinit();
                    self.allocator.destroy(self);
                }

                pub fn ensureExpand(self: *EntryGroup, max_count: usize) !void {
                    if (self.entries.items.len < max_count) {
                        try self.entries.appendNTimes(null, max_count - self.entries.items.len);

                        for (self.left_offsets.count()..max_count) |offset| {
                            try self.left_offsets.put(offset, true);
                        }
                    }
                }

                pub fn fetchEntry(self: *EntryGroup, index: usize, source_path: core.Event.Payload.SourcePath) !*Entry {
                    std.debug.assert(index < source_path.item_count);
                    try self.ensureExpand(source_path.item_count);

                    if (self.entries.items[index]) |entry| {
                        return entry;
                    }
                    else {
                        const entry = try Entry.init(self.allocator, source_path, &self.topics);
                        self.entries.items[index] = entry;
                        return entry;
                    }
                }

                pub fn popEntry(self: *EntryGroup, index: usize) ?*Entry {
                    defer _ = self.left_offsets.remove(index);

                    if (index < self.entries.items.len) {       
                        if (self.entries.items[index]) |entry| {
                            defer self.entries.items[index] = null;
                            return entry;
                        }
                    }

                    return null;
                }

                pub fn isExpired(self: *EntryGroup, hash: Symbol) bool {
                    return ! std.mem.eql(u8, self.source.hash, hash);
                }
            };

            const Entry = struct {
                header: core.Event.Payload.SourcePath,
                left_topics: std.BufSet,
                contents: std.BufMap,

                pub fn init(allocator: std.mem.Allocator, source_path: core.Event.Payload.SourcePath, topics: *std.BufSet) !*Entry {
                    const self = try allocator.create(Entry);
                    self.* =  .{
                        .header = try source_path.clone(allocator),
                        .left_topics = try topics.cloneWithAllocator(allocator),
                        .contents = std.BufMap.init(allocator),
                    };

                    return self;
                }

                pub fn deinit(self: *Entry, allocator: std.mem.Allocator) void {
                    self.contents.deinit();
                    self.left_topics.deinit();
                    self.header.deinit();
                    allocator.destroy(self);
                }

                pub fn update(self: *Entry, bodies: []const core.Event.Payload.TopicBody.Item) !CacheStatus {
                    for (bodies) |body| {
                        try self.contents.put(body.topic, body.content);
                        self.left_topics.remove(body.topic);
                    }

                    return if (self.left_topics.count() > 0) .missing else .fulfil;
                }
            };
        };
    }

    pub const TopicsMap = struct {
        allocator: std.mem.Allocator,
        entries: std.MultiArrayList(TopicsMap.Entry),
        // dialects: std.StringHashMap(u64),
        // dialects_rev: std.AutoHashMap(u64, Symbol),

        pub fn init(allocator: std.mem.Allocator) TopicsMap {
            return .{
                .allocator = allocator,
                .entries = .{},
            };
        }

        pub fn deinit(self: *TopicsMap) void {
            for (self.entries.items(.topic)) |topic| {
                self.allocator.free(topic);
            }
            self.entries.deinit(self.allocator);
        }

        pub fn addTopics(self: *TopicsMap, topic: core.events.Event.Payload.Topic) !void {
            for (topic.names) |s| {
                const entry: TopicsMap.Entry = .{
                    .category = topic.category,
                    .topic = try self.allocator.dupe(u8, s),
                };
                try self.entries.append(self.allocator, entry);
            }
        }

        pub fn iterator(self: *const TopicsMap) TopicsMap.Iterator {
            return .{
                .categories = self.entries.items(.category),
                .topics = self.entries.items(.topic),
                .size = self.entries.len,
            };
        }

        // TODO:
        // fn makeIntern(self: *TopicsMap, dialect: Symbol) !u64 {
        //     const entry = try self.dialects.getOrPut(dialect);
        //     if (entry.found_existing) {
        //         return entry.value_ptr.*;
        //     }

        //     const intern = self.dialects.count();

        //     entry.key_ptr.* = self.allocator.dupe(u8, dialect);
        //     entry.value_ptr.* = intern;

        //     self.dialects_rev.put(intern, entry.key_ptr.*);

        //     return intern;
        // }

        pub const Iterator = struct {
            categories: []const core.events.TopicCategory,
            topics: []const Symbol,
            index: usize = 0,
            size: usize,

            pub fn next(self: *TopicsMap.Iterator) ?(struct { category: core.events.TopicCategory, topic: Symbol }) {
                defer self.index += 1;
                if (self.index >= self.size) return null;

                return .{
                    .category = self.categories[self.index],
                    .topic = self.topics[self.index],
                };
            }
        };

        const Entry = struct {
            category: core.events.TopicCategory,
            topic: Symbol,
        };
    };
};