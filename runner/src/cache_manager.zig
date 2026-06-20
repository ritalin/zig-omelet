const std = @import("std");
const core = @import("core");

const types = core.types;
const events = core.events;

const Event = events.Event;

const GuestConfig = @import("./configs/Config.zig").Guest;

pub const CacheManager = struct {
    topics_map: TopicsMap,
    header_entries: HeaderMap,
    body_entries: BodyMap,
    generate_stages: std.BufSet,

    pub fn init(topics: TopicsMap, generate_stages: std.BufSet) CacheManager {
        return .{
            .topics_map = topics,
            .header_entries = .empty,
            .body_entries = .empty,
            .generate_stages = generate_stages,
        };
    }

    pub fn deinit(self: *CacheManager, allocator: std.mem.Allocator) void {
        deinit_header: {
            var iter = self.header_entries.iterator();
            while (iter.next()) |e| {
                removeCacheKey(allocator, e.key_ptr);
                e.value_ptr.deinit(allocator);
            }
            self.header_entries.deinit(allocator);
            break:deinit_header;
        }
        deinit_body: {
            var iter = self.body_entries.iterator();
            while (iter.next()) |e| {
                removeCacheKey(allocator, e.key_ptr);
                e.value_ptr.deinit(allocator);
            }
            self.body_entries.deinit(allocator);
            break:deinit_body;
        }
        self.topics_map.deinit();
        self.generate_stages.deinit();
    }

    pub fn register(self: *CacheManager, allocator: std.mem.Allocator, source: *const Event.Payload.SourcePath) !void {
        const desc: Event.Payload.SourceDescriptor = .{
            .category = source.category,
            .name = source.name, 
            .dialect = source.dialect, 
            .offset = 0,
        };
        const entry = try self.header_entries.getOrPut(allocator, desc);

        if (entry.found_existing) {
            // rid of entry
            self.unregisterBody(allocator, source.category, source.name, source.dialect, entry.value_ptr.progress.items.len);
            entry.value_ptr.deinit(allocator);
        }
        else {
            entry.key_ptr.* = events.Event.Payload.SourceDescriptor{  
                .category = source.category,
                .name = try allocator.dupe(u8, source.name),
                .dialect = try allocator.dupe(u8, source.dialect),
                .offset = 0,
            };
        }

        entry.value_ptr.* = try SourceEntry.create(allocator, source);
    }

    fn removeCacheKey(allocator: std.mem.Allocator, key: *Event.Payload.SourceDescriptor) void {
        allocator.free(key.name);
        allocator.free(key.dialect);
    }

    fn unregisterBody(self: *CacheManager, allocator: std.mem.Allocator, category: events.TopicCategory, name: types.Symbol, dialect: types.Symbol, n: usize) void {
        for (0..n) |offset| {
            const desc: events.Event.Payload.SourceDescriptor = .{ 
                .category = category,
                .name = name, 
                .dialect = dialect, 
                .offset = offset,
            };
            // Key is shared object, so it does not destroy.
            if (self.body_entries.fetchRemove(desc)) |kv| {
                var body = kv.value;
                body.deinit(allocator);
            }
        }
    }

    pub fn update(self: *CacheManager, allocator: std.mem.Allocator, guest_stage: types.StageName, response: Event.Payload.TopicBodyResponse) !Status {
        const key: Event.Payload.SourceDescriptor = .{
            .category = response.desc.category,
            .name = response.desc.name, 
            .dialect = response.desc.dialect, 
            .offset = 0,
        };
        if (self.header_entries.getEntry(key)) |kv| {
            if (kv.value_ptr.isExpired(response.hash)) return .expired;

            const desc: *Event.Payload.SourceDescriptor = kv.key_ptr;

            switch (response.response) {
                .progress => |n| {
                    try kv.value_ptr.expand(allocator, n);
                    return .progress;
                },
                .success => |topics| {
                    const new_desc: Event.Payload.SourceDescriptor = .init(.{desc.category, desc.name, desc.dialect, response.desc.offset});
                    var body_entry = try self.fetchBodyEntry(allocator, new_desc);
                    return body_entry.update(allocator, response.name_alt, topics);
                },
                .skipped => {
                    const new_desc: Event.Payload.SourceDescriptor = .init(.{desc.category, desc.name, desc.dialect, response.desc.offset});
                    self.finishBodyEntry(allocator, guest_stage, new_desc);
                    return .skipped;
                }
            }
        }

        return .expired;
    }

    fn fetchBodyEntry(self: *CacheManager, allocator: std.mem.Allocator, desc: Event.Payload.SourceDescriptor) !*BodyEntry {
        const entry = try self.body_entries.getOrPut(allocator, desc);

        if (!entry.found_existing) {
            entry.value_ptr.* = try BodyEntry.create(allocator, desc.category, &self.topics_map, &self.generate_stages);
        }

        return entry.value_ptr;
    }

    pub fn makeTopicBody(self: *CacheManager, desc: Event.Payload.SourceDescriptor) Event.Payload.TopicBody {
        const entry = self.body_entries.get(desc) orelse unreachable;

        return .{
            .desc = desc,
            .name_alt = entry.name_alt,
            .bodies = entry.entries.items,
        };
    }

    fn finishHeaderEntry(self: *CacheManager, allocator: std.mem.Allocator, desc: Event.Payload.SourceDescriptor, offset: usize) void {
        if (self.header_entries.getPtr(desc)) |entry| {
            if (entry.finish(offset) == .completed) {
                entry.deinit(allocator);
                _ = self.header_entries.remove(desc);
            }
        }
    }

    pub fn finishBodyEntry(self: *CacheManager, allocator: std.mem.Allocator, guest_stage: types.StageName, desc: Event.Payload.SourceDescriptor) void {
        if (self.body_entries.getPtr(desc)) |entry| {
            if (entry.finish(guest_stage) == .completed) {
                entry.deinit(allocator);
                _ = self.body_entries.remove(desc);
            }
        }

        const header_desc: Event.Payload.SourceDescriptor = .{
            .category = desc.category,
            .name = desc.name,
            .dialect = desc.dialect,
            .offset = 0,
        };
        self.finishHeaderEntry(allocator, header_desc, desc.offset);
    }

    pub const HeaderMap = std.HashMapUnmanaged(Event.Payload.SourceDescriptor, CacheManager.SourceEntry, CacheManager.CacheContext, std.hash_map.default_max_load_percentage);
    pub const BodyMap = std.HashMapUnmanaged(Event.Payload.SourceDescriptor, CacheManager.BodyEntry, CacheManager.CacheContext, std.hash_map.default_max_load_percentage);

    pub const Status = enum {
        expired,
        progress,
        ready,
        skipped,
        already_sent,
        completed,
    };

    const SourceEntry = struct {
        source: Event.Payload.SourcePath,
        progress: std.ArrayListUnmanaged(bool) = .empty,

        pub fn create(allocator: std.mem.Allocator, source: *const Event.Payload.SourcePath) !SourceEntry {
            return .{
                .source = Event.Payload.SourcePath.init(.{
                    source.category,
                    try allocator.dupe(u8, source.name),
                    try allocator.dupe(u8, source.path),
                    try allocator.dupe(u8, source.dialect),
                    try allocator.dupe(u8, source.hash),
                    1,
                }),
            };
        }

        pub fn deinit(self: *SourceEntry, allocator: std.mem.Allocator) void {
            allocator.free(self.source.name);
            allocator.free(self.source.path);
            allocator.free(self.source.dialect);
            allocator.free(self.source.hash);
            self.progress.deinit(allocator);
        }
        
        pub fn isExpired(self: *const SourceEntry, hash: types.Symbol) bool {
            return ! std.mem.eql(u8, self.source.hash, hash);
        }

        pub fn expand(self: *SourceEntry, allocator: std.mem.Allocator, n: usize) !void {
            self.progress.clearRetainingCapacity();
            try self.progress.appendNTimes(allocator, false, n);
        }

        pub fn finish(self: *SourceEntry, offset: usize) Status {
            if (offset < self.progress.items.len) {
                self.progress.items[offset] = true;
            }

            return if (std.mem.allEqual(bool, self.progress.items,true)) .completed else .progress;
        }
    };

    const BodyEntry = struct {
        name_alt: ?types.Symbol = null,
        left_topics: std.BufSet,
        entries: std.ArrayListUnmanaged(Event.Payload.TopicBody.Encoded) = .empty,
        generate_stages: std.BufSet,
//             left_offsets: std.AutoHashMap(usize, bool),

        pub fn create(allocator: std.mem.Allocator, category: events.TopicCategory, topics_map: *const TopicsMap, generate_stages: *const std.BufSet) !BodyEntry {
            var left_topics = std.BufSet.init(allocator);
            var iter = topics_map.iterator();
            while (iter.next()) |e| {
                if (e.category == category) {
                    try left_topics.insert(e.topic);
                }
            }

            return .{
                .left_topics = left_topics,
                .generate_stages = try generate_stages.cloneWithAllocator(allocator),
            };
        }

        pub fn deinit(self: *BodyEntry, allocator: std.mem.Allocator) void {
            if (self.name_alt) |name| allocator.free(name);
            self.left_topics.deinit();
            self.generate_stages.deinit();

            for (self.entries.items) |body| {
                allocator.free(body.topic);
                allocator.free(body.data);
            }
            self.entries.deinit(allocator);
        }

        pub fn update(self: *BodyEntry, allocator: std.mem.Allocator, name_alt: ?types.Symbol, bodies: []const Event.Payload.TopicBody.Encoded) !Status {
            if (self.left_topics.count() == 0) return .already_sent;
            
            if (name_alt) |name| {
                if (self.name_alt) |old_name| allocator.free(old_name);
                self.name_alt = try allocator.dupe(u8, name);
            }

            try self.entries.ensureUnusedCapacity(allocator, bodies.len);

            for (bodies) |body| {
                if (self.left_topics.contains(body.topic)) {
                    const topic = try allocator.dupe(u8, body.topic);
                    const data = try allocator.dupe(u8, body.data);
                    try self.entries.append(allocator, .init(.{topic, data}));
                    self.left_topics.remove(body.topic);
                }
            }

            return if (self.left_topics.count() == 0) .ready else .progress;
        }

        pub fn finish(self: *BodyEntry, stage: types.StageName) Status {
            self.generate_stages.remove(stage);

            return if (self.generate_stages.count() == 0) .completed else .progress;
        }
    };

    const CacheContext = struct {
        pub fn hash(_: @This(), key: events.Event.Payload.SourceDescriptor) u64 {
            var h = std.hash.Wyhash.init(0);
            h.update(std.mem.asBytes(&key.category));
            h.update(key.name);
            h.update(key.dialect);
            h.update(std.mem.asBytes(&key.offset));
            return h.final();
        }

        pub fn eql(ctx: CacheContext, lhs: events.Event.Payload.SourceDescriptor, rhs: events.Event.Payload.SourceDescriptor) bool {
            _ = ctx;
            return
                (lhs.category == rhs.category) and 
                std.mem.eql(u8, lhs.name, rhs.name) and
                std.mem.eql(u8, lhs.dialect, rhs.dialect) and
                (lhs.offset == rhs.offset)
            ;
        }                
    };

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

        // TODO:dialect management
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
            topics: []const types.Symbol,
            index: usize = 0,
            size: usize,

            pub fn next(self: *TopicsMap.Iterator) ?(struct { category: core.events.TopicCategory, topic: types.Symbol }) {
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
            topic: types.Symbol,
        };
    };
};