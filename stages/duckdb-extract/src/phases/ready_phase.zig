const std = @import("std");
const core = @import("core");
const c = @import("c");

const types = core.types;

const ReceiveEntry = core.sockets.ReceiveEntry;
const EventDispatcher = core.sockets.EventDispatcher;
const StructView = core.events.StructView;
const Event = core.events.Event;

const ExtractWorker = @import("../ExtractWorker.zig");

pub fn ExtractTopicBodyState(comptime GuestStage: type) type {
    return struct {
        t: *std.Io.Threaded,
        io: std.Io,

        const Self = @This();

        pub fn create(allocator: std.mem.Allocator) !Self {
            const t = try allocator.create(std.Io.Threaded);
            t.* = std.Io.Threaded.init(allocator, .{
                .concurrent_limit = std.Io.Limit.limited(try std.Thread.getCpuCount()),
            });

            return  .{
                .t = t,
                .io = t.io(),
            }; 
        }

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            self.t.deinit();
            allocator.destroy(self.t);
        }

        pub fn handle(self: *const Self, stage: *GuestStage, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
            switch (entry.event) {
                .probe => |phase| {
                    if ((phase == .ready) and (std.meta.eql(stage.dispatcher.phase, .{.kind = .ready, .agreement = .pending}))) {
                        var channel = try stage.connection.requestChannel();
                        try channel.submit(stage.connection.context.io, .ready, .{});

                        try stage.transitPhase(.ready, .confirmed);
                        return;
                    }
                },
                .source_path => |payload| {
                    if (self.t.concurrent_limit.toInt()) |limit| {
                        if (self.t.busy_count >= limit) {
                            try stage.log(.trace, "Worker pool is full", .{});
                            // will process latter
                            dirty.* = .delayed;
                            return;
                        }
                    }
                    try stage.log(.debug, "Accept source path: `{s}, dialect: {s}`", .{payload.name, payload.dialect});

                    const worker = try ExtractWorker.init(std.heap.c_allocator, payload);
                    try stage.reapers.detach(ExtractWorker.run, .{worker, self.io, stage.database, stage.connection.push_worker_socket.pipe});
                    try stage.log(.trace, "Begin worker process/name: {s}, dialect: {s}", .{payload.name, payload.dialect});
                    return;
                },
                else => {}
            }
            try stage.defaultHandler(entry, dirty);
        }
    };
}

test "test ready phase" {
    std.testing.refAllDecls(@This());
}

pub const tests = struct {
    const nnng = @import("nnng");
    const Setting = @import("../Setting.zig");
    const GuestStage = @import("../Stage.zig");
    
    const WorkerTestBed = struct {
    //     allocator: std.mem.Allocator,
        setting: *Setting,
        connection: *GuestStage.Connection,
        stage: GuestStage,
        src_dir: std.testing.TmpDir,

        pub fn init(io: std.Io, allocator: std.mem.Allocator) !WorkerTestBed {
            const src_dir: std.testing.TmpDir = try core.test_support.createTmpDir();

            const setting = try allocator.create(Setting);
            setting.* = .{
                .endpoints = try core.test_support.createEndpoint(src_dir, .{}),
                .log_level = .info,
                .log_style = .discard,
                .no_color = false,
                .schema_dir_set = &.{},
            };

            const connection = try allocator.create(GuestStage.Connection);
            connection.* = try GuestStage.Connection.create(io, allocator, setting.endpoints);
            var stage = try GuestStage.create(io, allocator, connection, setting);

            try stage.transitPhase(.ready, .pending);
            while (stage.dispatcher.queue.receive_queue.popFront()) |_| {}

            return .{
                .setting = setting,
                .connection = connection,
                .stage = stage,
                .src_dir = src_dir,
            };
        }

        pub fn deinit(self: *WorkerTestBed, allocator: std.mem.Allocator) void {
            self.stage.deinit();
            self.connection.deinit();
            allocator.destroy(self.connection);

            core.test_support.releaseEndpoint(self.setting.endpoints);
            allocator.destroy(self.setting);
            self.src_dir.cleanup();

            self.* = undefined;
        }

        pub fn postTestEvent(self: *WorkerTestBed, io: std.Io, allocator: std.mem.Allocator, desc: Event.Payload.SourceDescriptor, query: types.Symbol) !core.sockets.ReceiveEntry {
            var file = try self.src_dir.dir.createFile(io, "test.sql", .{});
            defer file.close(io);

            var buffer: [1024]u8 = undefined;
            var writer = file.writer(io, &buffer);
            try writer.interface.writeAll(query);
            try writer.interface.flush();

            const path_abs = path: {
                const path = try self.src_dir.dir.realPathFileAlloc(io, "test.sql", allocator);
                defer allocator.free(path);
                break:path try allocator.dupe(u8, path);
            };
            const source_path: Event.Payload.SourcePath = .{
                .category = desc.category,
                .name = desc.name,
                .dialect = desc.dialect,
                .path = path_abs,
                .hash = "deadbeaf",
                .item_count = 1,
            };
            
            return .{
                .pipe_id = 1,
                .event = .{ .source_path = source_path },
                .from_stage = "test",
                .buffer = path_abs,
                .msg = try nnng.Message.create(),
                .features = .{.replyable = true},
            };
        }

        pub fn spawnWorker(self: *WorkerTestBed, entry: core.sockets.ReceiveEntry) !void {
            switch (self.stage.state) {
                .ready => |state|{
                    var dirty: EventDispatcher.DirtyState = .unhandled;
                    try state.handle(&self.stage, entry, &dirty);
                },
                else => unreachable,
            }
        }

        fn onDispatchDelay(_: *EventDispatcher.Sized(1), _: core.sockets.ReceiveEntry, dirty: *EventDispatcher.DirtyState) anyerror!void {
            dirty.* = .delayed;
        }

        fn stealDispatchEvent(self: *WorkerTestBed) !?core.sockets.ReceiveEntry {
            while (true) {
                switch (try self.stage.dispatcher.iteration("test", onDispatchDelay)) {
                    .handled => break,
                    .awake => {},
                    .terminated => return null,
                }
            }

            //  steal receive entry
            return self.stage.dispatcher.queue.receive_queue.popBack();
        }

        pub fn expectProgress(self: *WorkerTestBed, desc: Event.Payload.SourceDescriptor, expect_count: usize) !void {
            var entry = try self.stealDispatchEvent() orelse unreachable;
            defer entry.deinit(self.stage.allocator);

            validate: {
                try std.testing.expectEqual(entry.event.tag(), .ready_topic_body);
                try std.testing.expectEqualDeep(entry.event.ready_topic_body.desc, desc);
                try std.testing.expectEqual(.progress, std.meta.activeTag(entry.event.ready_topic_body.response));
                try std.testing.expectEqual(expect_count, entry.event.ready_topic_body.response.progress);
                break:validate;
            }
        }

        pub fn expectSuccess(self: *WorkerTestBed, desc: Event.Payload.SourceDescriptor, expect_topics: []const types.Symbol) !void {
            var entry = try self.stealDispatchEvent() orelse unreachable;
            defer entry.deinit(self.stage.allocator);

            var left_topics = std.BufSet.init(self.stage.allocator);
            defer left_topics.deinit();

            for (expect_topics) |topic| {
                try left_topics.insert(topic);
            }

            validate: {
                try std.testing.expectEqual(entry.event.tag(), .ready_topic_body);
                try std.testing.expectEqualDeep(entry.event.ready_topic_body.desc, desc);
                try std.testing.expectEqual(.success, std.meta.activeTag(entry.event.ready_topic_body.response));
                try std.testing.expectEqual(expect_topics.len, entry.event.ready_topic_body.response.success.len);

                for (entry.event.ready_topic_body.response.success) |encoded| {
                    left_topics.remove(encoded.topic);
                }
                try std.testing.expectEqual(0, left_topics.count());
                break:validate;
            }
        }

        pub fn expectSkipped(self: *WorkerTestBed, desc: Event.Payload.SourceDescriptor) !void {
            var entry = try self.stealDispatchEvent() orelse unreachable;
            defer entry.deinit(self.stage.allocator);

            validate: {
                try std.testing.expectEqual(entry.event.tag(), .ready_topic_body);
                try std.testing.expectEqualDeep(entry.event.ready_topic_body.desc, desc);
                try std.testing.expectEqual(.skipped, std.meta.activeTag(entry.event.ready_topic_body.response));
                break:validate;
            }
        }

        pub fn expectLog(self: *WorkerTestBed, log_level: core.events.LogLevel) !void {
            var entry = try self.stealDispatchEvent() orelse unreachable;
            defer entry.deinit(self.stage.allocator);

            validate: {
                try std.testing.expectEqual(entry.event.tag(), .log);
                try std.testing.expectEqual(log_level, entry.event.log.level);
                break:validate;
            }
        }
    };

    test "worker workflow/single query (success flow)" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var test_bed = try WorkerTestBed.init(io, allocator);
        defer test_bed.deinit(allocator);

        const desc: Event.Payload.SourceDescriptor = .{.category = .source, .name = "test", .dialect = "duckdb", .offset = 0};
        var entry = try test_bed.postTestEvent(
            io, allocator, desc,
            \\ select 1
        );
        defer entry.deinit(allocator);

        try test_bed.spawnWorker(entry);

        progress: {
            try test_bed.expectProgress(desc, 1);
            break:progress;
        }
        success: {
            const topics = &.{
                c.topic_query, c.topic_placeholder, c.topic_placeholder_order, 
                c.topic_select_list, c.topic_bound_user_type, c.topic_anon_user_type,
            };
            try test_bed.expectSuccess(desc, topics);
            break:success;
        }
    }

    test "worker workflow/single query (unssuported query flow)" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var test_bed = try WorkerTestBed.init(io, allocator);
        defer test_bed.deinit(allocator);

        const desc: Event.Payload.SourceDescriptor = .{.category = .source, .name = "test", .dialect = "duckdb", .offset = 0};

        var entry = try test_bed.postTestEvent(
            io, allocator, desc,
            \\ create table T (id int primary key)
        );
        defer entry.deinit(allocator);

        try test_bed.spawnWorker(entry);

        progress: {
            try test_bed.expectProgress(desc, 1);
            break:progress;
        }
        log: {
            try test_bed.expectLog(.warn);
            break:log;
        }
        skipped: {
            try test_bed.expectSkipped(desc);
            break:skipped;
        }
    }

    test "worker workflow/multiple query (success flow) " {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var test_bed = try WorkerTestBed.init(io, allocator);
        defer test_bed.deinit(allocator);

        const desc_0: Event.Payload.SourceDescriptor = .{.category = .source, .name = "test", .dialect = "duckdb", .offset = 0};
        const desc_1: Event.Payload.SourceDescriptor = .{.category = .source, .name = "test", .dialect = "duckdb", .offset = 1};
        const desc_2: Event.Payload.SourceDescriptor = .{.category = .source, .name = "test", .dialect = "duckdb", .offset = 2};

        var entry = try test_bed.postTestEvent(
            io, allocator, desc_0,
            \\ select 1 as a;
            \\ select 2 as b;
            \\ select 3 as c;
        );
        defer entry.deinit(allocator);

        try test_bed.spawnWorker(entry);

        progress: {
            try test_bed.expectProgress(desc_0, 3);
            break:progress;
        }
        success: {
            const topics = &.{
                c.topic_query, c.topic_placeholder, c.topic_placeholder_order, 
                c.topic_select_list, c.topic_bound_user_type, c.topic_anon_user_type,
            };
            try test_bed.expectSuccess(desc_0, topics);
            break:success;
        }
        success: {
            const topics = &.{
                c.topic_query, c.topic_placeholder, c.topic_placeholder_order, 
                c.topic_select_list, c.topic_bound_user_type, c.topic_anon_user_type,
            };
            try test_bed.expectSuccess(desc_1, topics);
            break:success;
        }
        success: {
            const topics = &.{
                c.topic_query, c.topic_placeholder, c.topic_placeholder_order, 
                c.topic_select_list, c.topic_bound_user_type, c.topic_anon_user_type,
            };
            try test_bed.expectSuccess(desc_2, topics);
            break:success;
        }
    }

    test "worker workflow/multiple query (partially unssuported query flow)" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var test_bed = try WorkerTestBed.init(io, allocator);
        defer test_bed.deinit(allocator);

        const desc_0: Event.Payload.SourceDescriptor = .{.category = .source, .name = "test", .dialect = "duckdb", .offset = 0};
        const desc_1: Event.Payload.SourceDescriptor = .{.category = .source, .name = "test", .dialect = "duckdb", .offset = 1};

        var entry = try test_bed.postTestEvent(
            io, allocator, desc_0,
            \\ create table T (id int primary key);
            \\ select 1 as a;
        );
        defer entry.deinit(allocator);

        try test_bed.spawnWorker(entry);

        progress: {
            try test_bed.expectProgress(desc_0, 2);
            break:progress;
        }
        log: {
            try test_bed.expectLog(.warn);
            break:log;
        }
        skipped: {
            try test_bed.expectSkipped(desc_0);
            break:skipped;
        }
        success: {
            const topics = &.{
                c.topic_query, c.topic_placeholder, c.topic_placeholder_order, 
                c.topic_select_list, c.topic_bound_user_type, c.topic_anon_user_type,
            };
            try test_bed.expectSuccess(desc_1, topics);
            break:success;
        }
    }

    test "worker workflow/empty query" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var test_bed = try WorkerTestBed.init(io, allocator);
        defer test_bed.deinit(allocator);

        const desc: Event.Payload.SourceDescriptor = .{.category = .source, .name = "test", .dialect = "duckdb", .offset = 0};

        var entry = try test_bed.postTestEvent(io, allocator, desc, "");
        defer entry.deinit(allocator);

        try test_bed.spawnWorker(entry);

        log: {
            try test_bed.expectLog(.warn);
            break:log;
        }
        skipped: {
            try test_bed.expectSkipped(desc);
            break:skipped;
        }
    }

    test "worker workflow/empty query#2" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var test_bed = try WorkerTestBed.init(io, allocator);
        defer test_bed.deinit(allocator);

        const desc: Event.Payload.SourceDescriptor = .{.category = .source, .name = "test", .dialect = "duckdb", .offset = 0};

        var entry = try test_bed.postTestEvent(io, allocator, desc, "  \n\n");
        defer entry.deinit(allocator);

        try test_bed.spawnWorker(entry);

        log: {
            try test_bed.expectLog(.warn);
            break:log;
        }
        skipped: {
            try test_bed.expectSkipped(desc);
            break:skipped;
        }
    }

    test "worker workflow/invalid query" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var test_bed = try WorkerTestBed.init(io, allocator);
        defer test_bed.deinit(allocator);

        const desc: Event.Payload.SourceDescriptor = .{.category = .source, .name = "test", .dialect = "duckdb", .offset = 0};

        var entry = try test_bed.postTestEvent(
            io, allocator, desc,
            \\ SELCT 1
        );
        defer entry.deinit(allocator);

        try test_bed.spawnWorker(entry);

        log: {
            try test_bed.expectLog(.err);
            break:log;
        }
        skipped: {
            try test_bed.expectSkipped(desc);
            break:skipped;
        }
    }

    test "worker workflow/invalid query#2" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var test_bed = try WorkerTestBed.init(io, allocator);
        defer test_bed.deinit(allocator);

        const desc: Event.Payload.SourceDescriptor = .{.category = .source, .name = "test", .dialect = "duckdb", .offset = 0};

        // JSON file does not exist...
        var entry = try test_bed.postTestEvent(
            io, allocator, desc,
            \\ select 
            \\     unnest(data_1, recursive := true), unnest(data_2)
            \\ from (
            \\     select unnest(j)
            \\     from read_json("$dataset" := 'dummy.json') t(j)
            \\ )
        );
        defer entry.deinit(allocator);

        try test_bed.spawnWorker(entry);

        progress: {
            try test_bed.expectProgress(desc, 1);
            break:progress;
        }
        log: {
            try test_bed.expectLog(.err);
            break:log;
        }
        skipped: {
            try test_bed.expectSkipped(desc);
            break:skipped;
        }
    }

    test "worker workflow/single schema (success flow)" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var test_bed = try WorkerTestBed.init(io, allocator);
        defer test_bed.deinit(allocator);

        const desc: Event.Payload.SourceDescriptor = .{.category = .schema, .name = "test", .dialect = "duckdb", .offset = 0};

        var entry = try test_bed.postTestEvent(
            io, allocator, desc,
            \\ create type Visibility as enum ('hide', 'visible')
        );
        defer entry.deinit(allocator);

        try test_bed.spawnWorker(entry);

        progress: {
            try test_bed.expectProgress(desc, 1);
            break:progress;
        }
        success: {
            const topics = &.{
                c.topic_user_type, c.topic_bound_user_type, c.topic_anon_user_type,
            };
            try test_bed.expectSuccess(desc, topics);
            break:success;
        }
    }

    test "worker workflow/single schema (unssuported flow)" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var test_bed = try WorkerTestBed.init(io, allocator);
        defer test_bed.deinit(allocator);

        const desc: Event.Payload.SourceDescriptor = .{.category = .schema, .name = "test", .dialect = "duckdb", .offset = 0};

        var entry = try test_bed.postTestEvent(
            io, allocator, desc,
            \\ select 1
        );
        defer entry.deinit(allocator);

        try test_bed.spawnWorker(entry);

        progress: {
            try test_bed.expectProgress(desc, 1);
            break:progress;
        }
        log: {
            try test_bed.expectLog(.warn);
            break:log;
        }
        skipped: {
            try test_bed.expectSkipped(desc);
            break:skipped;
        }
    }

    test "worker workflow/multiple schema (success flow) " {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var test_bed = try WorkerTestBed.init(io, allocator);
        defer test_bed.deinit(allocator);

        const desc: Event.Payload.SourceDescriptor = .{.category = .schema, .name = "test", .dialect = "duckdb", .offset = 0};
        const desc_1: Event.Payload.SourceDescriptor = .{.category = .schema, .name = "test", .dialect = "duckdb", .offset = 1};

        var entry = try test_bed.postTestEvent(
            io, allocator, desc,
            \\ create type Visibility as enum ('hide', 'visible');
            \\ create type Status as enum ('failed', 'success');
        );
        defer entry.deinit(allocator);

        try test_bed.spawnWorker(entry);

        progress: {
            try test_bed.expectProgress(desc, 2);
            break:progress;
        }
        success: {
            const topics = &.{
                c.topic_user_type, c.topic_bound_user_type, c.topic_anon_user_type,
            };
            try test_bed.expectSuccess(desc, topics);
            break:success;
        }
        success: {
            const topics = &.{
                c.topic_user_type, c.topic_bound_user_type, c.topic_anon_user_type,
            };
            try test_bed.expectSuccess(desc_1, topics);
            break:success;
        }
    }

    test "worker workflow/multiple schema (partially unssuported flow)" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var test_bed = try WorkerTestBed.init(io, allocator);
        defer test_bed.deinit(allocator);

        const desc: Event.Payload.SourceDescriptor = .{.category = .schema, .name = "test", .dialect = "duckdb", .offset = 0};
        const desc_1: Event.Payload.SourceDescriptor = .{.category = .schema, .name = "test", .dialect = "duckdb", .offset = 1};

        var entry = try test_bed.postTestEvent(
            io, allocator, desc,
            \\ create type Status as enum ('failed', 'success');
            \\ select 1;
        );
        defer entry.deinit(allocator);

        try test_bed.spawnWorker(entry);

        progress: {
            try test_bed.expectProgress(desc, 2);
            break:progress;
        }
        success: {
            const topics = &.{
                c.topic_user_type, c.topic_bound_user_type, c.topic_anon_user_type,
            };
            try test_bed.expectSuccess(desc, topics);
            break:success;
        }
        log: {
            try test_bed.expectLog(.warn);
            break:log;
        }
        skipped: {
            try test_bed.expectSkipped(desc_1);
            break:skipped;
        }
    }

    test "worker workflow/empty schema" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var test_bed = try WorkerTestBed.init(io, allocator);
        defer test_bed.deinit(allocator);

        const desc: Event.Payload.SourceDescriptor = .{.category = .schema, .name = "test", .dialect = "duckdb", .offset = 0};

        var entry = try test_bed.postTestEvent(io, allocator, desc, "");
        defer entry.deinit(allocator);

        try test_bed.spawnWorker(entry);

        log: {
            try test_bed.expectLog(.warn);
            break:log;
        }
        skipped: {
            try test_bed.expectSkipped(desc);
            break:skipped;
        }
    }

    test "worker workflow/empty schema#2" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var test_bed = try WorkerTestBed.init(io, allocator);
        defer test_bed.deinit(allocator);

        const desc: Event.Payload.SourceDescriptor = .{.category = .schema, .name = "test", .dialect = "duckdb", .offset = 0};

        var entry = try test_bed.postTestEvent(io, allocator, desc, "  \n\n");
        defer entry.deinit(allocator);

        try test_bed.spawnWorker(entry);

        log: {
            try test_bed.expectLog(.warn);
            break:log;
        }
        skipped: {
            try test_bed.expectSkipped(desc);
            break:skipped;
        }
    }

    test "worker workflow/invalid schema" {
        const io = std.testing.io;
        const allocator = std.testing.allocator;

        var test_bed = try WorkerTestBed.init(io, allocator);
        defer test_bed.deinit(allocator);

        const desc: Event.Payload.SourceDescriptor = .{.category = .schema, .name = "test", .dialect = "duckdb", .offset = 0};

        var entry = try test_bed.postTestEvent(
            io, allocator, desc,
            \\ CREAT TYPE X AS ENUM ('x')
        );
        defer entry.deinit(allocator);

        try test_bed.spawnWorker(entry);

        log: {
            try test_bed.expectLog(.err);
            break:log;
        }
        skipped: {
            try test_bed.expectSkipped(desc);
            break:skipped;
        }
    }
};
