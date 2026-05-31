const std = @import("std");
const core = @import("core");
const c = @import("c");

const ReceiveEntry = core.sockets.ReceiveEntry;
const EventDispatcher = core.sockets.EventDispatcher;
const StructView = core.events.StructView;
const Event = core.events.Event;

pub fn ExtractTopicBodyState(comptime GuestStage: type) type {
    return struct {
        const Self = @This();

        pub const create: Self = .{};
        pub fn deinit(_: *Self) void {}

        pub fn handle(self: *const Self, stage: *GuestStage, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
            _ = self;
            switch (entry.event) {
                .probe_ready => {
                    var channel = try stage.connection.requestChannel();
                    try channel.submit(stage.connection.context.io, .ready, .{});
                },
                else => {
                    try stage.defaultHandler(entry, dirty);
                }
            }
        }
    };
}

// TODO:
// var lookup = std.StringHashMap(LookupEntry).init(self.allocator);
// defer lookup.deinit();
// while (self.connection.dispatcher.isReady()) {        
// switch (item.event) {

//     .source_path => |path| {
//         try self.logger.log(.debug, "Accept source path: `{s}`", .{path.path});
//         try self.logger.log(.trace, "Begin worker process", .{});

//         const p1 = try path.clone(self.allocator);
//         try lookup.put(p1.path, .{.path = p1, .item_count = 1});

//         try self.spawnWorker(path);
//     },
//     .worker_response => |res| {
//         try self.logger.log(.trace, "Receive worker respnse", .{});

//         if (try self.processWorkerResponse(item.from, res.content, lookup)) |event| {
//             try self.logger.log(.debug, "Redirect worker response (event: {})", .{event.tag()});
//             try self.connection.dispatcher.post(event);
//         }
//     },
//     .finish_source_path => {
//         if (lookup.count() == 0) {
//             try self.connection.dispatcher.post(.finish_topic_body);
//         }
//         else {
//             try self.connection.dispatcher.state.receiveTerminate();
//         }
//     },


// fn tryLoadSchema(self: *GuestStage, schema_dir_set: []const core.FilePath) !bool {
//     for (schema_dir_set) |path| {
//         const err = c.loadSchema(self.database, path.ptr, path.len);
//         switch (err) {
//             c.schema_dir_not_found => {
//                 try self.logger.log(.err, "Launch failed. Invalid schema location. ({s})", .{path});
//             },
//             c.schema_load_failed => {
//                 try self.logger.log(.err, "Launch failed. Invalid schema definitions.", .{});
//             },
//             else => {},
//         }
//     }

//     user_type: {
//         const err = c.retainUserTypeName(self.database);
//         switch (err) {
//             c.invalid_schema_catalog => {
//                 try self.logger.log(.err, "Launch failed. Invalid schema catalog.", .{});
//             },
//             else => {},
//         }
//         break:user_type;
//     }

//     return true;
// }

// fn spawnWorker(self: *GuestStage, path: core.Event.Payload.SourcePath) !void {
//     const worker = try ExtractWorker.init(
//         self.allocator, 
//         path.category, self.database, path
//     );
//     try self.connection.pull_sink_socket.spawn(worker);
// }

// const WorkerResultTags = std.StaticStringMap(core.EventType).initComptime(.{
//     .{"topic_body", .topic_body}, 
//     .{"log", .log},
// });

// pub const WorkerResponseTag = enum(u8) {
//     worker_progress = c.worker_progress,
//     worker_result = c.worker_result,
//     worker_finished = c.worker_finished,
//     worker_log = c.worker_log,
//     worker_skipped = c.worker_skipped,

//     pub fn fromString(tag: core.Symbol) WorkerResponseTag {
//         return std.meta.stringToEnum(WorkerResponseTag, tag).?;
//     }
// };

// fn processWorkerResponse(self: *GuestStage, from: Symbol, result_content: Symbol, lookup: *std.StringHashMap(LookupEntry)) !?core.Event {
//     var reader = core.CborStream.Reader.init(result_content);

//     const tag = try reader.readString();
//     const source_path = try reader.readString();

//     if (lookup.getPtr(source_path)) |entry| {
//         switch (WorkerResponseTag.fromString(tag)) {
//             .worker_progress => {
//                 entry.item_count = try reader.readUInt(usize);
//                 return null;
//             },
//             .worker_result => {
//                 defer Trace.info("Worker processed: `{s}`", .{source_path});
//                 return try processExtractResult(self.allocator, from, &reader, entry);
//             },
//             .worker_finished => {
//                 defer Trace.info("Worker finished: `{s}` (left: {})", .{source_path, lookup.count()});
//                 var path = entry.path;
//                 defer path.deinit();

//                 _ = lookup.remove(source_path);

//                 if (self.connection.dispatcher.state.level.terminating and (lookup.count() == 0)) {
//                     return .finish_topic_body;
//                 }
//                 else {
//                     return null;
//                 }
//             },
//             .worker_log => {
//                 return try processLogResult(self.allocator, from, &reader, entry);
//             },
//             .worker_skipped => {
//                 return try processSkipResult(self.allocator, from, &reader, entry);
//             }
//         }
//     }

//     const log_msg = try std.fmt.allocPrint(self.allocator, "Already processed: `{s}`", .{source_path});
//     defer self.allocator.free(log_msg);

//     return .{
//         .log = try core.Event.Payload.Log.init(self.allocator, .{.warn, log_msg}),
//     };
// }

// fn processExtractResult(allocator: std.mem.Allocator, from: Symbol, reader: *core.CborStream.Reader, entry: *LookupEntry) !core.Event {
//     _ = from;
//     const item_index = try reader.readUInt(u32);
//     const name_alt = try reader.readOptional(Symbol);

//     const items = try reader.readSlice(allocator, core.StructView(core.Event.Payload.TopicBody.Item));
//     defer allocator.free(items);

//     var topic_body = try core.Event.Payload.TopicBody.init(allocator, entry.path.values(), items);

//     if (name_alt) |name| {
//         var new_name = try allocator.dupe(u8, name);
//         defer allocator.free(new_name);
//         std.mem.swap(Symbol, &topic_body.header.name, &new_name);
//     }

//     return .{
//         .topic_body = topic_body.withNewIndex(item_index, entry.item_count),
//     };
// }

// fn processLogResult(allocator: std.mem.Allocator, from: Symbol, reader: *core.CborStream.Reader, entry: *LookupEntry) !core.Event {
//     _ = entry;
//     const log_level = core.Logger.stringToLogLevel(try reader.readString());
//     const content = try reader.readString();
    
//     const full_from = try std.fmt.allocPrint(allocator, "{s}/{s}", .{app_context, from});
//     defer allocator.free(full_from);

//     return .{
//         .log = try core.Event.Payload.Log.init(allocator,
//             .{log_level, content}
//         ),
//     };
// }

// fn processSkipResult(allocator: std.mem.Allocator, from: Symbol, reader: *core.CborStream.Reader, entry: *LookupEntry) !core.Event {
//     _ = from;
//     const item_index = try reader.readUInt(u32);

//     return .{
//         .skip_topic_body = try core.Event.Payload.SkipTopicBody.init(allocator, 
//             entry.path.values(),
//             item_index, 
//         ),
//     };
// }

// const LookupEntry = struct {
//     path: core.Event.Payload.SourcePath,
//     item_count: usize,
// };

// const WorkerTestContext = struct {
//     allocator: std.mem.Allocator,
//     stage: Stage,
//     src_dir: std.testing.TmpDir,
//     lookup: std.StringHashMap(LookupEntry),

//     const Stage  = GuestStage;

//     pub fn init(arena: *std.heap.ArenaAllocator) !WorkerTestContext {
//         const setting: Setting = .{
//             .arena = arena,
//             .endpoints = core.DebugEndPoint.StageEndpoint,
//             .log_level = .info,
//             .standalone = true,
//             .schema_dir_set = &.{},
//         };
//         const allocator = arena.allocator();

//         return .{
//             .allocator = allocator,
//             .stage = try Stage.init(allocator, setting),
//             .src_dir = std.testing.tmpDir(.{}),
//             .lookup = std.StringHashMap(LookupEntry).init(allocator),
//         };
//     }

//     pub fn deinit(self: *WorkerTestContext) void {
//         defer self.src_dir.cleanup();
//         defer self.lookup.deinit();
//         defer self.stage.deinit();
//     }

//     pub fn pushTestQuery(self: *WorkerTestContext, category: core.TopicCategory, query: core.Symbol) !core.Event.Payload.SourcePath {
//         var file = try self.src_dir.dir.createFile("test.sql", .{});
//         defer file.close();
//         try file.writeAll(query);

//         const path = try self.src_dir.dir.realpathAlloc(self.allocator, "test.sql");
//         defer self.allocator.free(path);

//         const source_path = try core.Event.Payload.SourcePath.init(self.allocator, .{
//             category,
//             "test",
//             path,
//             "test",
//             1
//         });
//         try self.lookup.put(source_path.path, .{.path = source_path, .item_count = 0});

//         return source_path.clone(self.allocator);
//     }

//     pub fn expectProgress(self: *WorkerTestContext, path: core.Event.Payload.SourcePath, from: Symbol, event: core.Event, expect_count: usize) !void {
//         validate: {
//             try std.testing.expectEqual(event.tag(), .worker_response);
                
//             var decoder = core.CborStream.Reader.init(event.worker_response.content);
//             try std.testing.expectEqualStrings(@tagName(.worker_progress), try decoder.readString());
//             try std.testing.expectEqualStrings(path.path, try decoder.readString());
//             try std.testing.expectEqual(expect_count, try decoder.readUInt(usize));
//             break:validate;
//         }
//         decode: {
//             const next_event = try self.stage.processWorkerResponse(from, event.worker_response.content, &self.lookup);
//             try std.testing.expect(next_event == null);
//             try std.testing.expectEqual(true, self.lookup.contains(path.path));
//             try std.testing.expectEqual(expect_count, self.lookup.get(path.path).?.item_count);
//             break:decode;
//         }
//     }

//     pub fn expectResult(
//         self: *WorkerTestContext, path: core.Event.Payload.SourcePath, from: Symbol, event: core.Event, 
//         expect_result: struct { name: Symbol, offset: usize, count: usize }, 
//         expect_topcs: []const Symbol) !void 
//     {
//         validate: {
//             try std.testing.expectEqual(event.tag(), .worker_response);
                
//             var decoder = core.CborStream.Reader.init(event.worker_response.content);
//             try std.testing.expectEqualStrings(@tagName(.worker_result), try decoder.readString());
//             try std.testing.expectEqualStrings(path.path, try decoder.readString());
//             try std.testing.expectEqual(expect_result.offset, try decoder.readUInt(usize));

//             const name_alt = try decoder.readOptional(Symbol);
//             try std.testing.expectEqual(expect_result.count > 1, name_alt != null);

//             const topic_bodies = try decoder.readSlice(self.allocator, core.StructView(core.Event.Payload.TopicBody.Item));
//             defer self.allocator.free(topic_bodies);

//             try expectTopics(self.allocator, topic_bodies, expect_topcs);
//             break:validate;
//         }
//         decode: {
//             const next_event = try self.stage.processWorkerResponse(from, event.worker_response.content, &self.lookup);
//             try std.testing.expect(next_event != null);
//             defer next_event.?.deinit();

//             try std.testing.expectEqual(true, self.lookup.contains(path.path));
//             try std.testing.expectEqual(.topic_body, next_event.?.tag());
//             try std.testing.expectEqualStrings(expect_result.name, next_event.?.topic_body.header.name);
//             try std.testing.expectEqual(expect_result.count, next_event.?.topic_body.header.item_count);
//             try std.testing.expectEqual(expect_result.offset, next_event.?.topic_body.index);
//             try std.testing.expectEqualStrings(path.path, next_event.?.topic_body.header.path);
//             break:decode;
//         }
//     }

//     fn expectTopics(allocator: std.mem.Allocator, topic_bodies: []const core.StructView(core.Event.Payload.TopicBody.Item), expect_topics: []const Symbol) !void {
//         try std.testing.expectEqual(expect_topics.len, topic_bodies.len);
        
//         var expect_set = std.BufSet.init(allocator);
//         defer expect_set.deinit();

//         for (expect_topics) |topic| {
//             try expect_set.insert(topic);
//         }

//         for (topic_bodies) |body| {
//             expect_set.remove(body[0]);
//         }

//         try std.testing.expectEqual(0, expect_set.count());
//     }

//     pub fn expectFinished(self: *WorkerTestContext, path: core.Event.Payload.SourcePath, from: Symbol, event: core.Event) !void {
//         validate: {
//             try std.testing.expectEqual(event.tag(), .worker_response);
                
//             var decoder = core.CborStream.Reader.init(event.worker_response.content);
//             try std.testing.expectEqualStrings(@tagName(.worker_finished), try decoder.readString());
//             try std.testing.expectEqualStrings(path.path, try decoder.readString());
//             break:validate;
//         }
//         decode: {
//             const next_event = try self.stage.processWorkerResponse(from, event.worker_response.content, &self.lookup);
//             try std.testing.expect(next_event == null);
//             try std.testing.expectEqual(false, self.lookup.contains(path.path));
//             break:decode;
//         }
//     }

//     pub fn expectLog(self: *WorkerTestContext, path: core.Event.Payload.SourcePath, from: Symbol, event: core.Event, expect_log_level: core.LogLevel) !void {
//         validate: {
//             try std.testing.expectEqual(event.tag(), .worker_response);
                
//             var decoder = core.CborStream.Reader.init(event.worker_response.content);
//             try std.testing.expectEqualStrings(@tagName(.worker_log), try decoder.readString());
//             try std.testing.expectEqualStrings(path.path, try decoder.readString());

//             const log_level = try decoder.readString();
//             const log_message = try decoder.readString();
//             try std.testing.expectEqual(std.meta.stringToEnum(core.LogLevel, log_level).?, expect_log_level);
//             try std.testing.expect(log_message.len > 0);
//             break:validate;
//         }
//         decode: {
//             const next_event = try self.stage.processWorkerResponse(from, event.worker_response.content, &self.lookup);
//             try std.testing.expect(next_event != null);
//             try std.testing.expectEqual(true, self.lookup.contains(path.path));
//             try std.testing.expectEqual(.log, next_event.?.tag());
//             try std.testing.expectEqual(expect_log_level, next_event.?.log.level);
//             try std.testing.expect(next_event.?.log.content.len > 0);
//             break:decode;
//         }
//     }

//     pub fn expectSkipResult(self: *WorkerTestContext, path: core.Event.Payload.SourcePath, from: Symbol, event: core.Event, expect_item_index: usize) !void {
//         validate: {
//             try std.testing.expectEqual(event.tag(), .worker_response);

//             var decoder = core.CborStream.Reader.init(event.worker_response.content);
//             try std.testing.expectEqualStrings(@tagName(.worker_skipped), try decoder.readString());
//             try std.testing.expectEqualStrings(path.path, try decoder.readString());
//             try std.testing.expectEqual(expect_item_index, try decoder.readUInt(usize));
//             break:validate;
//         }
//         decode: {
//             const next_event = try self.stage.processWorkerResponse(from, event.worker_response.content, &self.lookup);
//             try std.testing.expect(next_event != null);
//             try std.testing.expectEqual(true, self.lookup.contains(path.path));
//             try std.testing.expectEqual(.skip_topic_body, next_event.?.tag());
//             try std.testing.expectEqual(expect_item_index, next_event.?.skip_topic_body.index);
//             break:decode;
//         }
//     }
// };

pub const tests = struct {
};

const old_tests = struct {
    // test "worker workflow/single query (success flow)" {
    //     const allocator = std.testing.allocator;
    //     var arena = std.heap.ArenaAllocator.init(allocator);
    //     defer arena.deinit();

    //     var ctx = try WorkerTestContext.init(&arena);
    //     defer ctx.deinit();

    //     const path = try ctx.pushTestQuery(.source,
    //         \\ select 1
    //     );
    //     defer path.deinit();

    //     try ctx.stage.spawnWorker(path);

    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectProgress(path, item.from, item.event, 1);
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectResult(
    //             path, item.from, item.event, 
    //             .{ .name = "test", .offset = 0, .count = 1 }, 
    //             &.{
    //                 c.topic_query, c.topic_placeholder, c.topic_placeholder_order, 
    //                 c.topic_select_list, c.topic_bound_user_type, c.topic_anon_user_type
    //             }
    //         );
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectFinished(path, item.from, item.event);
    //         break:receive;
    //     }
    // }

    // test "worker workflow/single query (unssuported query flow)" {
    //     const allocator = std.testing.allocator;
    //     var arena = std.heap.ArenaAllocator.init(allocator);
    //     defer arena.deinit();

    //     var ctx = try WorkerTestContext.init(&arena);
    //     defer ctx.deinit();

    //     const path = try ctx.pushTestQuery(.source,
    //         \\ create table T (id int primary key)
    //     );
    //     defer path.deinit();

    //     try ctx.stage.spawnWorker(path);

    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectProgress(path, item.from, item.event, 1);
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectLog(path, item.from, item.event, .warn);
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectSkipResult(path, item.from, item.event, 0);
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectFinished(path, item.from, item.event);
    //         break:receive;
    //     }
    // }

    // test "worker workflow/multiple query (success flow) " {
    //     const allocator = std.testing.allocator;
    //     var arena = std.heap.ArenaAllocator.init(allocator);
    //     defer arena.deinit();

    //     var ctx = try WorkerTestContext.init(&arena);
    //     defer ctx.deinit();

    //     const path = try ctx.pushTestQuery(.source,
    //         \\ select 1 as a;
    //         \\ select 2 as b;
    //         \\ select 3 as c;
    //     );
    //     defer path.deinit();

    //     try ctx.stage.spawnWorker(path);

    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectProgress(path, item.from, item.event, 3);
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectResult(
    //             path, item.from, item.event, 
    //             .{ .name = "test_1", .offset = 0, .count = 3 }, 
    //             &.{
    //                 c.topic_query, c.topic_placeholder, c.topic_placeholder_order, 
    //                 c.topic_select_list, c.topic_bound_user_type, c.topic_anon_user_type
    //             }
    //         );
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectResult(
    //             path, item.from, item.event, 
    //             .{ .name = "test_2", .offset = 1, .count = 3 }, 
    //             &.{
    //                 c.topic_query, c.topic_placeholder, c.topic_placeholder_order, 
    //                 c.topic_select_list, c.topic_bound_user_type, c.topic_anon_user_type
    //             }
    //         );
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectResult(
    //             path, item.from, item.event, 
    //             .{ .name = "test_3", .offset = 2, .count = 3 }, 
    //             &.{
    //                 c.topic_query, c.topic_placeholder, c.topic_placeholder_order, 
    //                 c.topic_select_list, c.topic_bound_user_type, c.topic_anon_user_type
    //             }
    //         );
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectFinished(path, item.from, item.event);
    //         break:receive;
    //     }
    // }

    // test "worker workflow/multiple query (partially unssuported query flow)" {

    // }

    // test "worker workflow/empty query" {
    //     const allocator = std.testing.allocator;
    //     var arena = std.heap.ArenaAllocator.init(allocator);
    //     defer arena.deinit();

    //     var ctx = try WorkerTestContext.init(&arena);
    //     defer ctx.deinit();

    //     const path = try ctx.pushTestQuery(.source, "");
    //     defer path.deinit();

    //     try ctx.stage.spawnWorker(path);

    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectLog(path, item.from, item.event, .warn);
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectSkipResult(path, item.from, item.event, 0);
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectFinished(path, item.from, item.event);
    //         break:receive;
    //     }
    // }

    // test "worker workflow/empty query#2" {
    //     const allocator = std.testing.allocator;
    //     var arena = std.heap.ArenaAllocator.init(allocator);
    //     defer arena.deinit();

    //     var ctx = try WorkerTestContext.init(&arena);
    //     defer ctx.deinit();

    //     const path = try ctx.pushTestQuery(.source, "  \n\n ");
    //     defer path.deinit();

    //     try ctx.stage.spawnWorker(path);

    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectLog(path, item.from, item.event, .warn);
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectSkipResult(path, item.from, item.event, 0);
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectFinished(path, item.from, item.event);
    //         break:receive;
    //     }
    // }

    // test "worker workflow/invalid query" {
    //     const allocator = std.testing.allocator;
    //     var arena = std.heap.ArenaAllocator.init(allocator);
    //     defer arena.deinit();

    //     var ctx = try WorkerTestContext.init(&arena);
    //     defer ctx.deinit();

    //     const path = try ctx.pushTestQuery(.source,
    //         \\ SELCT 1
    //     );
    //     defer path.deinit();

    //     try ctx.stage.spawnWorker(path);

    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectLog(path, item.from, item.event, .err);
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectSkipResult(path, item.from, item.event, 0);
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectFinished(path, item.from, item.event);
    //         break:receive;
    //     }
    // }

    // test "worker workflow/single schema (success flow)" {
    //     const allocator = std.testing.allocator;
    //     var arena = std.heap.ArenaAllocator.init(allocator);
    //     defer arena.deinit();

    //     var ctx = try WorkerTestContext.init(&arena);
    //     defer ctx.deinit();

    //     const path = try ctx.pushTestQuery(.schema,
    //         \\ create type Visibility as enum ('hide', 'visible')
    //     );
    //     defer path.deinit();

    //     try ctx.stage.spawnWorker(path);

    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectProgress(path, item.from, item.event, 1);
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectResult(
    //             path, item.from, item.event, 
    //             .{ .name = "test", .offset = 0, .count = 1 }, 
    //             &.{
    //                 c.topic_user_type, c.topic_bound_user_type, c.topic_anon_user_type
    //             }
    //         );
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectFinished(path, item.from, item.event);
    //         break:receive;
    //     }
    // }

    // test "worker workflow/single schema (unssuported flow)" {
    //     const allocator = std.testing.allocator;
    //     var arena = std.heap.ArenaAllocator.init(allocator);
    //     defer arena.deinit();

    //     var ctx = try WorkerTestContext.init(&arena);
    //     defer ctx.deinit();

    //     const path = try ctx.pushTestQuery(.schema,
    //         \\ select 1
    //     );
    //     defer path.deinit();

    //     try ctx.stage.spawnWorker(path);

    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectProgress(path, item.from, item.event, 1);
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectLog(path, item.from, item.event, .warn);
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectSkipResult(path, item.from, item.event, 0);
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectFinished(path, item.from, item.event);
    //         break:receive;
    //     }
    // }

    // test "worker workflow/multiple schema (success flow) " {
    //     const allocator = std.testing.allocator;
    //     var arena = std.heap.ArenaAllocator.init(allocator);
    //     defer arena.deinit();

    //     var ctx = try WorkerTestContext.init(&arena);
    //     defer ctx.deinit();

    //     const path = try ctx.pushTestQuery(.schema,
    //         \\ create type Visibility as enum ('hide', 'visible');
    //         \\ create type Status as enum ('failed', 'success');
    //     );
    //     defer path.deinit();

    //     try ctx.stage.spawnWorker(path);

    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectProgress(path, item.from, item.event, 2);
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectResult(
    //             path, item.from, item.event, 
    //             .{ .name = "Visibility", .offset = 0, .count = 2 }, 
    //             &.{
    //                 c.topic_user_type, c.topic_bound_user_type, c.topic_anon_user_type
    //             }
    //         );
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectResult(
    //             path, item.from, item.event, 
    //             .{ .name = "Status", .offset = 1, .count = 2 }, 
    //             &.{
    //                 c.topic_user_type, c.topic_bound_user_type, c.topic_anon_user_type
    //             }
    //         );
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectFinished(path, item.from, item.event);
    //         break:receive;
    //     }
    // }

    // test "worker workflow/multiple schema (partially unssuported flow)" {
    // }

    // test "worker workflow/empty schema" {
    //     const allocator = std.testing.allocator;
    //     var arena = std.heap.ArenaAllocator.init(allocator);
    //     defer arena.deinit();

    //     var ctx = try WorkerTestContext.init(&arena);
    //     defer ctx.deinit();

    //     const path = try ctx.pushTestQuery(.schema, "");
    //     defer path.deinit();

    //     try ctx.stage.spawnWorker(path);

    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectLog(path, item.from, item.event, .warn);
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectSkipResult(path, item.from, item.event, 0);
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectFinished(path, item.from, item.event);
    //         break:receive;
    //     }
    // }

    // test "worker workflow/empty schema#2" {
    //     const allocator = std.testing.allocator;
    //     var arena = std.heap.ArenaAllocator.init(allocator);
    //     defer arena.deinit();

    //     var ctx = try WorkerTestContext.init(&arena);
    //     defer ctx.deinit();

    //     const path = try ctx.pushTestQuery(.schema, "  \n\n ");
    //     defer path.deinit();

    //     try ctx.stage.spawnWorker(path);

    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectLog(path, item.from, item.event, .warn);
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectSkipResult(path, item.from, item.event, 0);
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectFinished(path, item.from, item.event);
    //         break:receive;
    //     }
    // }

    // test "worker workflow/invalid schema" {
    //     const allocator = std.testing.allocator;
    //     var arena = std.heap.ArenaAllocator.init(allocator);
    //     defer arena.deinit();

    //     var ctx = try WorkerTestContext.init(&arena);
    //     defer ctx.deinit();

    //     const path = try ctx.pushTestQuery(.schema,
    //         \\ CREAT TYPE X AS ENUM ('x')
    //     );
    //     defer path.deinit();

    //     try ctx.stage.spawnWorker(path);

    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectLog(path, item.from, item.event, .err);
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectSkipResult(path, item.from, item.event, 0);
    //         break:receive;
    //     }
    //     receive: {
    //         const item = try ctx.stage.connection.dispatcher.dispatch() orelse @panic("Need to receive event");
    //         defer item.deinit();

    //         try ctx.expectFinished(path, item.from, item.event);
    //         break:receive;
    //     }
    // }
};