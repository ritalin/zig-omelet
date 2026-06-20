const std = @import("std");
const nnng = @import("nnng");
const nng_core = @import("nng_core");
const core = @import("core");
const c = @import("c");

const events = core.events;
const stage_name: core.types.StageName = @import("build_options").worker_stage;

const Worker = @This();

allocator: std.mem.Allocator,
category: events.TopicCategory,
name: core.types.Symbol,
path: core.types.FilePath,
dialect: core.types.Symbol,
hash: core.types.Symbol,
on_handle: *const fn (io: std.Io, database: c.DatabaseRef, stage: c.Slice, desc: c.SourceDescriptor, query: c.Slice, sender: nnng.PipeSender) anyerror!void,

pub fn init(allocator: std.mem.Allocator, source_path: events.Event.Payload.SourcePath) !Worker {
    return .{
        .allocator = allocator,
        .category = source_path.category,
        .path = try allocator.dupeZ(u8, source_path.path),
        .name = try allocator.dupe(u8, source_path.name),
        .dialect = try allocator.dupe(u8, source_path.dialect),
        .hash = try allocator.dupe(u8, source_path.hash),
        .on_handle = if (source_path.category == .source) SourceHandler.run else SchemaHandler.run,
    };
}

pub fn deinit(self: *Worker) void {
    self.allocator.free(self.name);
    self.allocator.free(self.path);
    self.allocator.free(self.dialect);
    self.allocator.free(self.hash);
}

const ResultSet = struct { core.Symbol, core.Symbol, bool };

pub fn run(self: Worker, io: std.Io, database: c.DatabaseRef, pipe: nnng.Pipe.Sync) void {
    var worker = self;
    defer worker.deinit();

    var file = 
        std.Io.Dir.cwd().openFile(io, worker.path, .{}) 
        catch |err| {
            switch (err) {
                error.FileNotFound => {
                    workerLog(worker.allocator, pipe, .err, "File not found: {s}", .{worker.path}) catch {};
                },            
                else => {
                    workerLog(worker.allocator, pipe, .err, "Invalid file: {s}", .{worker.path}) catch {};
                }
            }
            return;
        }
    ;
    defer file.close(io);

    var buffer: [4096]u8 = undefined;
    var reader = file.reader(io, &buffer);
    const q = reader.interface.allocRemaining(worker.allocator, .unlimited) catch {
        workerLog(worker.allocator, pipe, .err, "Failed to read file: {s}", .{worker.path}) catch {};
        return;
    };
    defer worker.allocator.free(q);

    const stage: c.Slice = .{ .ptr = stage_name.ptr, .len = stage_name.len};
    const desc: c.SourceDescriptor = .{
        .response_event_tag = @intFromEnum(events.EventType.ready_topic_body),
        .log_event_tag = @intFromEnum(events.EventType.log),
        .topic_category = @intFromEnum(worker.category),
        .name = .{ .ptr = worker.name.ptr, .len = worker.name.len },
        .dialect = .{ .ptr = worker.dialect.ptr, .len = worker.dialect.len },
        .hash = .{ .ptr = worker.hash.ptr, .len = worker.hash.len },
    };
    const query: c.Slice = .{.ptr = q.ptr, .len = q.len};

    worker.on_handle(io, database, stage, desc, query, pipe.item.sender()) catch {};

    if (core.Logger.accepted(.trace)) {
        workerLog(worker.allocator, pipe, .trace, "Finish worker process", .{}) catch {};
    }
}

fn workerLog(allocator: std.mem.Allocator, pipe: nnng.Pipe.Sync, comptime level: events.LogLevel, comptime fmt: []const u8, args: anytype) !void {
    var channel = try core.sockets.SendChannel.init(
        allocator,
        pipe.item.id,
        stage_name,
        pipe.item.sender()
    );
    const content = try std.fmt.allocPrint(allocator, fmt, args);
    defer allocator.free(content);

    const log: events.Event.Payload.Log = .{
        .level = level,
        .content = content,
    };
    try channel.encode(.{.log = log});
    try channel.submit(.{});
}

pub const SourceHandler = struct {
    pub fn run(io: std.Io, database: c.DatabaseRef, stage: c.Slice, desc: c.SourceDescriptor, query: c.Slice, sender: nnng.PipeSender) !void {
        var collector: c.CollectorRef = undefined;
        _ = c.initSourceCollector(database, stage, desc, &collector);
        defer c.deinitSourceCollector(collector);

        _ = c.executeDescribe(collector, query);
        try io.checkCancel();
        
        const len = c.getDescribeResultCount(collector);
        try sendResult(io, collector, len, sender, c.getDescribeResult);
    }
};

pub const SchemaHandler = struct {
    pub fn run(io: std.Io, database: c.DatabaseRef, stage: c.Slice, desc: c.SourceDescriptor, query: c.Slice, sender: nnng.PipeSender) !void {
        var collector: c.CollectorRef = undefined;
        _ = c.initUserTypeCollector(database, stage, desc, &collector);
        defer c.deinitUserTypeCollector(collector);

        _ = c.describeUserType(collector, query);
        try io.checkCancel();

        const len = c.getUserTypeResultCount(collector);
        try sendResult(io, collector, len, sender, c.getUserTypeResult);
    }
};

const ResultAccessorFn = *const fn (c.CollectorRef, usize) callconv(.c) ?*c.nng_msg;

fn sendResult(io: std.Io, collector: c.CollectorRef, result_count: usize, sender: nnng.PipeSender, accessor: ResultAccessorFn) !void {
    const BACKOFF_LIMIT = std.Io.Duration.fromMicroseconds(32);
    var backoff = std.Io.Duration.fromMicroseconds(1);

    for (0..result_count) |i| {
        const raw_msg = accessor(collector, i);
        if (raw_msg == null) continue;

        const msg = nnng.Message.fromRaw(@ptrCast(raw_msg.?));

        while (true) {
            break sender.submit(msg) catch |err| switch (err) {
                error.WouldBlock => {
                    try std.Io.sleep(io, backoff, .awake);
                    backoff = std.Io.Duration.fromMicroseconds(
                        @min(backoff.toMicroseconds() << 1, BACKOFF_LIMIT.toMicroseconds())
                    );
                    continue;
                },
                else => {
                    // discard
                    break;
                }
            };
        }
    }
}