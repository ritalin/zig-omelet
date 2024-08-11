const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");

const CodeBuilder = @import("./CodeBuilder.zig");

const Self = @This();

allocator: std.mem.Allocator,
source: core.Event.Payload.TopicBody,
output_base_path: core.FilePath,
output_path: core.FilePath,
is_new: bool,

pub fn init(allocator: std.mem.Allocator, source: core.Event.Payload.TopicBody, output_dir_path: core.FilePath) !*Self {
    const output_base_path = try std.fs.cwd().realpathAlloc(allocator, output_dir_path);
    const output_path = try allocator.dupe(u8, trimExtension(source.header.name));

    var dir = try std.fs.cwd().openDir(output_base_path, .{});
    defer dir.close();
    const is_new = if (dir.statFile(output_path)) |_| false else |_| true;

    const self = try allocator.create(Self);
    self.* = .{
        .allocator = allocator,
        .source = try source.clone(allocator),
        .output_base_path = output_base_path,
        .output_path = output_path,
        .is_new = is_new,
    };

    return self;
}

pub fn deinit(self: *Self) void {
    self.source.deinit();
    self.allocator.free(self.output_base_path);
    self.allocator.free(self.output_path);
    self.allocator.destroy(self);
}

// pub fn run(self: *Self) !void {
pub fn run(self: *Self, socket: *zmq.ZSocket) !void {
    // _ = socket;
    try sendLog(self.allocator, socket, .trace, "Begin generate from `{s}`", .{self.source.header.name});

    var builder = try CodeBuilder.init(self.allocator, self.output_base_path, self.output_path);
    defer builder.deinit();

    var walker = try CodeBuilder.Parser.beginParse(self.allocator, self.source.bodies);
    defer walker.deinit();

    while (try walker.walk()) |target| switch (target) {
        .query => |q| {
            try builder.applyQuery(q);
        },
        .parameter => |placeholder| {
            try builder.applyPlaceholder(placeholder);
        },
        .result_set => |field_types| {
            try builder.applyResultSets(field_types);
        },
    };

    const payload = payload: {
        var writer = try core.CborStream.Writer.init(self.allocator);
        defer writer.deinit();
        
        if (builder.build()) {
            _ = try writer.writeString(self.source.header.path);
            _ = try writer.writeString(self.output_path);
            _ = try writer.writeString("Successful");
            _ = try writer.writeEnum(ResultStatus, if (self.is_new) .new_file else .update_file);
        }
        else  |err| switch (err) {
            error.QueryFileGenerationFailed => {
                _ = try writer.writeString(self.source.header.path);
                _ = try writer.writeString(self.output_path);
                _ = try writer.writeString("Failed SQL file");
                _ = try writer.writeEnum(ResultStatus, .generate_failed);
            },
            error.TypeFileGenerationFailed => {
                _ = try writer.writeString(self.source.header.path);
                _ = try writer.writeString(self.output_path);
                _ = try writer.writeString("Failed Typescript file");
                _ = try writer.writeEnum(ResultStatus, .generate_failed);
            },
        }

        break :payload try writer.buffer.toOwnedSlice();
    };
    defer self.allocator.free(payload);

    const event: core.Event = .{
        .worker_result = try core.Event.Payload.WorkerResult.init(self.allocator, .{payload}),
    };
    defer event.deinit();

    try core.sendEvent(self.allocator, socket, "task", event);
}

fn sendLog(allocator: std.mem.Allocator, socket: *zmq.ZSocket, log_level: core.LogLevel, comptime fmt: []const u8, args: anytype) !void {
    const message = try std.fmt.allocPrint(allocator, fmt, args);
    defer allocator.free(message);

    const log: core.Event = .{
        .log = try core.Event.Payload.Log.init(allocator, .{log_level, message}),
    };
    defer log.deinit();
    try core.sendEvent(allocator, socket, "task", log);
}

fn trimExtension(name: core.Symbol) core.Symbol {
    const ext = std.fs.path.extension(name);
    if (std.mem.lastIndexOf(u8, name, ext)) |i| {
        return name[0..i];
    }
    else {
        return name;
    }
}

pub const ResultStatus = enum {
    new_file,
    update_file,
    generate_failed,
};