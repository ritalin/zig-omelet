const std = @import("std");
const nnng = @import("nnng");
const core = @import("core");
const app_context = @import("build_options").app_context;

const Event = core.events.Event;
const GenerateStatus = Event.Payload.GenerateResponse.Status;

const CodeBuilder = @import("./CodeBuilder.zig");

const Self = @This();

allocator: std.mem.Allocator,
source: Event.Payload.TopicBody,
output_root: std.Io.Dir,
on_handle: CodeBuilder.OnBuild,

pub fn init(io: std.Io, allocator: std.mem.Allocator, source: *const Event.Payload.TopicBody, output_dir_path: core.types.FilePath) !Self {
    return .{
        .allocator = allocator,
        .source = try Event.Payload.TopicBody.Support.clone(allocator, source),
        .output_root = try std.Io.Dir.cwd().openDir(io, output_dir_path, .{}),
        .on_handle = if (source.desc.category == .source) CodeBuilder.SourceGenerator.build else CodeBuilder.UserTypeGenerator.build,
    };
}

pub fn deinit(self: *Self, io: std.Io) void {
    self.output_root.close(io);
    Event.Payload.TopicBody.Support.release(self.allocator, &self.source);
}

// pub fn run(self: *Self) !void {
pub fn run(self: Self, io: std.Io, pipe: nnng.Pipe.Sync) !void {
    var worker = self;
    defer worker.deinit(io);

    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var builder = CodeBuilder.init(allocator);
    // TODO:
    // defer builder.deinit();

    var walker = CodeBuilder.Parser.beginParse(worker.source.bodies);
    // TODO:
    // defer walker.deinit();

    builder.apply(allocator, &walker) catch |err| {
        var channel = try core.sockets.SendChannel.init(worker.allocator, pipe.item.id, app_context, pipe.item.sender());
        defer channel.deinit();
        try worker.sendData(&channel, try self.failedMessage(allocator, err), .generate_failed);
        return;
    };

    result: {
        var channel = try core.sockets.SendChannel.init(worker.allocator, pipe.item.id, app_context, pipe.item.sender());
        defer channel.deinit();
        
        if ((worker.on_handle)(&builder, io, worker.allocator, worker.output_root, worker.source.desc.name)) |status| {
            try worker.sendData(&channel, try self.successMessage(allocator), status);
        }
        else |err| {
            try worker.sendData(&channel, try self.failedMessage(allocator, err), .generate_failed);
        }
        break:result;
    }

    if (core.Logger.accepted(.trace)) {
        var channel = try core.sockets.SendChannel.init(worker.allocator, pipe.item.id, app_context, pipe.item.sender());
        try sendLog(&channel, .trace, "Finish worker process");
    }
}

fn successMessage(self: *const Self, allocator: std.mem.Allocator) ![]const u8 {    
    const desc = self.source.desc;
    const name = self.source.name_alt orelse desc.name;

    if (desc.category == .schema) {
        return std.fmt.allocPrint(allocator, CodeBuilder.UserTypeGenerator.success_log_fmt, .{name, desc.dialect, @tagName(desc.category)});
    }
    else {
        return std.fmt.allocPrint(allocator, CodeBuilder.SourceGenerator.success_log_fmt, .{name, desc.dialect, @tagName(desc.category)});
    }
}

fn failedMessage(self: *const Self, allocator: std.mem.Allocator, err: anyerror) ![]const u8 {
    const desc = self.source.desc;
    const name = self.source.name_alt orelse desc.name;

    switch (err) {
        error.QueryFileGenerationFailed => {
            return std.fmt.allocPrint(allocator, "Failed SQL file/name: {s}, dialect: {s}, category: {s}", .{name, desc.dialect, @tagName(desc.category)});
        },
        error.TypeFileGenerationFailed => {
            return std.fmt.allocPrint(allocator, "Failed Typescript file/name: {s}, dialect: {s}, category: {s}", .{name, desc.dialect, @tagName(desc.category)});
        },
        else => {
            return std.fmt.allocPrint(allocator, "Unexpected error during generating stage ({})/name: {s}, dialect: {s}, category: {s}", .{err, name, desc.dialect, @tagName(desc.category)});
        }
    }
}

fn sendData(self: *const Self, channel: *core.sockets.SendChannel, message: core.types.Symbol, status: GenerateStatus) !void {
    const res: Event.Payload.GenerateResponse = .{
        .desc = self.source.desc,
        .status = status,
        .message = message,
    };
    try channel.encode(.{ .finish_generate = res });
    try channel.submit(.{});
}

fn sendLog(channel: *core.sockets.SendChannel, level: core.events.LogLevel, message: core.types.Symbol) !void {
    const log: Event.Payload.Log = .{
        .level = level,
        .content = message,
    };
    try channel.encode(.{.log = log});
    try channel.submit(.{});
}
