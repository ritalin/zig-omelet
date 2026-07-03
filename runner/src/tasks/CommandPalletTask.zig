const std = @import("std");
const clap = @import("clap");
const nnng = @import("nnng");
const cbor = @import("cbor");
const core = @import("core");

const Symbol = core.types.Symbol;
const Event = core.events.Event;

const app_context = @import("build_options").app_context;

const Self = @This();

pub fn run(io: std.Io, pipe: nnng.Pipe.Sync) void {
    handleInput(io, std.heap.c_allocator, pipe) catch {};
}

fn handleInput(io: std.Io, allocator: std.mem.Allocator, pipe: nnng.Pipe.Sync) !void {
    prompt: {
        var buffer: [256]u8 = undefined;
        const stdout = std.Io.File.stdout();
        var writer = stdout.writer(io, &buffer);
        try writer.interface.writeAll("> ");
        try writer.interface.flush();
        break:prompt;
    }

    var buffer: [256]u8 = undefined;
    const stdin = std.Io.File.stdin();
    var reader = stdin.reader(io, &buffer);

    const input = reader.interface.takeDelimiterExclusive('\n') catch |err| {
        log: {
            var channel = try core.sockets.SendChannel.init(allocator, pipe.item.id, app_context, pipe.item.sender());
            defer channel.deinit();
            try sendLog(allocator, &channel, .err, "Could not read from stdin/ err: {}", .{err});
            break:log;
        }
        response: {
            var channel = try core.sockets.SendChannel.init(allocator, pipe.item.id, app_context, pipe.item.sender());
            defer channel.deinit();
            try sendResponse(allocator, &channel, .invalid, &.{});
            break:response;
        }
        return;
    };
    _ = try reader.interface.discardDelimiterInclusive('\n');

    var iter = std.mem.splitScalar(u8, std.mem.trim(u8, input, &std.ascii.whitespace), ' ');
    const s = iter.next();

    const command = 
        resolveCommand(s)
        orelse {
            if ((s != null) and (s.?.len > 0)) {
                log: {
                    var channel = try core.sockets.SendChannel.init(allocator, pipe.item.id, app_context, pipe.item.sender());
                    defer channel.deinit();
                    try sendLog(allocator, &channel, .err, "Undefined command: `{?s}`", .{s});
                    break:log;
                }
            }
            response: {
                var channel = try core.sockets.SendChannel.init(allocator, pipe.item.id, app_context, pipe.item.sender());
                defer channel.deinit();
                try sendResponse(allocator, &channel, .invalid, &.{});
                break:response;
            }
            return;
        }
    ;

    var channel = try core.sockets.SendChannel.init(allocator, pipe.item.id, app_context, pipe.item.sender());
    defer channel.deinit();
    try sendResponse(allocator, &channel, .{.accept = command}, std.mem.trimStart(u8, iter.rest(), &std.ascii.whitespace));
}

fn resolveCommand(input: ?Symbol) ?Response.Command {
    if (input == null) return null;
    if (input.?.len == 0) return null;

    const command_parser = clap.parsers.enumeration(Response.Command);
    return command_parser(input.?) catch null;
}

fn sendLog(allocator: std.mem.Allocator, channel: *core.sockets.SendChannel, comptime level: core.events.LogLevel, comptime fmt: []const u8, args: anytype) !void {
    var buffer = std.Io.Writer.Allocating.init(allocator);
    defer buffer.deinit();

    try buffer.writer.print(fmt, args);
    try buffer.writer.flush();

    const log: Event.Payload.Log = .{ .level = level, .content = buffer.written() };
    try channel.encode(.{.log = log});
    try channel.submit(.{});
} 

fn sendResponse(allocator: std.mem.Allocator, channel: *core.sockets.SendChannel, status: Response.Status, rest: Symbol) !void {
    const res: Response = .{
        .status = status,
        .rest = rest,
    };
    const data = try res.intoRaw(allocator);
    defer allocator.free(data);

    try channel.encode(.{.worker_response = data});
    try channel.submit(.{});
}

pub const Response = struct {
    status: Response.Status,
    rest: core.types.Symbol,

    pub const fromRaw = decodeFromRawBynary;
    pub const intoRaw = encodeIntoRawBynary;

    pub const Command = CommandArgIid(.{});
    pub const Status = union(enum) {
        invalid: void,
        accept: Command,
    };
};

fn encodeIntoRawBynary(res: *const Response, allocator: std.mem.Allocator) !core.types.BinaryData {
    var buffer = std.Io.Writer.Allocating.init(allocator);
    defer buffer.deinit();

    var writer = try cbor.CborStream.Writer.init(&buffer.writer);

    switch (res.status) {
        .invalid => {
            _ = try writer.writeEnum(std.meta.FieldEnum(Response.Status), .invalid);
            _ = try writer.writeString(res.rest);
        },
        .accept => |cmd| {
            _ = try writer.writeEnum(std.meta.FieldEnum(Response.Status), .accept);
            _ = try writer.writeEnum(Response.Command, cmd);
            _ = try writer.writeString(res.rest);
        }
    }

    try buffer.writer.flush();
    return buffer.toOwnedSlice();
}

fn decodeFromRawBynary(data: core.types.BinaryData) !Response {
    var reader = cbor.CborStream.Reader.createFromSlice(data);
    const tag = try reader.readEnum(std.meta.FieldEnum(Response.Status));

    switch (tag) {
        .invalid => {
            const rest = try reader.readString();
            return .{ .status = .invalid, .rest = rest };
        },
        .accept => {
            const status: Response.Status = .{ .accept = try reader.readEnum(Response.Command) };
            const rest = try reader.readString();
            return .{ .status = status, .rest = rest };
        },
    }
}

pub fn CommandArgIid(comptime descriptions: core.settings.types.DescriptionMap) type {
    return enum {
        help,
        quit,
        run,

        pub const Decls: []const clap.Param(@This()) = &.{
            .{.id = .help, .takes_value = .none},
            .{.id = .quit, .takes_value = .none},
            .{.id = .run, .takes_value = .none},
        };

        const arg_view = core.settings.types.ArgHelp(@This(), descriptions);
        pub const description = arg_view.description;
        pub const value = arg_view.value;        
    };
}