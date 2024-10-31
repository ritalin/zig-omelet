const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");

const Symbol = core.Symbol;
const worker_context = "command-pallet";

allocator: std.mem.Allocator,
buffer: std.ArrayList(u8),

const Self = @This();

pub fn init(allocator: std.mem.Allocator) !*Self {
    const self = try allocator.create(Self);
    self.* = .{
        .allocator = allocator,
        .buffer = std.ArrayList(u8).init(allocator),
    };

    return self;
}

pub fn deinit(self: *Self) void {
    self.buffer.deinit();
    self.allocator.destroy(self);
}

pub fn run(self: *Self, socket: *zmq.ZSocket) !void {
    const stdin = std.io.getStdIn().reader();

    const input = wait_input: while (true) {
        try stdin.streamUntilDelimiter(self.buffer.writer(), '\n', null);
        const s = try self.buffer.toOwnedSliceSentinel(0);

        if (std.mem.trim(u8, s, &.{ ' ', '\t' }).len > 0) {
            break:wait_input s;
        }
        self.allocator.free(s);
    };
    defer self.allocator.free(input);

    var tokens = std.zig.Tokenizer.init(input);
    const tk = tokens.next();
    switch (tk.tag) {
        .identifier => {
            const s = input[tk.loc.start..tk.loc.end];
            const cmd = std.meta.stringToEnum(Command, s);

            if (cmd == null) {
                try invalidCommand(self.allocator, socket, s);
            }
            else {
                try invokeCommand(self.allocator, socket, cmd.?, &tokens);
            }
        },
        else => {
            try invalidCommand(self.allocator, socket, input);
        }
    }
}

fn invokeCommand(allocator: std.mem.Allocator, socket: *zmq.ZSocket, command: Command, next_tokens: *std.zig.Tokenizer) !void {
    _ = next_tokens;

    var writer = try core.CborStream.Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeEnum(Status, .accept);
    _ = try writer.writeEnum(Command, command);

    const content = try writer.buffer.toOwnedSlice();
    defer allocator.free(content);

    const event: core.Event = .{ 
        .worker_response = try core.Event.Payload.WorkerResponse.init(allocator, .{ content })
    };
    defer event.deinit();

    try core.sendEvent(allocator, socket, .{.kind = .post, .from = worker_context, .event = event});
}

fn invalidCommand(allocator: std.mem.Allocator, socket: *zmq.ZSocket, command: Symbol) !void {
    const message = try std.fmt.allocPrint(allocator, "Invalid command: `{s}`\n", .{command});
    defer allocator.free(message);

    var writer = try core.CborStream.Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeEnum(Status, .invalid);
    _ = try writer.writeString(message);

    const content = try writer.buffer.toOwnedSlice();
    defer allocator.free(content);

    const event: core.Event = .{ 
        .worker_response = try core.Event.Payload.WorkerResponse.init(allocator, .{ content })
    };
    defer event.deinit();

    try core.sendEvent(allocator, socket, .{.kind = .post, .from = worker_context, .event = event});
}


pub const Command = enum(u8) {
    help = 1,
    quit,
    run,
};

pub const Status = enum(u8) {
    invalid = 1,
    accept,
};

const CommandHelp = std.StaticStringMap(Symbol).initComptime(.{
    .{ @tagName(.help), "Show help text" },
    .{ @tagName(.quit), "Exit this program" },
    .{ @tagName(.quit), "Run that invoked subcommand" },
});

pub fn showCommandhelp() !void {
    for (0..CommandHelp.kvs.len) |i| {
        std.debug.print("{s} {s}\n", .{CommandHelp.kvs.keys[i], CommandHelp.kvs.values[i]});
    }

    std.debug.print("\n", .{});
}