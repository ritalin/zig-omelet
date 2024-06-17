const std = @import("std");
const zmq = @import("zmq");

const types = @import("./types.zig");
const Symbol = types.Symbol;
const Connection = @import("./sockets/Connection.zig");

const Self = @This();

allocator: std.mem.Allocator,
app_context: Symbol,
connection: *Connection.Client,
stand_alone: bool,

pub fn init(allocator: std.mem.Allocator, app_context: Symbol, connection: *Connection.Client, stand_alone: bool) Self {
    return .{
        .allocator = allocator,
        .app_context = app_context,
        .connection = connection,
        .stand_alone = stand_alone,
    };
}

pub fn log(self: *Self, log_level: types.LogLevel, comptime content: Symbol, args: anytype) !void {
    var buf = std.ArrayList(u8).init(self.allocator);
    defer buf.deinit();
    const writer = buf.writer();

    try std.fmt.format(writer, "[{s}] ", .{self.app_context});
    try std.fmt.format(writer, content, args);

    // const log_message = try buf.toOwnedSlice();
    // defer self.allocator.free(log_message);
    const log_message = buf.items;

    if (self.stand_alone) {
        Server.log(log_level, log_message);
    }
    else {
        Server.log(log_level, log_message);

        try self.connection.dispatcher.post(.{
            .log = try types.EventPayload.Log.init(
                self.allocator, log_level, log_message
            )
        });
    }
    // Server.log(log_level, "End output log");
}

pub fn Scoped(comptime scope: @Type(.EnumLiteral)) type {
    return struct {
        pub fn log(level: std.log.Level, message: []const u8) void {
            switch (level) {
                .err => std.log.scoped(scope).err("{s}", .{message}),
                .warn => std.log.scoped(scope).warn("{s}", .{message}),
                .info => std.log.scoped(scope).info("{s}", .{message}),
                .debug => std.log.scoped(scope).debug("{s}", .{message}),
            }
        }
    };
}

pub const Server = struct {
    pub const systemLog = std.log.scoped(.default);
    pub const traceLog = std.log.scoped(.trace);

    pub fn log(level: types.LogLevel, message: []const u8) void {
        const log_level = level.toStdLevel();
        
        switch (level.ofScope()) {
            .trace => Scoped(.trace).log(log_level, message),
            else => Scoped(.default).log(log_level, message),
        }
    }
};
