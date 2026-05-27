const std = @import("std");
const builtin = @import("builtin");

const root = @import("./root.zig");
const types = root.types;
const events = root.events;

var level_filter = resetFilter(if (builtin.mode == .Debug) .debug else .info);

pub const IntegratedHandler = struct {
    ptr: *anyopaque,
    handler: *const fn (ptr: *anyopaque, level: events.LogLevel, msg: []const u8) anyerror!void,
};

var on_integrated: ?IntegratedHandler = null;

pub fn putAppLog(terminal: std.Io.Terminal, level: events.LogLevel, stage_name: types.StageName, comptime fmt: []const u8, args: anytype) std.Io.Terminal.SetColorError!void {
    level: {
        try putLogLevel(terminal, level);
        break:level;
    }
    stage: {
        try terminal.writer.writeByte(' ');
        try putStageName(terminal, stage_name);
        try terminal.writer.writeByte(' ');
        break:stage;
    }
    msg: {
        try terminal.writer.print(fmt ++ "\n", args);
        break:msg;
    }
}

fn putLogLevel(terminal: std.Io.Terminal, level: events.LogLevel) std.Io.Terminal.SetColorError!void {
    try terminal.setColor(switch (level) {
        .err => .red,
        .warn => .yellow,
        .info => .green,
        .debug => .blue,
        .trace => .magenta,
    });
    try terminal.writer.writeAll(level.asText());
}

fn putStageName(terminal: std.Io.Terminal, stage_name: types.StageName) std.Io.Terminal.SetColorError!void {
    try terminal.setColor(.reset);
    try terminal.writer.writeByte('[');

    try terminal.setColor(.bold);
    try terminal.writer.writeAll(stage_name);

    try terminal.setColor(.reset);
    try terminal.writer.writeByte(']');
}

// TODO:
// pub fn withAppContext(comptime app_context: Symbol) type {
//     return struct {
//         allocator: std.mem.Allocator,
//         dispatcher: *Connection.EventDispatcher(app_context),
//         stand_alone: bool,

//         const Self = @This();

//         pub fn init(allocator: std.mem.Allocator, dispatcher: *Connection.EventDispatcher(app_context), stand_alone: bool) Self {
//             return .{
//                 .allocator = allocator,
//                 .dispatcher = dispatcher,
//                 .stand_alone = stand_alone,
//             };
//         }

//         pub fn log(self: *Self, log_level: events.LogLevel, comptime content: Symbol, args: anytype) !void {
//             if (level_filter.contains(log_level)) {
//                 var buf = std.ArrayList(u8).init(self.allocator);
//                 defer buf.deinit();
//                 const writer = buf.writer();

//                 try std.fmt.format(writer, content, args);

//                 if (self.stand_alone) {
//                     Stage.log(log_level, app_context, buf.items);
//                 }

//                 const event: events.Event = .{
//                     .log = try events.Event.Payload.Log.init(
//                         self.allocator, .{log_level, buf.items}
//                     )
//                 };

//                 try self.dispatcher.post(event);
//             }
//         }
//     };
// }

// /// Log received from stage
// pub const Stage = struct {
//     pub fn log(level: events.LogLevel, stage_name: types.Symbol, message: []const u8) void {
//         if (level_filter.contains(level)) {
//             const log_level = level.toStdLevel();
            
//             switch (level.ofScope()) {
//                 .trace => Scoped(.trace).log(log_level, stage_name, message),
//                 else => Scoped(.default).log(log_level, stage_name, message),
//             }
//         }
//     }

//     fn Scoped(comptime scope: @Type(.enum_literal)) type {
//         return struct {
//             pub fn log(level: std.log.Level, stage_name: types.Symbol, message: []const u8) void {
//                 switch (level) {
//                     .err => std.log.scoped(scope).err("[{s}] {s}", .{stage_name, message}),
//                     .warn => std.log.scoped(scope).warn("[{s}] {s}", .{stage_name, message}),
//                     .info => std.log.scoped(scope).info("[{s}] {s}", .{stage_name, message}),
//                     .debug => std.log.scoped(scope).debug("[{s}] {s}", .{stage_name, message}),
//                 }
//             }
//         };
//     }
// };

// fn ArgsWithStageName(type_info: std.builtin.Type) type {
//     const info = type_info.@"struct";

//     comptime var item_types: [info.fields.len+1]type = undefined;

//     item_types[0] = types.Symbol;

//     inline for (info.fields, 1..) |f, i| {
//         item_types[i] = f.type;
//     }

//     return std.meta.Tuple(&item_types);
// }

// fn directLogArgs(stage_name: types.Symbol, args: anytype) ArgsWithStageName(@typeInfo(@TypeOf(args))) {
//     var result: ArgsWithStageName(@typeInfo(@TypeOf(args))) = undefined;

//     result[0] = stage_name;

//     inline for (std.meta.fields(@TypeOf(args)), 1..) |f, i| {
//         result[i] = @field(args, f.name);
//     }

//     return result;
// }

// fn Direct(comptime stage_name: types.Symbol, comptime scope: @Type(.enum_literal)) type {
//     const S = std.log.scoped(scope);

//     return struct {
//         pub fn err(comptime message: []const u8, args: anytype) void {
//             if (log_disabled) return;
//             S.err("[{s}] " ++ message, directLogArgs(stage_name, args));
//         }
//         pub fn warn(comptime message: []const u8, args: anytype) void {
//             if (log_disabled) return;
//             if (! level_filter.contains(.warn)) return;

//             S.warn("[{s}] " ++ message, directLogArgs(stage_name, args));
//         }
//         pub fn info(comptime message: []const u8, args: anytype) void {
//             if (log_disabled) return;
//             if (! level_filter.contains(.info)) return;

//             S.info("[{s}] " ++ message, directLogArgs(stage_name, args));
//         }
//         pub fn debug(comptime message: []const u8, args: anytype) void {
//             if (log_disabled) return;
//             if ((scope == .trace) and (! level_filter.contains(.trace))) return;
//             if (! level_filter.contains(.debug)) return;

//             S.debug("[{s}] " ++ message, directLogArgs(stage_name, args));
//         }
//     };
// }

// pub fn SystemDirect(comptime stage_name: types.Symbol) type {
//     return Direct(stage_name, .default);
// }
// pub fn TraceDirect(comptime stage_name: types.Symbol) type {
//     return Direct(stage_name, .trace);
// }

// pub fn filterWith(level: events.LogLevel) void {
//     level_filter = resetFilter(level);
// }

fn resetFilter(level: events.LogLevel) events.LogLevelSet {
    var filter = events.LogLevelSet.initFull();

    const field_len = std.meta.fields(events.LogLevel).len;
    for (@intFromEnum(level)+1..field_len) |value| {
        filter.remove(@enumFromInt(value));
    }

    return filter;
}

pub fn enableIntegratedLog(handler: IntegratedHandler) void {
    on_integrated = handler;
}

pub fn forwardIntegratedLog(comptime level: std.log.Level, comptime scope: @EnumLiteral(), comptime format: []const u8, args: anytype) void {
    if (on_integrated == null) {
        return std.log.defaultLog(level, scope, format, args);
    }

    var buffer: std.Io.Writer.Allocating = .init(std.heap.c_allocator);
    defer buffer.deinit();

    buffer.writer.print(format, args) catch { return; };

    const new_level = switch (level) {
        .err => .err,
        .warn => .warn,
        .info => .info,
        .debug => if (scope == .trace) .trace else .debug,
    };

    (on_integrated.?.handler)(on_integrated.?.ptr, new_level, buffer.writer.buffered()) catch {};
}

// pub fn stringToLogLevel(s: types.Symbol) events.LogLevel {
//     return std.meta.stringToEnum(events.LogLevel, s) orelse .err;
// }

// pub fn disable() void {
//     log_disabled = true;
// }

test "log test" {
    std.testing.refAllDecls(@This());
}

pub const tests = struct {
    test "colored log level" {
        var buffer = std.Io.Writer.Allocating.init(std.testing.allocator);
        defer buffer.deinit();

        const terminal: std.Io.Terminal = .{ .writer = &buffer.writer, .mode = .escape_codes };

        err: {
            terminal.writer.end = 0;
            try putLogLevel(terminal, .err);
            try std.testing.expectEqualStrings("\x1b[31mERROR", terminal.writer.buffered());
            break:err;
        }
        warn: {
            terminal.writer.end = 0;
            try putLogLevel(terminal, .warn);
            try std.testing.expectEqualStrings("\x1b[33mWARN", terminal.writer.buffered());
            break:warn;
        }
        info: {
            terminal.writer.end = 0;
            try putLogLevel(terminal, .info);
            try std.testing.expectEqualStrings("\x1b[32mINFO", terminal.writer.buffered());
            break:info;
        }
        debug: {
            terminal.writer.end = 0;
            try putLogLevel(terminal, .debug);
            try std.testing.expectEqualStrings("\x1b[34mDEBUG", terminal.writer.buffered());
            break:debug;
        }
        trace: {
            terminal.writer.end = 0;
            try putLogLevel(terminal, .trace);
            try std.testing.expectEqualStrings("\x1b[35mTRACE", terminal.writer.buffered());
            break:trace;
        }
    }

    test "stage name" {
        var buffer = std.Io.Writer.Allocating.init(std.testing.allocator);
        defer buffer.deinit();

        const terminal: std.Io.Terminal = .{ .writer = &buffer.writer, .mode = .escape_codes };

        try putStageName(terminal, "runner");
        try std.testing.expectEqualStrings("\x1b[0m[\x1b[1mrunner\x1b[0m]", terminal.writer.buffered());
    }

    test "app log" {
        var buffer = std.Io.Writer.Allocating.init(std.testing.allocator);
        defer buffer.deinit();

        const terminal: std.Io.Terminal = .{ .writer = &buffer.writer, .mode = .escape_codes };

        try putAppLog(terminal, .debug, "runner", "{} {s} {}", .{1, "foo", 3});
        const expected = "\x1b[34mDEBUG \x1b[0m[\x1b[1mrunner\x1b[0m] 1 foo 3\n";
        try std.testing.expectEqualStrings(expected, terminal.writer.buffered());
    }

    const TestIntegratedAdapter = struct {
        buffer: *std.Io.Writer.Allocating,

        const stage_name = "stage";
        const Adapter = @This();

        fn handle(ptr: *anyopaque, level: events.LogLevel, msg: []const u8) anyerror!void {
            const self: *Adapter = @ptrCast(@alignCast(ptr));

            const terminal: std.Io.Terminal = .{ .writer = &self.buffer.writer, .mode = .escape_codes };
            try putAppLog(terminal, level, stage_name, "{s}", .{ msg });
        }
    };

    test "integrated app log" {
        var buffer = std.Io.Writer.Allocating.init(std.testing.allocator);
        defer buffer.deinit();

        var adapter: TestIntegratedAdapter = .{ .buffer = &buffer };

        enableIntegratedLog(.{ .ptr = &adapter, .handler = TestIntegratedAdapter.handle });
        defer on_integrated = null;

        info: {
            buffer.writer.end = 0;
            forwardIntegratedLog(.info, .app, "{s}", .{ "quaxx" });
            const expected = "\x1b[32mINFO \x1b[0m[\x1b[1mstage\x1b[0m] quaxx\n";
            try std.testing.expectEqualStrings(expected, buffer.writer.buffered());
            break:info;
        }
        debug: {
            buffer.writer.end = 0;
            forwardIntegratedLog(.debug, .app, "{s}", .{ "quaxx" });
            const expected = "\x1b[34mDEBUG \x1b[0m[\x1b[1mstage\x1b[0m] quaxx\n";
            try std.testing.expectEqualStrings(expected, buffer.writer.buffered());
            break:debug;
        }
    }

    test "integrated trace log" {
        var buffer = std.Io.Writer.Allocating.init(std.testing.allocator);
        defer buffer.deinit();

        var adapter: TestIntegratedAdapter = .{ .buffer = &buffer };
    
        enableIntegratedLog(.{ .ptr = &adapter, .handler = TestIntegratedAdapter.handle });
        defer on_integrated = null;

        info: {
            buffer.writer.end = 0;
            forwardIntegratedLog(.info, .trace, "{s}", .{ "quaxx" });
            const expected = "\x1b[32mINFO \x1b[0m[\x1b[1mstage\x1b[0m] quaxx\n";
            try std.testing.expectEqualStrings(expected, buffer.writer.buffered());
            break:info;
        }
        debug: {
            buffer.writer.end = 0;
            forwardIntegratedLog(.debug, .trace, "{s}", .{ "quaxx" });
            const expected = "\x1b[35mTRACE \x1b[0m[\x1b[1mstage\x1b[0m] quaxx\n";
            try std.testing.expectEqualStrings(expected, buffer.writer.buffered());
            break:debug;
        }
    }
};