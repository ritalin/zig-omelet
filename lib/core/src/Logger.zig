const std = @import("std");
const zmq = @import("zmq");

const types = @import("./types.zig");
const Symbol = types.Symbol;
const Connection = @import("./sockets/Connection.zig");


var level_filter = resetFilter(.info);

pub fn withAppContext(comptime app_context: Symbol) type {
    return struct {
        allocator: std.mem.Allocator,
        dispatcher: *Connection.EventDispatcher(app_context),
        stand_alone: bool,

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator, dispatcher: *Connection.EventDispatcher(app_context), stand_alone: bool) Self {
            return .{
                .allocator = allocator,
                .dispatcher = dispatcher,
                .stand_alone = stand_alone,
            };
        }

        pub fn log(self: *Self, log_level: types.LogLevel, comptime content: Symbol, args: anytype) !void {
            if (level_filter.contains(log_level)) {
                var buf = std.ArrayList(u8).init(self.allocator);
                defer buf.deinit();
                const writer = buf.writer();

                try std.fmt.format(writer, content, args);

                if (self.stand_alone) {
                    Stage.log(log_level, app_context, buf.items);
                }

                const event: types.Event = .{
                    .log = try types.Event.Payload.Log.init(
                        self.allocator, .{log_level, buf.items}
                    )
                };

                try self.dispatcher.post(event);
            }
        }
    };
}

/// Log received from stage
pub const Stage = struct {
    pub fn log(level: types.LogLevel, stage_name: types.Symbol, message: []const u8) void {
        if (level_filter.contains(level)) {
            const log_level = level.toStdLevel();
            
            switch (level.ofScope()) {
                .trace => Scoped(.trace).log(log_level, stage_name, message),
                else => Scoped(.default).log(log_level, stage_name, message),
            }
        }
    }

    fn Scoped(comptime scope: @Type(.EnumLiteral)) type {
        return struct {
            pub fn log(level: std.log.Level, stage_name: types.Symbol, message: []const u8) void {
                switch (level) {
                    .err => std.log.scoped(scope).err("[{s}] {s}", .{stage_name, message}),
                    .warn => std.log.scoped(scope).warn("[{s}] {s}", .{stage_name, message}),
                    .info => std.log.scoped(scope).info("[{s}] {s}", .{stage_name, message}),
                    .debug => std.log.scoped(scope).debug("[{s}] {s}", .{stage_name, message}),
                }
            }
        };
    }
};

fn ArgsWithStageName(type_info: std.builtin.Type) type {
    const info = type_info.Struct;

    comptime var item_types: [info.fields.len+1]type = undefined;

    item_types[0] = types.Symbol;

    inline for (info.fields, 1..) |f, i| {
        item_types[i] = f.type;
    }

    return std.meta.Tuple(&item_types);
}

fn directLogArgs(stage_name: types.Symbol, args: anytype) ArgsWithStageName(@typeInfo(@TypeOf(args))) {
    var result: ArgsWithStageName(@typeInfo(@TypeOf(args))) = undefined;

    result[0] = stage_name;

    inline for (std.meta.fields(@TypeOf(args)), 1..) |f, i| {
        result[i] = @field(args, f.name);
    }

    return result;
}

fn Direct(comptime stage_name: types.Symbol, comptime scope: @Type(.EnumLiteral)) type {
    const S = std.log.scoped(scope);

    return struct {
        pub fn err(comptime message: []const u8, args: anytype) void {
            S.err("[{s}] " ++ message, directLogArgs(stage_name, args));
        }
        pub fn warn(comptime message: []const u8, args: anytype) void {
            if (! level_filter.contains(.warn)) return;

            S.warn("[{s}] " ++ message, directLogArgs(stage_name, args));
        }
        pub fn info(comptime message: []const u8, args: anytype) void {
            if (! level_filter.contains(.info)) return;

            S.info("[{s}] " ++ message, directLogArgs(stage_name, args));
        }
        pub fn debug(comptime message: []const u8, args: anytype) void {
            if ((scope == .trace) and (! level_filter.contains(.trace))) return;
            if (! level_filter.contains(.debug)) return;

            S.debug("[{s}] " ++ message, directLogArgs(stage_name, args));
        }
    };
}

pub fn SystemDirect(comptime stage_name: types.Symbol) type {
    return Direct(stage_name, .default);
}
pub fn TraceDirect(comptime stage_name: types.Symbol) type {
    return Direct(stage_name, .trace);
}

pub fn filterWith(level: types.LogLevel) void {
    level_filter = resetFilter(level);
}

fn resetFilter(level: types.LogLevel) types.LogLevelSet {
    var filter = types.LogLevelSet.initFull();

    const field_len = std.meta.fields(types.LogLevel).len;
    for (@intFromEnum(level)+1..field_len) |value| {
        filter.remove(@enumFromInt(value));
    }

    return filter;
}

pub fn stringToLogLevel(s: types.Symbol) types.LogLevel {
    return std.meta.stringToEnum(types.LogLevel, s) orelse .err;
}
