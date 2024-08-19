const std = @import("std");
const Symbol = @import("../types.zig").Symbol;
const LogLevel = @import("../events/events.zig").LogLevel;

pub fn resolveLogLevel(value: ?Symbol) !LogLevel {
    if (value) |v| {
        return std.meta.stringToEnum(LogLevel, v)
        orelse return error.SettingLoadFailed;
    }
    else {
        return .info;
    }

}