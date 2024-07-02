const std = @import("std");
const types = @import("../types.zig");

pub fn resolveLogLevel(value: ?types.Symbol) !types.LogLevel {
    if (value) |v| {
        return std.meta.stringToEnum(types.LogLevel, v)
        orelse return error.SettingLoadFailed;
    }
    else {
        return .info;
    }

}