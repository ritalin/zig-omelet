const std = @import("std");
const root = @import("../root.zig");

pub fn resolveGuestLogStyle(s: ?root.types.Symbol) ?root.Logger.LogStyle {
    if (s == null) return null;
    const tag = std.meta.stringToEnum(std.meta.FieldEnum(root.Logger.LogStyle), s.?) orelse return null;

    return switch (tag) {
        .stderr => .stderr,
        .discard => .discard,
        .integrated => .{.integrated = .batch},
    };
}