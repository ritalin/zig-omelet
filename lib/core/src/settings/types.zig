const std = @import("std");

pub fn LoadResult(comptime Result: type, comptime HelpSettingType: type) type {
    return union(enum) {
        success: Result,
        help: HelpSettingType,
    };
}

pub const DescriptionItem = struct {
    desc: []const u8, 
    value: []const u8, 
    required: bool = false,
};

pub const DescriptionMap = std.StaticStringMap(DescriptionItem);

pub fn ArgHelp(comptime Id: type, comptime _description: DescriptionMap) type {
    return struct {
        pub fn description(self: Id) []const u8 {
            const usage = _description.get(@tagName(self)) orelse return "";
            return usage.desc;
        }
        pub fn value(self: Id) []const u8 {
            const usage = _description.get(@tagName(self)) orelse return "VALUE";
            return usage.value;
        }
    };
}

pub const ArgScanner = @import("./args_parser.zig").ArgScanner;
pub const ArgParserPair = @import("./args_parser.zig").ArgParserPair;

test "setting test" {
    std.testing.refAllDecls(@This());
}