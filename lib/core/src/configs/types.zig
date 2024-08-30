const std = @import("std");

pub const StageCategory = enum {
    stage_watch,
    stage_extract,
    stage_generate,
};

pub const StageStrategy = std.enums.EnumMap(StageCategory, enum {one, many, optional});