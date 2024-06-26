const std = @import("std");
const core = @import("core");

const Config = @This();

stage_watch: Config.Stage,
stage_extract: []const Config.Stage,
stage_generate: []const Config.Stage,

pub const Stage = struct {
    path: core.FilePath,
    extra_args: []const core.Symbol,
    managed: bool,
};

pub const StageCount = std.enums.EnumFieldStruct(std.meta.FieldEnum(Config), usize, 0);

pub fn stageCount(self: Config) StageCount {
    return .{
        .stage_watch = 1,
        .stage_extract = self.stage_extract.len,
        .stage_generate = self.stage_generate.len,
    };
}
