const std = @import("std");
const root = @import("../root.zig");

const Symbol = root.types.Symbol;
const FilePath = root.types.FilePath;

pub const StageKind = enum { watch, extract, generate, init };
pub const StageStrategyKind = enum {one, many, optional};
pub const StageStrategy = std.enums.EnumMap(StageKind, StageStrategyKind);
pub const ConfigFileCandidates = std.enums.EnumFieldStruct(enum {current_dir, home_dir, executable_dir}, ?FilePath, @as(?FilePath, null));

pub const ConfigCategory = enum {
    defaults,
    configs,
    settings,

    pub fn destPath(self: ConfigCategory) Symbol {
        return @tagName(self);
    }

    pub fn templateDir(self: ConfigCategory) Symbol {
        return switch (self) {
            .defaults => "defaults",
            .configs, .settings => @tagName(self),
        };
    }
};