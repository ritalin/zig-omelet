const std = @import("std");

pub const Endpoints = struct {
    req_rep: Symbol,
    pub_sub: Symbol,
    push_pull: Symbol,
    worker: ?Symbol = null,
};

pub const FilePath = []const u8;
pub const Symbol = []const u8;
pub const SymbolZ = [:0]const u8;
pub const BinaryData = []const u8;
pub const StageName = []const u8;

pub const FilterKind = enum {include, exclude};

pub const ConfigCategory = enum {
    defaults,
    configs,

    pub fn destPath(self: ConfigCategory) Symbol {
        return @tagName(self);
    }

    pub fn templateDir(self: ConfigCategory) Symbol {
        return switch (self) {
            .defaults => "default-templates",
            .configs => @tagName(self),
        };
    }
};
pub const SubcommandArgId = enum {
    generate,
    @"init-default",
    @"init-config",
};
