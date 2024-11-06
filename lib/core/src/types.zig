const std = @import("std");

pub const IPC_PROTOCOL = "ipc://";
// IPC channel root directory
pub const CHANNEL_ROOT = "/tmp/duckdb-ext-ph";

//
// Channel endpoints
//
pub const REQ_PORT = "req_c2s";
pub const PUBSUB_PORT = "cmd_s2c";

pub const Endpoints = struct {
    req_rep: Symbol,
    pub_sub: Symbol,
};

pub const FilePath = []const u8;
pub const Symbol = []const u8;

pub const LogScope = enum {
    trace, default,
};

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
};
