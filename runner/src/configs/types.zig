const std = @import("std");
const core = @import("core");

const default_args = @import("../settings/default_args.zig");
const DufaultArg = default_args.DufaultArg;

const HeartbeatTask = @import("../tasks/HeartbeatTask.zig");

pub const Host = struct {
    heartbeat_interval: std.Io.Duration,
    heartbeat_limit: HeartbeatTask.Limit,
    ready_progress_interval: std.Io.Duration,
};

pub const Guest = struct {
    name: core.types.Symbol,
    location: core.types.FilePath,
    kind: core.configs.types.StageKind,
    mode: Guest.Mode = .managed,
    extra_args: ExtraArgSet,

    pub const Mode = enum { daemon, managed };
    pub const ExtraArgSet = union(core.configs.types.StageKind) {
        watch: ExtraArg(core.configs.guests.GuestWatch.ArgId(.{})),
        extract: ExtraArg(core.configs.guests.GuestExtract.ArgId(.{})),
        generate: ExtraArg(core.configs.guests.GuestGenerate.ArgId(.{})),
        init: ExtraArg(core.configs.guests.GuestInitialze.ArgId(.{})),
    };
};

pub fn ExtraArg(comptime ArgId: type) type {
    return std.enums.EnumFieldStruct(ArgId, DufaultArg, .default);
}

pub const path_candidates: core.configs.types.ConfigFileCandidates = .{
    .current_dir = ".omelet",
    .home_dir = ".omelet",
    .executable_dir = "",
};
