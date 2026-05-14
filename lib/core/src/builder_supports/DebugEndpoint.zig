const std = @import("std");

const types = @import("../types.zig");

/// Ports
const CMD_S2C_PORT = std.fmt.comptimePrint("ipc://{s}/default/{s}", .{types.CHANNEL_ROOT, types.PUBSUB_PORT});
const PUSH_C2S_PORT = std.fmt.comptimePrint("ipc://{s}/default/{s}", .{types.CHANNEL_ROOT, types.PUSHPULL_PORT});
const REQ_C2S_PORT = std.fmt.comptimePrint("ipc://{s}/default/{s}", .{types.CHANNEL_ROOT, types.REQ_PORT});

const StageChannel = std.StaticStringMap(types.Symbol).initComptime(.{
    .{"--request-channel", std.fmt.comptimePrint("--request-channel={s}", .{REQ_C2S_PORT})},
    .{"--subscribe-channel", std.fmt.comptimePrint("--subscribe-channel={s}", .{CMD_S2C_PORT})},
    .{"--push-channel", std.fmt.comptimePrint("--push-channel={s}", .{PUSH_C2S_PORT})},
});

pub const Endpoint: types.Endpoints = .{
    .req_rep = REQ_C2S_PORT,
    .push_pull = PUSH_C2S_PORT,
    .pub_sub = CMD_S2C_PORT,
};

pub fn applyStageChannel(runner: *std.Build.Step.Run) !void {
    for (StageChannel.keys()) |k| {
        arg: {
            for (runner.argv.items) |arg| {
                if (std.meta.activeTag(arg) == .bytes) {
                    if (std.mem.startsWith(u8, arg.bytes, k)) break :arg;
                }
            }
            runner.addArg(StageChannel.get(k).?);
        }
    }

    runner.addArg("--standalone");
}

const RunnerChannel = std.StaticStringMap(types.Symbol).initComptime(.{
    .{"--reqrep-channel", std.fmt.comptimePrint("--reqrep-channel={s}", .{REQ_C2S_PORT})},
    .{"--pubsub-channel", std.fmt.comptimePrint("--pubsub-channel={s}", .{CMD_S2C_PORT})},
});

pub fn applyRunnerChannel(runner: *std.Build.Step.Run) void {
    for (RunnerChannel.keys()) |k| {
        arg: {
            for (runner.argv.items) |arg| {
                if (std.meta.activeTag(arg) == .bytes) {
                    if (std.mem.startsWith(u8, arg.bytes, k)) break :arg;
                }
            }
            runner.addArg(RunnerChannel.get(k).?);
        }
    }
}
