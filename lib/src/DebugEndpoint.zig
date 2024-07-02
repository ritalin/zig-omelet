const std = @import("std");

const types = @import("./types.zig");

// /// (control) Server -> Client
const CMD_S2C_BIND_PORT = std.fmt.comptimePrint("ipc://{s}/default/cmd_s2c", .{types.CHANNEL_ROOT});
const CMD_S2C_CONN_PORT = std.fmt.comptimePrint("ipc://{s}/default/cmd_s2c", .{types.CHANNEL_ROOT});
// /// (source) Client -> Server
const REQ_C2S_BIND_PORT = std.fmt.comptimePrint("ipc://{s}/default/req_c2s", .{types.CHANNEL_ROOT});
const REQ_C2S_CONN_PORT = std.fmt.comptimePrint("ipc://{s}/default/req_c2s", .{types.CHANNEL_ROOT});

const StageChannel = std.StaticStringMap(types.Symbol).initComptime(.{
    .{"--request-channel", std.fmt.comptimePrint("--request-channel={s}", .{REQ_C2S_CONN_PORT})},
    .{"--subscribe-channel", std.fmt.comptimePrint("--subscribe-channel={s}", .{CMD_S2C_CONN_PORT})},
});

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
    .{"--reqrep-channel", std.fmt.comptimePrint("--reqrep-channel={s}", .{REQ_C2S_BIND_PORT})},
    .{"--pubsub-channel", std.fmt.comptimePrint("--pubsub-channel={s}", .{CMD_S2C_BIND_PORT})},
});

pub fn applyRunnerChannel(runner: *std.Build.Step.Run, subcommand: types.Symbol) void {
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

    runner.addArg(subcommand);
}