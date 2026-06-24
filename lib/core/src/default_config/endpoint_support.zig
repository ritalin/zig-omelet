const std = @import("std");
const builtin = @import("builtin");

const types = @import("../types.zig");

// default IPC channel root directory
pub const CHANNEL_ROOT = if (builtin.os.tag != .windows) "/tmp/omelet" else "omelet";
pub const CHANNEL_DIR = "default";

//
// Channel endpoints
//
const CMD_S2C_PORT = "cmd_s2c";
const PUSH_C2S_PORT = "push_c2s";
const REQ_C2S_PORT = "req_c2s";

pub const WORKER_ENDPOINT = "inproc://sync-thread";

pub const Config = struct {
    channel_root: types.SymbolZ = CHANNEL_ROOT,
    channel_dir: types.SymbolZ = CHANNEL_DIR,
    worker_endpoint: ?types.SymbolZ = null,

    pub const default: Config = .{};
};

pub fn renewIpcConfig(io: std.Io, allocator: std.mem.Allocator, config: *const Config) !Config {
    var random_bytes: [12]u8 = undefined;
    io.random(&random_bytes);
    var sub_path: [24]u8 = undefined;
    _ = std.base64.url_safe.Encoder.encode(&sub_path, &random_bytes);

    return .{
        .channel_root = try allocator.dupeSentinel(u8, config.channel_root, 0),
        .channel_dir = try allocator.dupeSentinel(u8, &sub_path, 0),
        .worker_endpoint = if (config.worker_endpoint) |worker| try allocator.dupeSentinel(u8, worker, 0) else null,
    };
}

pub fn releaseIpcConfig(allocator: std.mem.Allocator, config: *Config) void {
    allocator.free(config.channel_root);
    allocator.free(config.channel_dir);
    if (config.worker_endpoint) |worker| allocator.free(worker);
}

pub fn comptimeIpc(comptime config: @This().Config) types.Endpoints {
    return .{
        .req_rep = std.fmt.comptimePrint("ipc://{s}/{s}/{s}", .{config.channel_root, config.channel_dir, REQ_C2S_PORT}),
        .pub_sub = std.fmt.comptimePrint("ipc://{s}/{s}/{s}", .{config.channel_root, config.channel_dir, CMD_S2C_PORT}),
        .push_pull = std.fmt.comptimePrint("ipc://{s}/{s}/{s}", .{config.channel_root, config.channel_dir, PUSH_C2S_PORT}),
        .worker = config.worker_endpoint,
    };
}

pub fn runtimeIpc(allocator: std.mem.Allocator, config: @This().Config) !types.Endpoints {
    return .{
        .req_rep = try std.fmt.allocPrint(allocator, "ipc://{s}/{s}/{s}", .{config.channel_root, config.channel_dir, REQ_C2S_PORT}),
        .pub_sub = try std.fmt.allocPrint(allocator, "ipc://{s}/{s}/{s}", .{config.channel_root, config.channel_dir, CMD_S2C_PORT}),
        .push_pull = try std.fmt.allocPrint(allocator, "ipc://{s}/{s}/{s}", .{config.channel_root, config.channel_dir, PUSH_C2S_PORT}),
        .worker = config.worker_endpoint,
    };
}

pub fn releaseRuntimeIpc(allocator: std.mem.Allocator, endpoints: *types.Endpoints) void {
    allocator.free(endpoints.req_rep);
    allocator.free(endpoints.pub_sub);
    allocator.free(endpoints.push_pull);
}

pub fn createIpcStorage(io: std.Io, config: *const @This().Config) !void {
    if (builtin.os.tag != .windows) {
        const dir = try std.Io.Dir.cwd().createDirPathOpen(io, config.channel_root, .{});
        defer dir.close(io);

        try dir.createDirPath(io, config.channel_dir);
    }
}

pub fn releaseIpcStorage(io: std.Io, config: *const @This().Config) void {
    const dir = std.Io.Dir.cwd().openDir(io, config.channel_root, .{}) catch return;
    defer dir.close(io);

    dir.deleteTree(io, config.channel_dir) catch {};
}

// TODO:
// const StageChannel = std.StaticStringMap(types.Symbol).initComptime(.{
//     .{"--request-channel", std.fmt.comptimePrint("--request-channel={s}", .{REQ_C2S_PORT})},
//     .{"--subscribe-channel", std.fmt.comptimePrint("--subscribe-channel={s}", .{CMD_S2C_PORT})},
//     .{"--push-channel", std.fmt.comptimePrint("--push-channel={s}", .{PUSH_C2S_PORT})},
// });
//
// pub fn applyStageChannel(runner: *std.Build.Step.Run) !void {
//     for (StageChannel.keys()) |k| {
//         arg: {
//             for (runner.argv.items) |arg| {
//                 if (std.meta.activeTag(arg) == .bytes) {
//                     if (std.mem.startsWith(u8, arg.bytes, k)) break :arg;
//                 }
//             }
//             runner.addArg(StageChannel.get(k).?);
//         }
//     }

//     runner.addArg("--standalone");
// }

// const RunnerChannel = std.StaticStringMap(types.Symbol).initComptime(.{
//     .{"--reqrep-channel", std.fmt.comptimePrint("--reqrep-channel={s}", .{REQ_C2S_PORT})},
//     .{"--pubsub-channel", std.fmt.comptimePrint("--pubsub-channel={s}", .{CMD_S2C_PORT})},
// });

// pub fn applyRunnerChannel(runner: *std.Build.Step.Run) void {
//     for (RunnerChannel.keys()) |k| {
//         arg: {
//             for (runner.argv.items) |arg| {
//                 if (std.meta.activeTag(arg) == .bytes) {
//                     if (std.mem.startsWith(u8, arg.bytes, k)) break :arg;
//                 }
//             }
//             runner.addArg(RunnerChannel.get(k).?);
//         }
//     }
// }
