const std = @import("std");
const known_folders = @import("known_folders");
const root = @import("../root.zig");

const types = root.types;

const Endpoint = root.configs.Endpoint;

pub fn createTmpDir() !std.testing.TmpDir {
    const io = std.testing.io;
    const allocator = std.testing.allocator;

    var random_bytes: [12]u8 = undefined;
    io.random(&random_bytes);
    var sub_path: [16]u8 = undefined;
    _ = std.base64.url_safe.Encoder.encode(&sub_path, &random_bytes);

    var env = try std.testing.environ.createMap(allocator);
    defer env.deinit();

    const tmp_dir: std.Io.Dir = (try known_folders.open(io, allocator, &env, .cache, .{})).?;
    defer tmp_dir.close(io);

    const parent_dir = try tmp_dir.createDirPathOpen(io, "omelet", .{});
    const dir = try parent_dir.createDirPathOpen(io, &sub_path, .{});

    return .{
        .dir = dir,
        .parent_dir = parent_dir,
        .sub_path = sub_path,
    };
}

pub fn createEndpoint(tmp_dir: std.testing.TmpDir, config: root.configs.Endpoint.Config) !types.Endpoints {
    const io = std.testing.io;
    const allocator = std.testing.allocator;

    if (comptime @import("builtin").os.tag != .windows) {
        const ep_config = try testEndpointConfig(io, allocator, &tmp_dir, config);
        defer {
            allocator.free(ep_config.channel_root);
            allocator.free(ep_config.channel_dir);
        }
        return Endpoint.runtimeIpc(allocator, ep_config);
    }
    else {
        return Endpoint.runtimeIpc(allocator, .{ 
            .channel_root = &tmp_dir.sub_path, 
            .channel_dir = config.channel_dir, 
            .worker_endpoint = config.worker_endpoint,
        });
    }
}

fn testEndpointConfig(io: std.Io, allocator: std.mem.Allocator, tmp_dir: *const std.testing.TmpDir, config: Endpoint.Config) !Endpoint.Config {
    const ep_dir = try tmp_dir.dir.createDirPathOpen(io, config.channel_dir, .{});
    defer ep_dir.close(io);

    const channel_root = try tmp_dir.parent_dir.realPathFileAlloc(io, ".", allocator);
    const channel_dir = try std.fmt.allocPrintSentinel(allocator, "{f}", .{ std.fs.path.fmtJoin(&.{ &tmp_dir.sub_path, config.channel_dir }) }, 0);

    return .{
        .channel_root = channel_root,
        .channel_dir = channel_dir,
        .worker_endpoint = config.worker_endpoint,
    };
}

pub fn releaseEndpoint(endpoint: types.Endpoints) void {
    const allocator = std.testing.allocator;
    allocator.free(endpoint.req_rep);
    allocator.free(endpoint.pub_sub);
    allocator.free(endpoint.push_pull);
}
