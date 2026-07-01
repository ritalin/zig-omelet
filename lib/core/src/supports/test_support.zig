const std = @import("std");
const builtin = @import("builtin");

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

// TODO:
// pub fn createEndpoint(tmp_dir: std.testing.TmpDir) !types.Endpoints {
//     const io = std.testing.io;
//     const allocator = std.testing.allocator;

//     if (builtin.os.tag != .windows) {
//         const ep_config = try testEndpointConfig(io, allocator, &tmp_dir);
//         defer {
//             allocator.free(ep_config.channel_root);
//             allocator.free(ep_config.channel_dir);
//         }
//         return Endpoint.runtimeIpc(allocator, ep_config);
//     }
//     else {
//         const config: root.configs.Endpoint.Confiig = .test_default;

//         return Endpoint.runtimeIpc(allocator, .{ 
//             .channel_root = &tmp_dir.sub_path, 
//             .channel_dir = config.channel_dir, 
//             .worker_endpoint = config.worker_endpoint,
//         });
//     }
// }

const test_default: Endpoint.Config = .{
    .channel_root = if (builtin.os.tag != .windows) "/tmp/omelet-test" else "omelet-test",
    .channel_dir = Endpoint.Config.default.channel_dir,
    .worker_endpoint = Endpoint.Config.default.worker_endpoint,
};

pub fn testEndpointConfig(io: std.Io, tmp_dir: *const std.testing.TmpDir, options: struct{worker_endpoint: ?types.SymbolZ = null}) !Endpoint.Config {
    const allocator = std.testing.allocator;
    const config: Endpoint.Config = .{
        .channel_root = test_default.channel_root,
        .channel_dir = try allocator.dupeSentinel(u8, &tmp_dir.sub_path, 0),
        .worker_endpoint = options.worker_endpoint orelse test_default.worker_endpoint,
    };

    if (builtin.os.tag != .windows) {
        const ep_root = try std.Io.Dir.cwd().createDirPathOpen(io, config.channel_root, .{});
        defer ep_root.close(io);
        try ep_root.createDirPath(io, config.channel_dir);
    }

    return config;
}

pub fn releaseEndpoint(io: std.Io, endpoint: *types.Endpoints, config: *Endpoint.Config) void {
    const allocator = std.testing.allocator;
    allocator.free(endpoint.req_rep);
    allocator.free(endpoint.pub_sub);
    allocator.free(endpoint.push_pull);

    drop_sub_path: {
        if (builtin.os.tag != .windows) {
            const ep_root = 
                std.Io.Dir.cwd().createDirPathOpen(io, config.channel_root, .{})
                catch break:drop_sub_path
            ;
            defer ep_root.close(io);

            ep_root.deleteTree(io, config.channel_dir) catch {};
        }
    }
    allocator.free(config.channel_dir);
}
