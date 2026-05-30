const std = @import("std");
const known_folders = @import("known_folders");
const root = @import("../root.zig");

const types = root.types;

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

pub fn createEndpoint(dir: std.Io.Dir) !types.Endpoints {
    const io = std.testing.io;
    const allocator = std.testing.allocator;
    return .{
        .req_rep = try createEndpointInternal(io, allocator, dir, "ipc", "req_rep"),
        .pub_sub = try createEndpointInternal(io, allocator, dir, "ipc", "pub_sub"),
        .push_pull = try createEndpointInternal(io, allocator, dir, "ipc", "push"),
    };

}

fn createEndpointInternal(io: std.Io, allocator: std.mem.Allocator, dir: std.Io.Dir, schema: types.Symbol, name: types.Symbol) !types.Symbol {
    const dir_path = try dir.realPathFileAlloc(io, ".", allocator);
    defer allocator.free(dir_path);
    return std.fmt.allocPrint(allocator, "{s}://{s}/{s}", .{ schema, dir_path, name });
}

pub fn releaseEndpoint(endpoint: types.Endpoints) void {
    const allocator = std.testing.allocator;
    allocator.free(endpoint.req_rep);
    allocator.free(endpoint.pub_sub);
    allocator.free(endpoint.push_pull);
}
