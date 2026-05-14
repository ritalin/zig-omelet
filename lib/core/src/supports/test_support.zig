const std = @import("std");
const root = @import("../root.zig");

const types = root.types;

pub fn createEndpoint(io: std.Io, allocator: std.mem.Allocator, tmp_dir: std.testing.TmpDir) types.Endpoints {
    return .{
        .req_rep = try createEndpointInternal(io, allocator, tmp_dir.dir, "ipc", "req_rep"),
        .pub_sub = try createEndpointInternal(io, allocator, tmp_dir.dir, "ipc", "pub_sub"),
        .push_pull = try createEndpointInternal(io, allocator, tmp_dir.dir, "ipc", "push_pull"),
    };

}

fn createEndpointInternal(io: std.Io, allocator: std.mem.Allocator, dir: std.Io.Dir, schema: types.Symbol, name: types.Symbol) !types.Symbol {
    const path = try dir.realPathFileAlloc(io, name, allocator);
    defer allocator.free(path);
    return std.fmt.allocPrint(allocator, "{s}://{s}", .{ schema, path });
}

pub fn releaseEndpoint(allocator: std.mem.Allocator, endpoint: types.Endpoints) void {
    allocator.free(endpoint.req_rep);
    allocator.free(endpoint.pub_sub);
    allocator.free(endpoint.push_pull);
}
