const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const log = core.Logger.SystemDirect(@import("build_options").app_context);

const Setting = @This();

arena: *std.heap.ArenaAllocator,
endpoints: core.Endpoints,
log_level: core.LogLevel,
standalone: bool,

pub fn loadFromArgs(allocator: std.mem.Allocator) !Setting {
    var arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);
    const managed_allocator = arena.allocator();
    
    _ = managed_allocator;

    return .{
        .arena = arena,
        .log_level = .trace,
        .endpoints = .{ .req_rep = "", .pub_sub = "" },
        .standalone = true,
    };
}

pub fn deinit(self: *Setting) void {
    self.arena.deinit();
    self.arena.child_allocator.destroy(self.arena);
}
