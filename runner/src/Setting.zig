const std = @import("std");

const core = @import("core");

const Setting = @This();

arena: *std.heap.ArenaAllocator,
runner_endpoints: core.Endpoints,
stage_endpoints: core.Endpoints,
source_dir: []const core.FilePath,
watch: bool,