const std = @import("std");

const runners = @import("./catch2_runner.zig");
pub const TestOptions = runners.TestOptions;
pub const TestSpec = runners.TestSpec;

pub const run_catch2 = runners.run_catch2;