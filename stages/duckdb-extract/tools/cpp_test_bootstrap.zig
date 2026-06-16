const std = @import("std");
const runner = @import("test_runner");

pub fn main(init: std.process.Init) !void {
    var options: runner.TestOptions = .{};
    defer options.deinit(init.gpa);

    var iter = try init.minimal.args.iterateAllocator(init.gpa);
    defer iter.deinit();
    _ = iter.next();

    while (true) {
        const k: []const u8 = iter.next() orelse break;
        const v: []const u8 = iter.next() orelse return error.MissingValue;

        if (ARG_ROUTER.get(k)) |handler| {
            try handler(&options, init.gpa, v);
        }
    }

    try std.testing.expectEqual(0, try runner.run_catch2(init.io, init.gpa, options));
}

const ARG_ROUTER: std.StaticStringMap(*const fn (options: *runner.TestOptions, allocator: std.mem.Allocator, value: []const u8) anyerror!void) = .initComptime(.{
    .{ "--include-name", handleIncludeName },
    .{ "--exclude-name", handleExcludeName },
    .{ "--include-tag", handleIncludeTag },
    .{ "--exclude-tag", handleExcludeTag },
});

fn handleIncludeName(options: *runner.TestOptions, allocator: std.mem.Allocator, value: []const u8) !void {
    try options.include_specs.append(allocator, .{.name = value });
}

fn handleExcludeName(options: *runner.TestOptions, allocator: std.mem.Allocator, value: []const u8) !void {
    try options.exclude_specs.append(allocator, .{.name = value });
}

fn handleIncludeTag(options: *runner.TestOptions, allocator: std.mem.Allocator, value: []const u8) !void {
    try options.include_specs.append(allocator, .{.tag = value });
}

fn handleExcludeTag(options: *runner.TestOptions, allocator: std.mem.Allocator, value: []const u8) !void {
    try options.exclude_specs.append(allocator, .{.tag = value });
}
