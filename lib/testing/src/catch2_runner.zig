const std = @import("std");

extern fn run_catch2_test(test_report_path: [*:0]const u8, argc: c_int, argv: [*]const [*:0]const u8) callconv(.c) c_int;

const TEST_OUTPUT: [:0]const u8 = "test_result.txt";

pub fn run_catch2(io: std.Io, allocator: std.mem.Allocator, options: TestOptions) !c_int {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const managed_allocator = arena.allocator();
    var args: std.ArrayListUnmanaged([*:0]const u8) = .empty;
    try options.buildArgs(managed_allocator, "your_program_name", &args);

    const err = run_catch2_test(TEST_OUTPUT, @intCast(args.items.len), args.items.ptr);
    if (err > 0) {
        var write_buffer: [4096]u8 = undefined;
        const terminal = std.debug.lockStderr(&write_buffer).terminal();
        defer std.debug.unlockStderr();

        const file = try std.Io.Dir.cwd().openFile(io, TEST_OUTPUT, .{});
        defer file.close(io);

        var read_buffer: [4096]u8 = undefined;
        var reader = file.reader(io, &read_buffer);

        _ = try reader.interface.streamRemaining(terminal.writer);
        try terminal.writer.flush();
    }

    return err;
}

pub const TestOptions = struct {
    include_specs: std.ArrayListUnmanaged(TestSpec) = .empty,
    exclude_specs: std.ArrayListUnmanaged(TestSpec) = .empty,

    pub fn deinit(self: *TestOptions, allocator: std.mem.Allocator) void {
        self.include_specs.deinit(allocator);
        self.exclude_specs.deinit(allocator);
    }

    fn buildArgs(self: TestOptions, allocator: std.mem.Allocator, runner_name: [:0]const u8, args: *std.ArrayListUnmanaged([*:0]const u8)) !void {
        try args.append(allocator, runner_name.ptr);

        for (self.include_specs.items) |spec| {
            switch (spec) {
                .name => |value| {
                    try TestOptions.buildArgsInternal(allocator, "{s}", value, args);
                },
                .tag => |value| {
                    try TestOptions.buildArgsInternal(allocator, "[{s}]", value, args);
                },
            }
        }
        for (self.exclude_specs.items) |spec| {
            switch (spec) {
                .name => |value| {
                    try TestOptions.buildArgsInternal(allocator, "~{s}", value, args);
                },
                .tag => |value| {
                    try TestOptions.buildArgsInternal(allocator, "~[{s}]", value, args);
                },
            }
        }
    }

    fn buildArgsInternal(allocator: std.mem.Allocator, comptime fmt: []const u8, value: []const u8, args: *std.ArrayListUnmanaged([*:0]const u8)) !void {
        const arg = try std.fmt.allocPrintSentinel(allocator, fmt, .{value}, 0);
        try args.append(allocator, arg.ptr);
    }
};

pub const TestSpec = union(enum) {
    name: []const u8,
    tag: []const u8,
};