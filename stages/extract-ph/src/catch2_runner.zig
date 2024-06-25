const std = @import("std");

extern fn run_catch2_test(test_report_path: [*c]const u8) callconv(.C) c_int;

const TEST_OUTPUT: [:0]const u8 = "test_result.txt";

pub fn run_catch2(allocator: std.mem.Allocator) !c_int {
    const err = run_catch2_test(TEST_OUTPUT);
    if (err > 0) {
        const file = try std.fs.cwd().openFile(TEST_OUTPUT, .{});
        defer file.close();

        const meta = try file.metadata();
        const data = try file.readToEndAlloc(allocator, meta.size());
        defer allocator.free(data);

        std.debug.print("Catch2 output:\n{s}\n", .{data}); 
    }

    return err;
}