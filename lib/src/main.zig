const std = @import("std");
const CborStream = @import("./CborStream.zig");
const Writer = CborStream.Writer;
const Reader = CborStream.Reader;
const types = @import("./types.zig");
const event_cbor = @import("./decode_event_cbor.zig");
const encodeEventInternal = event_cbor.__encodeEventInternal;
const decodeEventInternal = event_cbor.__decodeEventInternal;

pub fn main() !void {
    const test_allocator = std.heap.page_allocator;
    const arena = try test_allocator.create(std.heap.ArenaAllocator);
    defer arena.child_allocator.destroy(arena);
    arena.* = std.heap.ArenaAllocator.init(test_allocator);
    // var arena = std.heap.ArenaAllocator.init(test_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    const TestTuple = std.meta.Tuple(&.{[]const u8, u16});
    const expected: TestTuple = .{ "item_1", 888 };

    var writer = try Writer.init(allocator);
    defer writer.deinit();

    _ = try writer.writeTuple(TestTuple, expected);

    var reader = Reader.init(writer.buffer.items);

    const v = try reader.readTuple(TestTuple);
    _ = v;
}