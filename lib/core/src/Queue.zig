const std = @import("std");

pub fn Queue(comptime Entry: type) type {
    return struct {
        allocator: std.mem.Allocator,
        queue: std.DoublyLinkedList(Entry),

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{
                .allocator = allocator,
                .queue = std.DoublyLinkedList(Entry){},
            };
        }

        pub fn deinit(self: *Self) void {
            while (self.dequeue()) |*entry| {
                entry.deinit();
            }
            self.* = undefined;
        }

        pub fn enqueue(self: *Self, entry: Entry) !void {
            const node = try self.allocator.create(std.DoublyLinkedList(Entry).Node);

            node.data = entry;
            self.queue.append(node);
        }

        pub fn dequeue(self: *Self) ?Entry {
            if (self.queue.popFirst()) |node| {
                defer self.allocator.destroy(node);
                return node.data;
            }

            return null;
        }

        pub fn peek(self: *Self) ?Entry {
            return if (self.queue.first) |node| node.data else null;
        }

        pub fn prepend(self: *Self, entry: Entry) !void {
            const node = try self.allocator.create(std.DoublyLinkedList(Entry).Node);

            node.data = entry;
            self.queue.prepend(node);
        }

        pub fn hasMore(self: Self) bool {
            return self.queue.first != null;
        }

        pub fn count(self: Self) usize {
            return self.queue.len;
        }
    };
}