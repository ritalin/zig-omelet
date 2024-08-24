const std = @import("std");

pub fn PathMatcher(comptime TChar: type) type {
    std.debug.assert(comptime (TChar == u8) or (TChar == u21));

    return struct {
        pub const FilterKind = enum {include, exclude};
        pub const FilterKinds = std.enums.EnumFieldStruct(FilterKind, bool, false);
        pub const FilterKindSet = std.enums.EnumSet(FilterKind);

        include_tree: PatriciaAhoCorasick,
        exclude_tree: PatriciaAhoCorasick,

        const FilePath = []const TChar;
        const Self = @This();

        pub fn deinit(self: *Self) void {
            self.include_tree.deinit();
            self.exclude_tree.deinit();
        }

        pub fn matchByInclude(self: Self, path: FilePath) FilterKinds {
            return self.include_tree.match(path) orelse .{.include = true};
        }

        pub fn matchByExclude(self: Self, path: FilePath) FilterKinds {
            return self.exclude_tree.match(path) orelse .{};
        }

        pub const Builder = struct {
            allocator: std.mem.Allocator,
            filter_dirs: std.ArrayList(FilterEntry),

            const FilterEntry = struct {kind: FilterKind, path: FilePath};

            pub fn init(allocator: std.mem.Allocator) Builder {
                return .{
                    .allocator = allocator,
                    .filter_dirs = std.ArrayList(FilterEntry).init(allocator),
                };
            }

            pub fn deinit(self: *Builder) void {
                for (self.filter_dirs.items) |filter| {
                    self.allocator.free(filter.path);
                }
                self.filter_dirs.deinit();
            }

            pub fn addFilterDir(self: *Builder, kind: FilterKind, path: FilePath) !void {
                return self.filter_dirs.append(.{.kind = kind, .path = try self.allocator.dupe(TChar, path)});
            }

            pub fn build(self: *Builder) !Self {
                var include_tree = try PatriciaAhoCorasick.init(self.allocator);
                var exclude_tree = try PatriciaAhoCorasick.init(self.allocator);

                for (self.filter_dirs.items) |filter| {
                    switch (filter.kind) {
                        .include => try include_tree.addPattern(filter.kind, filter.path),
                        .exclude => try exclude_tree.addPattern(filter.kind, filter.path),
                    }
                }

                try include_tree.build();
                try exclude_tree.build();

                return .{
                    .include_tree = include_tree,
                    .exclude_tree = exclude_tree
                };
            }
        };

        const PatriciaNode = struct {
            id: usize,
            children: std.AutoHashMap(TChar, *PatriciaNode),
            fail: ?*PatriciaNode = null,
            kinds: FilterKindSet,
            prefix: []TChar,
            is_pattern_end: bool = true,

        //     std::unordered_map<char, std::unique_ptr<PatriciaNode>> children;
        //     PatriciaNode* fail;
        //     bool is_output;
        //     std::string prefix;
            pub fn init(allocator: std.mem.Allocator, id: usize) !*PatriciaNode {
                const self = try allocator.create(PatriciaNode);
                self.* = .{
                    .id = id,
                    .children = std.AutoHashMap(TChar, *PatriciaNode).init(allocator),
                    .kinds = FilterKindSet.initEmpty(),
                    .prefix = &[0]TChar{},
                };

                return self;
            }
        };

        const PatriciaAhoCorasick = struct {
            arena: *std.heap.ArenaAllocator,
            root: *PatriciaNode,
            next_id: usize = 1,

        //     std::unique_ptr<PatriciaNode> root;

        //     PatriciaAhoCorasick() : root(std::make_unique<PatriciaNode>()) {}
            pub fn init(allocator: std.mem.Allocator) !PatriciaAhoCorasick {
                const arena = try allocator.create(std.heap.ArenaAllocator);
                arena.* = std.heap.ArenaAllocator.init(allocator);

                return .{
                    .arena = arena,
                    .root = try PatriciaNode.init(arena.allocator(), 0),
                };
            }

            pub fn deinit(self: *PatriciaAhoCorasick) void {
                self.arena.deinit();
                self.arena.child_allocator.destroy(self.arena);
            }

            fn genId(self: *PatriciaAhoCorasick) usize {
                defer self.next_id += 1;
                return self.next_id;
            }

            pub fn addPattern(self: *PatriciaAhoCorasick, kind: FilterKind, pattern: FilePath) !void {
                const allocator = self.arena.allocator();

                var node = self.root;
                var i: usize = 0;

                while (i < pattern.len) {
                    const prefix = pattern[i];
                    const child_entry = try node.children.getOrPut(prefix);

                    if (! child_entry.found_existing) {
                        const new_node = try PatriciaNode.init(allocator, self.genId());
                        new_node.prefix = try allocator.dupe(TChar, pattern[i..]);
                        child_entry.value_ptr.* = new_node;
                        node = new_node;
                        node.kinds.insert(kind);
                        break;
                    }
                    else {
                        const child = child_entry.value_ptr.*;

                        if (std.mem.indexOfDiff(TChar, child.prefix, pattern[i..])) |j| {
                            i += j;
                            if (j == child.prefix.len) {
                                node = child;
                                continue;
                            }
                            else {
                                const split_node = try PatriciaNode.init(allocator, self.genId());
                                split_node.prefix = child.prefix[0..j];
                                split_node.is_pattern_end = false;

                                child.prefix = child.prefix[j..];
                                try split_node.children.put(child.prefix[0], child);
                                try node.children.put(split_node.prefix[0], split_node);

                                node = split_node;
                                if (i < pattern.len) {
                                    const new_node = try PatriciaNode.init(allocator, self.genId());
                                    new_node.prefix = try allocator.dupe(TChar, pattern[i..]);
                                    new_node.kinds.insert(kind);
                                    try node.children.put(new_node.prefix[0], new_node);
                                }
                                break;
                            }
                        }
                        else {
                            child.kinds.insert(kind);
                            child.is_pattern_end = true;
                        }
                    }
                }
            }

            pub fn build(self: *PatriciaAhoCorasick) !void {
                const allocator = self.arena.allocator();

                const Q = std.TailQueue(*PatriciaNode);
                var q = Q{};

                self.root.fail = null;

                enqueue: {
                    const q_node = try allocator.create(Q.Node);
                    q_node.*.data = self.root;
                    q.append(q_node);
                    break:enqueue;
                }

                while (q.popFirst()) |nd| {
                    defer allocator.destroy(nd);

                    var current = nd.data;

                    var curr_iter = current.children.iterator();
                    while (curr_iter.next()) |item| {
                        const ch = item.key_ptr.*;
                        const child = item.value_ptr.*;
                        var fail = current.fail;

                        // while (fail != root.get()) {
                        while (fail) |fail_node| {
                            if (fail_node.children.get(ch)) |child_fail| {
                                if (std.mem.startsWith(TChar, child.prefix, child_fail.prefix)) {
                                    child.fail = child_fail;
                                    break;
                                }
                            }
                            fail = fail_node.fail;
                        }
                        else {
                            child.fail = self.root;
                        }

                        child.kinds.setUnion(child.fail.?.kinds);
                        
                        enqueue: {
                            const q_node = try allocator.create(Q.Node);
                            q_node.*.data = child;
                            q.append(q_node);
                            break:enqueue;
                        }
                    }
                }
            }

            pub inline fn hasFilter(self: PatriciaAhoCorasick) bool {
                return self.root.children.count() >= 0;
            } 

            pub fn match(self: PatriciaAhoCorasick, text: FilePath) ?FilterKinds {
                if (self.root.children.count() == 0) return null;

                var current = self.root;
                var i: usize = 0;

                while (i < text.len) {
                    if (current.children.get(text[i])) |child| {
                        if (std.mem.startsWith(TChar, text[i..], child.prefix)) {
                            i += child.prefix.len;
                            if (child.is_pattern_end) {
                                return .{
                                    .include = child.kinds.contains(.include),
                                    .exclude = child.kinds.contains(.exclude),
                                };
                            }
                            current = child;
                            continue;
                        }
                    }

                    if (current == self.root) {
                        i += 1;
                    } 
                    else {
                        current = current.fail orelse self.root;
                    }

                }

                return .{};
            }

            fn dumpInternal(allocator: std.mem.Allocator, node: *PatriciaNode, depth: usize) !void {
                var indent = std.ArrayList(u8).init(allocator);
                defer indent.deinit();
                try indent.appendNTimes(' ', depth * 4);

                std.debug.print("{s}id: {}\n", .{indent.items, node.id});
                std.debug.print("{s}prefix: '{s}', pat_end: {}, kinds: {}\n", .{indent.items, node.prefix, node.is_pattern_end, node.kinds.bits.mask});

                std.debug.print("{s}fail: ", .{indent.items});

                if (node.fail) |fail| {
                    std.debug.print("id = {}\n", .{fail.id});
                }
                else {
                    std.debug.print("null\n", .{});
                }

                std.debug.print("{s}children: [\n", .{indent.items});

                var iter = node.children.valueIterator();

                while (iter.next()) |child_ptr| {
                    try dumpInternal(allocator, child_ptr.*, depth+1);
                }

                std.debug.print("{s}]\n", .{indent.items});
            }

            pub fn dump(self: *PatriciaAhoCorasick) !void {
                try dumpInternal(self.arena.allocator(), self.root, 0);
            }
        };
    };
}

pub fn toUnicodeString(allocator: std.mem.Allocator, text: []const u8) ![]const u21 {
    var codepoints = std.ArrayList(u21).init(allocator);
    defer codepoints.deinit();

    var view = try std.unicode.Utf8View.init(text);
    var iter = view.iterator();

    while (iter.nextCodepoint()) |cp| {
        try codepoints.append(cp);
    }

    return codepoints.toOwnedSlice();
}

test "Filter#1 (ASCII only)" {
    const AsciiMatcher = PathMatcher(u8);

    const filters: []const AsciiMatcher.Builder.FilterEntry = &.{
        .{.kind = .include, .path = "documents"}, 
        .{.kind = .exclude, .path = "downloads"}, 
        .{.kind = .include, .path = "sounds"}, 
        .{.kind = .include, .path = ".txt"}, 
        .{.kind = .include, .path = "font-style"},
        .{.kind = .include, .path = "font.ttc"}, 
        .{.kind = .include, .path = ".tt"}, 
    };
    const file_paths: []const AsciiMatcher.FilePath = &.{
        "/home/user/documents/file1.txt",
        "/home/user/downloads/file2.pdf",
        "/home/user/fonts/file3.ta",
        "/home/user/fonts/font.tt"
    };

    const include_expects = &[_]?AsciiMatcher.FilterKinds {
        .{.include = true, .exclude = false},
        .{.include = false, .exclude = false},
        .{.include = false, .exclude = false},
        .{.include = true, .exclude = false},
    };
    const exclude_expects = &[_]?AsciiMatcher.FilterKinds {
        .{.include = false, .exclude = false},
        .{.include = false, .exclude = true},
        .{.include = false, .exclude = false},
        .{.include = false, .exclude = false},
    };


    const allocator = std.testing.allocator;
    var builder = AsciiMatcher.Builder.init(allocator);
    defer builder.deinit();

    for (filters) |filter| {
        try builder.addFilterDir(filter.kind, filter.path);
    }
    var matcher = try builder.build();
    defer matcher.deinit();

    // try matcher.tree.dump();

    for (file_paths, 0..) |path, i| {
        filter: {
            const result = matcher.matchByInclude(path);
            try std.testing.expectEqualDeep(include_expects[i], result);
            break:filter;
        }
        filter: {
            const result = matcher.matchByExclude(path);
            try std.testing.expectEqualDeep(exclude_expects[i], result);
            break:filter;
        }
    }
}

test "Filter#2 (日本語)" {
    const allocator = std.testing.allocator;
    const AsciiMatcher = PathMatcher(u21);

    const filters: []const AsciiMatcher.Builder.FilterEntry = &.{
        .{.kind = .include, .path = try toUnicodeString(allocator, "日本語")}, 
        .{.kind = .exclude, .path = try toUnicodeString(allocator, "英語")}, 
    };
    defer {
        for (filters) |p| { allocator.free(p.path); }
    }
    const file_paths: []const AsciiMatcher.FilePath = &.{
        try toUnicodeString(allocator, "/home/user/downloads/日本語/file2.pdf"),
        try toUnicodeString(allocator, "/home/user/downloads/中国語/file2.pdf"),
        try toUnicodeString(allocator, "/home/user/downloads/ポルトガル語/file2.pdf"),
        try toUnicodeString(allocator, "/home/user/downloads/英語/file2.pdf"),
    };
    defer {
        for (file_paths) |path| { allocator.free(path); }
    }

    const include_expects = &[_]?AsciiMatcher.FilterKinds {
        .{.include = true, .exclude = false},
        .{.include = false, .exclude = false},
        .{.include = false, .exclude = false},
        .{.include = false, .exclude = false},
    };
    const exclude_expects = &[_]?AsciiMatcher.FilterKinds {
        .{.include = false, .exclude = false},
        .{.include = false, .exclude = false},
        .{.include = false, .exclude = false},
        .{.include = false, .exclude = true},
    };

    var builder = AsciiMatcher.Builder.init(allocator);
    defer builder.deinit();

    for (filters) |filter| {
        try builder.addFilterDir(filter.kind, filter.path);
    }
    var matcher = try builder.build();
    defer matcher.deinit();

    // try matcher.tree.dump();

    for (file_paths, 0..) |path, i| {
        filter: {
            const result = matcher.matchByInclude(path);
            try std.testing.expectEqualDeep(include_expects[i], result);
            break:filter;
        }
        filter: {
            const result = matcher.matchByExclude(path);
            try std.testing.expectEqualDeep(exclude_expects[i], result);
            break:filter;
        }
    }
}

test "Filter#3 (no filter)" {
    const AsciiMatcher = PathMatcher(u8);

    const filters: []const AsciiMatcher.Builder.FilterEntry = &.{};
    const file_paths: []const AsciiMatcher.FilePath = &.{
        "/home/user/documents/file1.txt",
        "/home/user/downloads/file2.pdf",
        "/home/user/fonts/file3.ta",
        "/home/user/fonts/font.tt"
    };
    const include_expects = &[_]?AsciiMatcher.FilterKinds {
        .{.include = true, .exclude = false},
        .{.include = true, .exclude = false},
        .{.include = true, .exclude = false},
        .{.include = true, .exclude = false},
    };
    const exclude_expects = &[_]?AsciiMatcher.FilterKinds {
        .{.include = false, .exclude = false},
        .{.include = false, .exclude = false},
        .{.include = false, .exclude = false},
        .{.include = false, .exclude = false},
    };

    const allocator = std.testing.allocator;
    var builder = AsciiMatcher.Builder.init(allocator);
    defer builder.deinit();

    for (filters) |filter| {
        try builder.addFilterDir(filter.kind, filter.path);
    }
    var matcher = try builder.build();
    defer matcher.deinit();

    // try matcher.tree.dump();

    for (file_paths, 0..) |path, i| {
        filter: {
            const result = matcher.matchByInclude(path);
            try std.testing.expectEqualDeep(include_expects[i], result);
            break:filter;
        }
        filter: {
            const result = matcher.matchByExclude(path);
            try std.testing.expectEqualDeep(exclude_expects[i], result);
            break:filter;
        }
    }
}

// int main() {
//     std::vector<std::string> filters = {"documents", "downloads", ".txt"};
//     std::vector<std::string> filePaths = {
//         "/home/user/documents/file1.txt",
//         "/home/user/downloads/file2.pdf",
//         "/home/user/pictures/image.jpg"
//     };

//     FilePathFilter pathFilter(filters);

//     for (const auto& path : filePaths) {
//         if (pathFilter.contains(path)) {
//             std::cout << "File path " << path << " matches a filter." << std::endl;
//         } else {
//             std::cout << "File path " << path << " does not match any filter." << std::endl;
//         }
//     }

//     return 0;
// }