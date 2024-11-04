const std = @import("std");

pub const OutputStyle = enum {
    camel_case,
    pascal_case,
};

pub fn format(allocator: std.mem.Allocator, input: []const u8, style: OutputStyle) ![]const u8 {
    var iter = std.mem.splitScalar(u8, input, '_');

    var buf = std.ArrayList(u8).init(allocator);
    defer buf.deinit();
    var writer = buf.writer();
    
    first_word: {
        var phrase_iter = try PhraseIterator.init(allocator, iter.first());
        try formatWord(&writer, &phrase_iter, style);
        break:first_word;
    }
    while (iter.next()) |phrase| {
        var phrase_iter = try PhraseIterator.init(allocator, phrase);
        try formatWord(&writer, &phrase_iter, .pascal_case);
    }

    return buf.toOwnedSlice();
}

fn formatWord(writer: *std.ArrayList(u8).Writer, iter: *PhraseIterator, style: OutputStyle) !void {
    var s: OutputStyle = style;

    while (iter.next()) |x| {
        try writer.writeByte(switch (s) {
            .camel_case => std.ascii.toLower(x.word[0]),
            .pascal_case => std.ascii.toUpper(x.word[0]),
        });
        s = .pascal_case;
    
        var i: usize = 1;
        while (i < x.word.len): (i += 1) {
            try writer.writeByte(std.ascii.toLower(x.word[i]));
        }
    } 
}

const PhraseIterator = struct {
    allocator: std.mem.Allocator,
    stack: Stack,

    const Entry = struct {word: []const u8};
    const Stack = std.SinglyLinkedList(Entry);

    pub fn init(allocator: std.mem.Allocator, input: []const u8) !PhraseIterator {
        var self: PhraseIterator = .{
            .allocator = allocator,
            .stack = Stack{},
        };

        try self.parse(input);

        return self;
    }

    fn parse(self: *PhraseIterator, input: []const u8) !void {
        const State = enum {invalid, numeric, upper, lower};

        var end_index: usize = input.len;

        while (end_index > 0) {
            const i = end_index-1;
            const tk = index: {
                var state: State = .invalid;
                var offset: usize = 0;
                while (offset <= i): (offset += 1) {
                    const c = input[i-offset];
                    switch (state) {
                        .invalid => {
                            state = 
                                if (std.ascii.isDigit(c)) .numeric
                                else if (std.ascii.isUpper(c)) .upper
                                else if (std.ascii.isLower(c)) .lower
                                else .invalid
                            ;
                        },
                        .numeric => {
                            state = 
                                if (std.ascii.isUpper(c)) .upper
                                else if (std.ascii.isLower(c)) .lower
                                else state
                            ;
                        },
                        .upper => {
                            state = 
                                if (std.ascii.isUpper(c)) .upper
                                else if (std.ascii.isLower(c) or std.ascii.isDigit(c)) {
                                    break:index .{ i-offset+1 };
                                }
                                else state
                            ;
                        },
                        .lower => {
                            state = 
                                if (std.ascii.isUpper(c)) {
                                    break:index .{ i-offset };
                                }
                                else state
                            ;
                        }
                    }
                }

                break:index .{ end_index-offset };
            };

            const node = try self.allocator.create(Stack.Node);
            node.*.data = .{
                .word = input[tk[0]..end_index],
            };
            self.stack.prepend(node);

            end_index = tk[0];
        }
    }

    pub fn next(self: *PhraseIterator) ?Entry {
        const node = self.stack.popFirst() orelse return null;
        defer self.allocator.destroy(node);

        return node.data;
    }
};

fn runFormat(style: OutputStyle, input: []const u8, expect: []const u8) !void {
    const actual = try format(std.testing.allocator, input, style);
    defer std.testing.allocator.free(actual);

    try std.testing.expectEqualStrings(expect, actual);
}

test "From word#1" {
    try runFormat(.camel_case, "id", "id");
    try runFormat(.pascal_case, "id", "Id");
}

test "From word#2 (upper case)" {
    try runFormat(.camel_case, "ID", "id");
    try runFormat(.pascal_case, "ID", "Id");
}

test "From snale_case input" {
    try runFormat(.camel_case, "user_profile_id", "userProfileId");
    try runFormat(.pascal_case, "user_profile_id", "UserProfileId");
}

test "From snale_case input#2 (upper case)" {
    try runFormat(.camel_case, "USER_PROFILE_ID", "userProfileId");
    try runFormat(.pascal_case, "USER_PROFILE_ID", "UserProfileId");
}

test "From snale_case+PascalCase input#3" {
    try runFormat(.camel_case, "UserProfile_Id", "userProfileId");
    try runFormat(.pascal_case, "UserProfile_Id", "UserProfileId");
}

test "From snale_case+camelCase input#4" {
    try runFormat(.camel_case, "userProfile_Id", "userProfileId");
    try runFormat(.pascal_case, "userProfile_Id", "UserProfileId");
}

