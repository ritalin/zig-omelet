const std = @import("std");
const clap = @import("clap");
const root = @import("../root.zig");

pub fn ArgScanner(comptime ArgIterator: type) type {
    return struct {
        iter: *ArgIterator,
        pending: ?root.types.Symbol = null,
        state: State = .consumed,

        const Self = @This();

        pub fn init(iter: *ArgIterator) Self {
            return .{
                .iter = iter,
            };
        }

        pub fn scan(self: *Self) bool {
            if (self.state == .consumed) {
                self.pending = self.iter.next();
                self.state = .pending;
            }
            return self.pending != null;
        }

        pub fn next(self: *Self) ?root.types.Symbol {
            switch (self.state) {
                .pending => {
                    defer self.state = .consumed;
                    return self.pending;
                },
                .consumed => {
                    self.pending = self.iter.next();
                    return self.pending;
                }
            }
        }

        pub fn reset(self: *Self) void {
            self.state = .pending;
        }

        const State = enum {
            pending,
            consumed,
        };
    };
}

pub fn ArgParserPair(comptime BaseArgId: type, comptime ExtraArgId: type, comptime ArgIterator: type) type {
    return struct {
        base: clap.streaming.Clap(BaseArgId, ArgScanner(ArgIterator)),
        extra: clap.streaming.Clap(ExtraArgId, ArgScanner(ArgIterator)),

        const Self = @This();
        const Scanner = ArgScanner(ArgIterator);

        pub fn init(input: *Scanner, diag: *clap.Diagnostic) Self {
            return .{
                .base = .{
                    .params = BaseArgId.Decls,
                    .iter = input,
                    .diagnostic = diag,
                },
                .extra = .{
                    .params = ExtraArgId.Decls,
                    .iter = input,
                    .diagnostic = diag,
                },
            };
        }

        pub fn next(self: *Self, scanner: *Scanner) !?ParseResult {
            const base_arg = self.base.next()
            catch |err| switch (err) {
                error.InvalidArgument => {
                    scanner.reset();
                    const extra_arg = try self.extra.next();
                    return if (extra_arg) |arg| .{ .extra = arg } else null;
                },
                else => return err,
            };

            return if (base_arg)  |arg| .{.base = arg} else null;
        }

        pub const ParseResult = union(std.meta.FieldEnum(Self)) {
            base: clap.streaming.Arg(BaseArgId),
            extra: clap.streaming.Arg(ExtraArgId),
        };
    };
}

test "arg parser test" {
    std.testing.refAllDecls(@This());
}

pub const tests = struct {
    const GuestBaseConfig = @import("../configs/guest_args.zig").GuestBaseConfig;
    const GuestBaseArgId = GuestBaseConfig.ArgId(.{});

    const GuestWatch = @import("../configs/guest_args.zig").GuestWatch;
    const GuestWatchArgId = GuestWatch.ArgId(.{});

    test "no args" {
        const args = &.{};

        var iter = clap.args.SliceIterator{ .args = args };
        var scanner: ArgScanner(clap.args.SliceIterator) = .init(&iter);
        var diag: clap.Diagnostic = .{};
        var parsers: ArgParserPair(GuestBaseArgId, GuestWatchArgId, clap.args.SliceIterator) = .init(&scanner, &diag);

        try std.testing.expectEqual(false, scanner.scan());
        try std.testing.expectEqual(null, parsers.next(&scanner));
    }

    test "base args only " {
        const args = &.{
            "--reqrep-channel", "inproc://reqrep",
            "--pubsub-channel", "inproc://pubsub",  
            "--pushpull-channel", "inproc://pushpull",
            "--log-level", "info",        
            "--log-style", "integrated",       
            "--no-color",                   
        };

        var iter = clap.args.SliceIterator{ .args = args };
        var scanner: ArgScanner(clap.args.SliceIterator) = .init(&iter);
        var diag: clap.Diagnostic = .{};
        var parsers: ArgParserPair(GuestBaseArgId, GuestWatchArgId, clap.args.SliceIterator) = .init(&scanner, &diag);

        parse: {
            try std.testing.expectEqual(true, scanner.scan());
            const next_arg = try parsers.next(&scanner) orelse unreachable;
            try std.testing.expectEqual(.base, std.meta.activeTag(next_arg));
            try std.testing.expectEqual(.req_rep, next_arg.base.param.id);
            try std.testing.expectEqualDeep("inproc://reqrep", next_arg.base.value);
            break:parse;
        }
        parse: {
            try std.testing.expectEqual(true, scanner.scan());
            const next_arg = try parsers.next(&scanner) orelse unreachable;
            try std.testing.expectEqual(.base, std.meta.activeTag(next_arg));
            try std.testing.expectEqual(.pub_sub, next_arg.base.param.id);
            try std.testing.expectEqualDeep("inproc://pubsub", next_arg.base.value);
            break:parse;
        }
        parse: {
            try std.testing.expectEqual(true, scanner.scan());
            const next_arg = try parsers.next(&scanner) orelse unreachable;
            try std.testing.expectEqual(.base, std.meta.activeTag(next_arg));
            try std.testing.expectEqual(.push_pull, next_arg.base.param.id);
            try std.testing.expectEqualDeep("inproc://pushpull", next_arg.base.value);
            break:parse;
        }
        parse: {
            try std.testing.expectEqual(true, scanner.scan());
            const next_arg = try parsers.next(&scanner) orelse unreachable;
            try std.testing.expectEqual(.base, std.meta.activeTag(next_arg));
            try std.testing.expectEqual(.log_level, next_arg.base.param.id);
            try std.testing.expectEqualDeep("info", next_arg.base.value);
            break:parse;
        }
        parse: {
            try std.testing.expectEqual(true, scanner.scan());
            const next_arg = try parsers.next(&scanner) orelse unreachable;
            try std.testing.expectEqual(.base, std.meta.activeTag(next_arg));
            try std.testing.expectEqual(.log_style, next_arg.base.param.id);
            try std.testing.expectEqualDeep("integrated", next_arg.base.value);
            break:parse;
        }
        parse: {
            try std.testing.expectEqual(true, scanner.scan());
            const next_arg = try parsers.next(&scanner) orelse unreachable;
            try std.testing.expectEqual(.base, std.meta.activeTag(next_arg));
            try std.testing.expectEqual(.no_color, next_arg.base.param.id);
            try std.testing.expectEqualDeep(null, next_arg.base.value);
            break:parse;
        }
        parse: {
            try std.testing.expectEqual(false, scanner.scan());
            try std.testing.expectEqual(null, parsers.next(&scanner));
            break:parse;
        }
    }

    test "extra args only " {
        const args = &.{
            "--source-dir", "/path/to/sources",
            "--schema-dir", "/path/to/schemas",
            "--include-filter", "foo",
            "--exclude-filter", "bar",
            "--watch",
        };

        var iter = clap.args.SliceIterator{ .args = args };
        var scanner: ArgScanner(clap.args.SliceIterator) = .init(&iter);
        var diag: clap.Diagnostic = .{};
        var parsers: ArgParserPair(GuestBaseArgId, GuestWatchArgId, clap.args.SliceIterator) = .init(&scanner, &diag);

        parse: {
            try std.testing.expectEqual(true, scanner.scan());
            const next_arg = try parsers.next(&scanner) orelse unreachable;
            try std.testing.expectEqual(.extra, std.meta.activeTag(next_arg));
            try std.testing.expectEqual(.source_dir_set, next_arg.extra.param.id);
            try std.testing.expectEqualDeep("/path/to/sources", next_arg.extra.value);
            break:parse;
        }
        parse: {
            try std.testing.expectEqual(true, scanner.scan());
            const next_arg = try parsers.next(&scanner) orelse unreachable;
            try std.testing.expectEqual(.extra, std.meta.activeTag(next_arg));
            try std.testing.expectEqual(.schema_dir_set, next_arg.extra.param.id);
            try std.testing.expectEqualDeep("/path/to/schemas", next_arg.extra.value);
            break:parse;
        }
        parse: {
            try std.testing.expectEqual(true, scanner.scan());
            const next_arg = try parsers.next(&scanner) orelse unreachable;
            try std.testing.expectEqual(.extra, std.meta.activeTag(next_arg));
            try std.testing.expectEqual(.include_filter_set, next_arg.extra.param.id);
            try std.testing.expectEqualDeep("foo", next_arg.extra.value);
            break:parse;
        }
        parse: {
            try std.testing.expectEqual(true, scanner.scan());
            const next_arg = try parsers.next(&scanner) orelse unreachable;
            try std.testing.expectEqual(.extra, std.meta.activeTag(next_arg));
            try std.testing.expectEqual(.exclude_filter_set, next_arg.extra.param.id);
            try std.testing.expectEqualDeep("bar", next_arg.extra.value);
            break:parse;
        }
        parse: {
            try std.testing.expectEqual(true, scanner.scan());
            const next_arg = try parsers.next(&scanner) orelse unreachable;
            try std.testing.expectEqual(.extra, std.meta.activeTag(next_arg));
            try std.testing.expectEqual(.watch, next_arg.extra.param.id);
            try std.testing.expectEqualDeep(null, next_arg.extra.value);
            break:parse;
        }
        parse: {
            try std.testing.expectEqual(false, scanner.scan());
            try std.testing.expectEqual(null, parsers.next(&scanner));
            break:parse;
        }
    }

    test "sequential args" {
        const args = &.{
            "--reqrep-channel", "inproc://reqrep",
            "--no-color",                   
            "--source-dir", "/path/to/sources",
            "--watch",
        };

        var iter = clap.args.SliceIterator{ .args = args };
        var scanner: ArgScanner(clap.args.SliceIterator) = .init(&iter);
        var diag: clap.Diagnostic = .{};
        var parsers: ArgParserPair(GuestBaseArgId, GuestWatchArgId, clap.args.SliceIterator) = .init(&scanner, &diag);

         parse: {
            try std.testing.expectEqual(true, scanner.scan());
            const next_arg = try parsers.next(&scanner) orelse unreachable;
            try std.testing.expectEqual(.base, std.meta.activeTag(next_arg));
            try std.testing.expectEqual(.req_rep, next_arg.base.param.id);
            try std.testing.expectEqualDeep("inproc://reqrep", next_arg.base.value);
            break:parse;
        }
        parse: {
            try std.testing.expectEqual(true, scanner.scan());
            const next_arg = try parsers.next(&scanner) orelse unreachable;
            try std.testing.expectEqual(.base, std.meta.activeTag(next_arg));
            try std.testing.expectEqual(.no_color, next_arg.base.param.id);
            try std.testing.expectEqualDeep(null, next_arg.base.value);
            break:parse;
        }
         parse: {
            try std.testing.expectEqual(true, scanner.scan());
            const next_arg = try parsers.next(&scanner) orelse unreachable;
            try std.testing.expectEqual(.extra, std.meta.activeTag(next_arg));
            try std.testing.expectEqual(.source_dir_set, next_arg.extra.param.id);
            try std.testing.expectEqualDeep("/path/to/sources", next_arg.extra.value);
            break:parse;
        }
        parse: {
            try std.testing.expectEqual(true, scanner.scan());
            const next_arg = try parsers.next(&scanner) orelse unreachable;
            try std.testing.expectEqual(.extra, std.meta.activeTag(next_arg));
            try std.testing.expectEqual(.watch, next_arg.extra.param.id);
            try std.testing.expectEqualDeep(null, next_arg.extra.value);
            break:parse;
        }
        parse: {
            try std.testing.expectEqual(false, scanner.scan());
            try std.testing.expectEqual(null, parsers.next(&scanner));
            break:parse;
        }
   }

    test "scrambled args" {
        const args = &.{
            "--reqrep-channel", "inproc://reqrep",
            "--source-dir", "/path/to/sources",
            "--no-color",                   
            "--watch",
        };

        var iter = clap.args.SliceIterator{ .args = args };
        var scanner: ArgScanner(clap.args.SliceIterator) = .init(&iter);
        var diag: clap.Diagnostic = .{};
        var parsers: ArgParserPair(GuestBaseArgId, GuestWatchArgId, clap.args.SliceIterator) = .init(&scanner, &diag);

         parse: {
            try std.testing.expectEqual(true, scanner.scan());
            const next_arg = try parsers.next(&scanner) orelse unreachable;
            try std.testing.expectEqual(.base, std.meta.activeTag(next_arg));
            try std.testing.expectEqual(.req_rep, next_arg.base.param.id);
            try std.testing.expectEqualDeep("inproc://reqrep", next_arg.base.value);
            break:parse;
        }
         parse: {
            try std.testing.expectEqual(true, scanner.scan());
            const next_arg = try parsers.next(&scanner) orelse unreachable;
            try std.testing.expectEqual(.extra, std.meta.activeTag(next_arg));
            try std.testing.expectEqual(.source_dir_set, next_arg.extra.param.id);
            try std.testing.expectEqualDeep("/path/to/sources", next_arg.extra.value);
            break:parse;
        }
        parse: {
            try std.testing.expectEqual(true, scanner.scan());
            const next_arg = try parsers.next(&scanner) orelse unreachable;
            try std.testing.expectEqual(.base, std.meta.activeTag(next_arg));
            try std.testing.expectEqual(.no_color, next_arg.base.param.id);
            try std.testing.expectEqualDeep(null, next_arg.base.value);
            break:parse;
        }
        parse: {
            try std.testing.expectEqual(true, scanner.scan());
            const next_arg = try parsers.next(&scanner) orelse unreachable;
            try std.testing.expectEqual(.extra, std.meta.activeTag(next_arg));
            try std.testing.expectEqual(.watch, next_arg.extra.param.id);
            try std.testing.expectEqualDeep(null, next_arg.extra.value);
            break:parse;
        }
        parse: {
            try std.testing.expectEqual(false, scanner.scan());
            try std.testing.expectEqual(null, parsers.next(&scanner));
            break:parse;
        }
    }

    test "unhandled unknown arg" {
        const args = &.{
            "--reqrep-channel", "inproc://reqrep",
            "--source-dir", "/path/to/sources",
            "--output-dir", "/path/to/output",
        };

        var iter = clap.args.SliceIterator{ .args = args };
        var scanner: ArgScanner(clap.args.SliceIterator) = .init(&iter);
        var diag: clap.Diagnostic = .{};
        var parsers: ArgParserPair(GuestBaseArgId, GuestWatchArgId, clap.args.SliceIterator) = .init(&scanner, &diag);

         parse: {
            try std.testing.expectEqual(true, scanner.scan());
            const next_arg = try parsers.next(&scanner) orelse unreachable;
            try std.testing.expectEqual(.base, std.meta.activeTag(next_arg));
            try std.testing.expectEqual(.req_rep, next_arg.base.param.id);
            try std.testing.expectEqualDeep("inproc://reqrep", next_arg.base.value);
            break:parse;
        }
        parse: {
            try std.testing.expectEqual(true, scanner.scan());
            const next_arg = try parsers.next(&scanner) orelse unreachable;
            try std.testing.expectEqual(.extra, std.meta.activeTag(next_arg));
            try std.testing.expectEqual(.source_dir_set, next_arg.extra.param.id);
            try std.testing.expectEqualDeep("/path/to/sources", next_arg.extra.value);
            break:parse;
        }
        parse: {
            try std.testing.expectEqual(true, scanner.scan());
            try std.testing.expectError(error.InvalidArgument, parsers.next(&scanner));
            break:parse;
        }
    }

    test "missing value in base args" {
        const args = &.{
            "--source-dir", "/path/to/sources",
            "--reqrep-channel",
        };

        var iter = clap.args.SliceIterator{ .args = args };
        var scanner: ArgScanner(clap.args.SliceIterator) = .init(&iter);
        var diag: clap.Diagnostic = .{};
        var parsers: ArgParserPair(GuestBaseArgId, GuestWatchArgId, clap.args.SliceIterator) = .init(&scanner, &diag);

        parse: {
            try std.testing.expectEqual(true, scanner.scan());
            const next_arg = try parsers.next(&scanner) orelse unreachable;
            try std.testing.expectEqual(.extra, std.meta.activeTag(next_arg));
            try std.testing.expectEqual(.source_dir_set, next_arg.extra.param.id);
            try std.testing.expectEqualDeep("/path/to/sources", next_arg.extra.value);
            break:parse;
        }
        parse: {
            try std.testing.expectEqual(true, scanner.scan());
            try std.testing.expectError(error.MissingValue, parsers.next(&scanner));
            break:parse;
        }
    }

    test "missing value in extra args" {
        const args = &.{
            "--reqrep-channel", "inproc://reqrep",
            "--source-dir",
        };

        var iter = clap.args.SliceIterator{ .args = args };
        var scanner: ArgScanner(clap.args.SliceIterator) = .init(&iter);
        var diag: clap.Diagnostic = .{};
        var parsers: ArgParserPair(GuestBaseArgId, GuestWatchArgId, clap.args.SliceIterator) = .init(&scanner, &diag);

        parse: {
            try std.testing.expectEqual(true, scanner.scan());
            const next_arg = try parsers.next(&scanner) orelse unreachable;
            try std.testing.expectEqual(.base, std.meta.activeTag(next_arg));
            try std.testing.expectEqual(.req_rep, next_arg.base.param.id);
            try std.testing.expectEqualDeep("inproc://reqrep", next_arg.base.value);
            break:parse;
        }
        parse: {
            try std.testing.expectEqual(true, scanner.scan());
            try std.testing.expectError(error.MissingValue, parsers.next(&scanner));
            break:parse;
        }
    }

    test "value for flag in base args" {
        const args = &.{
            "--watch",
            "--no-color=true",                   
        };

        var iter = clap.args.SliceIterator{ .args = args };
        var scanner: ArgScanner(clap.args.SliceIterator) = .init(&iter);
        var diag: clap.Diagnostic = .{};
        var parsers: ArgParserPair(GuestBaseArgId, GuestWatchArgId, clap.args.SliceIterator) = .init(&scanner, &diag);

        parse: {
            try std.testing.expectEqual(true, scanner.scan());
            const next_arg = try parsers.next(&scanner) orelse unreachable;
            try std.testing.expectEqual(.extra, std.meta.activeTag(next_arg));
            try std.testing.expectEqual(.watch, next_arg.extra.param.id);
            try std.testing.expectEqualDeep(null, next_arg.extra.value);
            break:parse;
        }
        parse: {
            try std.testing.expectEqual(true, scanner.scan());
            try std.testing.expectError(error.DoesntTakeValue, parsers.next(&scanner));
            break:parse;
        }
    }

    test "value for flag in extra args" {
        const args = &.{
            "--no-color",                   
            "--watch=true",
        };

        var iter = clap.args.SliceIterator{ .args = args };
        var scanner: ArgScanner(clap.args.SliceIterator) = .init(&iter);
        var diag: clap.Diagnostic = .{};
        var parsers: ArgParserPair(GuestBaseArgId, GuestWatchArgId, clap.args.SliceIterator) = .init(&scanner, &diag);

        parse: {
            try std.testing.expectEqual(true, scanner.scan());
            const next_arg = try parsers.next(&scanner) orelse unreachable;
            try std.testing.expectEqual(.base, std.meta.activeTag(next_arg));
            try std.testing.expectEqual(.no_color, next_arg.base.param.id);
            try std.testing.expectEqualDeep(null, next_arg.base.value);
            break:parse;
        }
        parse: {
            try std.testing.expectEqual(true, scanner.scan());
            try std.testing.expectError(error.DoesntTakeValue, parsers.next(&scanner));
            break:parse;
        }
   }
};