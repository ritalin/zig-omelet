const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");

const CodeBuilder = @import("./CodeBuilder.zig");

const Self = @This();

allocator: std.mem.Allocator,
source: core.Event.Payload.TopicBody,
output_root: std.fs.Dir,
output_name: core.FilePath,
on_handle: CodeBuilder.OnBuild,

pub fn init(allocator: std.mem.Allocator, source: core.Event.Payload.TopicBody, output_dir_path: core.FilePath) !*Self {
    const self = try allocator.create(Self);
    self.* = .{
        .allocator = allocator,
        .source = try source.clone(allocator),
        .output_root = try std.fs.cwd().openDir(output_dir_path, .{}),
        .output_name = try allocator.dupe(u8, trimExtension(source.header.name)),
        .on_handle = if (source.header.category == .source) CodeBuilder.SourceGenerator.build else CodeBuilder.UserTypeGenerator.build,
    };

    return self;
}

pub fn deinit(self: *Self) void {
    self.output_root.close();

    self.source.deinit();
    self.allocator.free(self.output_name);
    self.allocator.destroy(self);
}

// pub fn run(self: *Self) !void {
pub fn run(self: *Self, socket: *zmq.ZSocket) !void {
    // _ = socket;
    try sendLog(self.allocator, socket, .trace, "Begin generate from `{s}`", .{self.source.header.name});

    var builder = try CodeBuilder.init(self.allocator);
    defer builder.deinit();

    var walker = try CodeBuilder.Parser.beginParse(self.allocator, self.source.bodies);
    defer walker.deinit();

    var passed_set: TargetFields = .{};

    try parsePayload(&walker, &passed_set, builder);

    const payload = payload: {
        var writer = try core.CborStream.Writer.init(self.allocator);
        defer writer.deinit();
        
        if (self.on_handle(builder, self.output_root, self.output_name)) |status| {
            try self.writeLogPayload(&writer, self.output_name, "Successful", status);
        }
        else |err| switch (err) {
            error.QueryFileGenerationFailed => {
                try self.writeLogPayload(&writer, self.output_name, "Failed SQL file", .generate_failed);
            },
            error.TypeFileGenerationFailed => {
                try self.writeLogPayload(&writer, self.output_name, "Failed Typescript file", .generate_failed);
            },
            else => {
                try self.writeLogPayload(&writer, self.output_name, "Unexpected error during generating stage", .generate_failed);
            },
        }

        break :payload try writer.buffer.toOwnedSlice();
    };
    defer self.allocator.free(payload);

    const event: core.Event = .{
        .worker_response = try core.Event.Payload.WorkerResponse.init(self.allocator, .{payload}),
    };
    defer event.deinit();

    try core.sendEvent(self.allocator, socket, .{ .kind = .post, .from = "task", .event = event });
}

fn writeLogPayload(self: *Self, writer: *core.CborStream.Writer, name: core.Symbol, message: core.Symbol, status: ResultStatus) !void {
    const output_path = path: {
        if (self.source.header.category == .schema) {
            break:path try std.fmt.allocPrint(self.allocator, CodeBuilder.UserTypeGenerator.log_fmt, .{name});
        }
        else {
            break:path try std.fmt.allocPrint(self.allocator, CodeBuilder.SourceGenerator.log_fmt, .{name});
        }
    };
    defer self.allocator.free(output_path);

    _ = try writer.writeString(self.source.header.path);
    _ = try writer.writeString(output_path);
    _ = try writer.writeString(message);
    _ = try writer.writeEnum(ResultStatus, status);
}


const TargetFields = std.enums.EnumFieldStruct(std.meta.FieldEnum(CodeBuilder.Target), bool, false);

fn parsePayload(walker: *CodeBuilder.Parser.ResultWalker, passed_set: *TargetFields, builder: *CodeBuilder) !void {
    while (try walker.walk()) |target| switch (target) {
        .query => |q| {
            try builder.applyQuery(q);
            passed_set.query = true;
        },
        .parameter => |placeholder| {
            if (!passed_set.bound_user_type) {
                try parsePayload(walker, passed_set, builder);
            }
            if (!passed_set.anon_user_type) {
                try parsePayload(walker, passed_set, builder);
            }

            try builder.applyPlaceholder(placeholder, builder.user_type_names, builder.anon_user_types);
            passed_set.parameter = true;
        },
        .parameter_order => |orders| {
            try builder.applyPlaceholderOrder(orders);
            passed_set.parameter_order = true;
        },
        .result_set => |field_types| {
            if (!passed_set.bound_user_type) {
                try parsePayload(walker, passed_set, builder);
            }
            if (!passed_set.anon_user_type) {
                try parsePayload(walker, passed_set, builder);
            }

            try builder.applyResultSets(field_types, builder.user_type_names, builder.anon_user_types);
            passed_set.result_set = true;
        },
        .user_type => |definition| {
            if (!passed_set.bound_user_type) {
                try parsePayload(walker, passed_set, builder);
            }
            if (!passed_set.anon_user_type) {
                try parsePayload(walker, passed_set, builder);
            }

            try builder.applyUserType(definition);
            passed_set.user_type = true;
        },
        .bound_user_type => |names| {
            try builder.applyBoundUserType(names);
            passed_set.bound_user_type = true;
        },
        .anon_user_type => |definitions| {
            try builder.applyAnonymousUserType(definitions);
            passed_set.anon_user_type = true;
        },
    };
}

fn sendLog(allocator: std.mem.Allocator, socket: *zmq.ZSocket, log_level: core.LogLevel, comptime fmt: []const u8, args: anytype) !void {
    const message = try std.fmt.allocPrint(allocator, fmt, args);
    defer allocator.free(message);

    const log: core.Event = .{
        .log = try core.Event.Payload.Log.init(allocator, .{log_level, message}),
    };
    defer log.deinit();
    try core.sendEvent(allocator, socket, .{ .kind = .post, .from = "task", .event = log });
}

fn trimExtension(name: core.Symbol) core.Symbol {
    const ext = std.fs.path.extension(name);
    if (std.mem.lastIndexOf(u8, name, ext)) |i| {
        return name[0..i];
    }
    else {
        return name;
    }
}

pub const ResultStatus = CodeBuilder.ResultStatus;
