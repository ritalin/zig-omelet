const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");

const Setting = @import("./Setting.zig");
const CodeBuilder = @import("./CodeBuilder.zig");
const app_context = @import("build_options").app_context;

const Connection = core.sockets.Connection.Client(app_context, GenerateWorker);
const Logger = core.Logger.withAppContext(app_context);

const GenerateWorker = @import("./GenerateWorker.zig");

const Self = @This();

allocator: std.mem.Allocator,
context: *zmq.ZContext,
connection: *Connection,
logger: Logger,


pub fn init(allocator: std.mem.Allocator, setting: Setting) !Self {
    const context = try allocator.create(zmq.ZContext);
    context.* = try zmq.ZContext.init(allocator);

    var connection = try Connection.init(allocator, context);
    try connection.subscribe_socket.addFilters(.{
        .ready_topic_body = true,
        .topic_body = true,
        .finish_topic_body = true,
        .quit_all = true,
        .quit = true,
    });
    try connection.connect(setting.endpoints);

    return .{
        .allocator = allocator,
        .context = context,
        .connection = connection,
        .logger = Logger.init(allocator, connection.dispatcher, setting.standalone),
    };
}

pub fn deinit(self: *Self) void {
    self.connection.deinit();
    self.context.deinit();
    self.allocator.destroy(self.context);
}

pub fn run(self: *Self, setting: Setting) !void {
    try self.logger.log(.debug, "Beginning...", .{});
    try self.logger.log(.debug, "Subscriber filters: {}", .{self.connection.subscribe_socket.listFilters()});

    dump_setting: {
        try self.logger.log(.debug, "CLI: Req/Rep Channel = {s}", .{setting.endpoints.req_rep});
        try self.logger.log(.debug, "CLI: Pub/Sub Channel = {s}", .{setting.endpoints.pub_sub});
        break :dump_setting;
    }
    launch: {
        try self.connection.dispatcher.post(.{
            .launched = try core.EventPayload.Stage.init(self.allocator, app_context),
        });
        break :launch;
    }

    var lookup = std.StringHashMap(core.EventPayload.SourcePath).init(self.allocator);
    defer lookup.deinit();

    try self.connection.dispatcher.state.ready();

    while (self.connection.dispatcher.isReady()) {
        const _item = self.connection.dispatcher.dispatch() catch |err| switch (err) {
            error.InvalidResponse => {
                try self.logger.log(.warn, "Unexpected data received", .{});
                continue;
            },
            else => return err,
        };

        if (_item) |*item| {
            defer item.deinit();

            switch (item.event) {
                .ready_topic_body => {
                    try self.logger.log(.err, "Ready for generating", .{});
                    try self.connection.dispatcher.post(.ready_generate);
                },
                .topic_body => |source| {
                    try self.connection.dispatcher.approve();
                    try self.logger.log(.trace, "Accept source: `{s}`", .{source.header.path});

                    const path = try source.header.clone(self.allocator);
                    try lookup.put(path.path, path);

                    const worker = try GenerateWorker.init(self.allocator, source, setting.output_dir_path);
                    try self.connection.pull_sink_socket.spawn(worker);
                    try self.connection.dispatcher.post(.ready_generate);
                },
                .worker_result => |result| {
                    try self.processWorkResult(result.content, &lookup);

                    if (self.connection.dispatcher.state.level.terminating) {
                        if (lookup.count() == 0) {
                            try self.connection.dispatcher.post(.ready_generate);
                        }
                    }
                },
                .finish_topic_body => {
                    try self.connection.dispatcher.state.receiveTerminate();

                    if (lookup.count() == 0) {
                        try self.connection.dispatcher.post(.ready_generate);
                    }
                },
                .quit, .quit_all => {
                    if (lookup.count() == 0) {
                        try self.connection.dispatcher.post(.{
                            .quit_accept = try core.EventPayload.Stage.init(self.allocator, app_context),
                        });
                    }
                },
                .log => |log| {
                    try self.logger.log(log.level, "{s}", .{log.content});
                },
                else => {
                    try self.logger.log(.warn, "Discard command: {}", .{std.meta.activeTag(item.event)});
                },
            }
        }
    }
}

fn processWorkResult(self: *Self, result_content: core.Symbol, lookup: *std.StringHashMap(core.EventPayload.SourcePath)) !void {
    var reader = core.CborStream.Reader.init(result_content);

    const source_path = try reader.readString();
    const dest_name = try reader.readString();
    const message = try reader.readString();
    const status = try reader.readEnum(GenerateWorker.ResultStatus);
    
    const kv_ = lookup.fetchRemove(source_path);
    defer {
        if (kv_) |*kv| kv.value.deinit();
    }

    if (status == .generate_failed) {
        try self.logger.log(.err, "{s} of `{s}`", .{
            message, source_path,
        });
    }
    else {
        try self.logger.log(.info, "{s} of `{s}/*` {s}", .{
            message,
            dest_name,
            if (status == .new_file) "✨" else "",
        });
    }
    try self.logger.log(.trace, "End generate from `{s}`", .{if (kv_) |kv| kv.value.name else "????"});
}