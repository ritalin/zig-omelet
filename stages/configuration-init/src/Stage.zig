const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");

const Setting = @import("./Setting.zig");
const app_context = @import("build_options").app_context;

const Connection = core.sockets.Connection.Client(app_context, void);
const Logger = core.Logger.withAppContext(app_context);

const Self = @This();

allocator: std.mem.Allocator,
context: *zmq.ZContext,
connection: *Connection,
logger: Logger,

pub fn init(allocator: std.mem.Allocator, setting: Setting) !Self {
    const context = try allocator.create(zmq.ZContext);
    context.* = try zmq.ZContext.init(allocator);
    errdefer allocator.destroy(context);
    errdefer context.deinit();

    var connection = try Connection.init(allocator, context);
    errdefer connection.deinit();
    try connection.subscribe_socket.addFilters(.{
        .request_topic = true,
        .ready_watch_path = true,
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

pub fn run(self: *Self,  setting: Setting) !void {
    try self.logger.log(.debug, "Beginning...", .{});
    try self.logger.log(.debug, "Subscriber filters: {}", .{self.connection.subscribe_socket.listFilters()});

    dump_setting: {
        try self.logger.log(.debug, "CLI: Req/Rep Channel = {s}", .{setting.endpoints.req_rep});
        try self.logger.log(.debug, "CLI: Pub/Sub Channel = {s}", .{setting.endpoints.pub_sub});
        break :dump_setting;
    }
    launch: {
        try self.connection.dispatcher.post(.launched);
        break :launch;
    }

    try self.connection.dispatcher.state.ready();

    while (self.connection.dispatcher.isReady()) {
        self.waitNextDispatch(setting) catch {
            try self.connection.dispatcher.postFatal(@errorReturnTrace());
        };
    }
}

fn waitNextDispatch(self: *Self, setting: Setting) !void {
    const _item = self.connection.dispatcher.dispatch() catch |err| switch (err) {
        error.InvalidResponse => {
            try self.logger.log(.warn, "Unexpected data received", .{});
            return;
        },
        else => return err,
    };

    if (_item) |*item| {
        defer item.deinit();

        switch (item.event) {
            .request_topic => {
                topics: {
                    const topic = try core.Event.Payload.Topic.init(
                        self.allocator, 
                        .source,
                        &.{},
                        true,
                    );
                    
                    try self.connection.dispatcher.post(.{.topic = topic});
                    break :topics;
                }
            },
            .ready_watch_path => {
                try self.handleGenerate(setting);
                try self.connection.dispatcher.post(.finish_generate);
            },
            .quit => {
                try self.connection.dispatcher.quitAccept();
            },
            .quit_all => {
                try self.connection.dispatcher.quitAccept();
                try self.connection.pull_sink_socket.stop();
            },
            else => {
                try self.logger.log(.warn, "Discard command: {}", .{std.meta.activeTag(item.event)});
            },
        }
    }
}

fn handleGenerate(self: *Self, setting: Setting) !void {
    var out_dir = std.fs.cwd().makeOpenPath(setting.output_dir_path, .{}) 
    catch {
        try self.logger.log(.err, "Failed to access estination dir: `{s}`", .{setting.output_dir_path});
        return;
    };
    defer out_dir.close();

    var source_dir = std.fs.cwd().openDir(setting.source_dir_path, .{}) 
    catch {
        try self.logger.log(.err, "Failed to access template root dir: `{s}`", .{setting.source_dir_path});
        return;
    };
    defer source_dir.close();

    const config_file_name = try std.fmt.allocPrint(self.allocator, "{s}.zon", .{@tagName(setting.command)});
    defer self.allocator.free(config_file_name);

    var file = out_dir.openFile(config_file_name, .{})
    catch |err0| switch (err0) {
        error.FileNotFound => {
            std.fs.Dir.copyFile(source_dir, config_file_name, out_dir, config_file_name, .{})
            catch |err| switch (err) {
                error.FileNotFound => {
                    const full_path = try std.fs.path.join(self.allocator, &.{setting.source_dir_path, config_file_name});
                    defer self.allocator.free(full_path);

                    try self.logger.log(.warn, "Failed to access template file: `{s}`", .{config_file_name});
                },
                else => return err,
            };
            return;
        },
        else => return err0,
    };
    defer file.close();

    const full_path = try std.fs.path.join(self.allocator, &.{setting.output_dir_path, config_file_name});
    defer self.allocator.free(full_path);

    try self.logger.log(.warn, "Already exists: `{s}`", .{full_path});
}