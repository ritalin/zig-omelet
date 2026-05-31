const std = @import("std");
const core = @import("core");

const ReceiveEntry = core.sockets.ReceiveEntry;
const EventDispatcher = core.sockets.EventDispatcher;
const StructView = core.events.StructView;
const Event = core.events.Event;

pub fn NewConfigurationState(comptime GuestStage: type) type {
    return struct {
        const Self = @This();

        pub const create: Self = .{};
        pub fn deinit(_: *Self) void {}

        pub fn handle(self: *const Self, stage: *GuestStage, entry: ReceiveEntry, dirty: *EventDispatcher.DirtyState) !void {
            _ = self;
            switch (entry.event) {
                .probe_ready => {
                    var channel = try stage.connection.requestChannel();
                    try channel.submit(stage.connection.context.io, .ready, .{});
                },
                else => {
                    try stage.defaultHandler(entry, dirty);
                }
            }
        }
    };
}
    // TODO:
    // try self.connection.dispatcher.state.ready();
    // .request_topic => {
    //     topics: {
    //         const topic = try core.Event.Payload.Topic.init(
    //             self.allocator, 
    //             .source,
    //             &.{},
    //             true,
    //         );
            
    //         try self.connection.dispatcher.post(.{.topic = topic});
    //         break :topics;
    //     }
    // },
    // .ready_watch_path => {
    //     try self.handleGenerate(setting);
    //     try self.connection.dispatcher.post(.finish_generate);
    // },


// fn handleGenerate(self: *GuestStage, setting: Setting) !void {
//     const source_dir_path = path: {
//         if (setting.from_scope) |scope| {
//             break:path try std.fs.path.join(self.allocator, &.{setting.output_dir_path, scope, setting.category.destPath()});
//         }
//         else {
//             break:path try std.fs.path.join(self.allocator, &.{setting.source_dir_path, setting.category.templateDir()});
//         }
//     };
//     defer self.allocator.free(source_dir_path);

//     var source_dir = std.fs.cwd().openDir(source_dir_path, .{}) 
//     catch {
//         try self.logger.log(.err, "Failed to access template root dir: `{s}`", .{setting.source_dir_path});
//         return;
//     };
//     defer source_dir.close();

//     const config_file_name = try std.fmt.allocPrint(self.allocator, "{s}.zon", .{@tagName(setting.command)});
//     defer self.allocator.free(config_file_name);

//     var file = source_dir.openFile(config_file_name, .{})
//     catch {
//         const full_path = try std.fs.path.join(self.allocator, &.{source_dir_path, config_file_name});
//         defer self.allocator.free(full_path);

//         try self.logger.log(.warn, "Failed to access template file: `{s}`", .{config_file_name});
//         return;
//     };
//     defer file.close();

//     for (setting.scope_set) |scope| {
//         try self.handleGenerateInternal(setting, source_dir, scope, config_file_name);
//     }
// }

// fn handleGenerateInternal(self: *GuestStage, setting: Setting, source_dir: std.fs.Dir, scope: core.Symbol, config_file_name: core.Symbol) !void {
//     const out_dir_path = try std.fs.path.join(self.allocator, &.{
//         setting.output_dir_path, scope, setting.category.destPath()
//     });
//     defer self.allocator.free(out_dir_path);

//     var out_dir = std.fs.cwd().makeOpenPath(out_dir_path, .{}) 
//     catch {
//         try self.logger.log(.err, "Failed to access destination dir: `{s}`", .{setting.output_dir_path});
//         return;
//     };
//     defer out_dir.close();

//     var file = out_dir.openFile(config_file_name, .{})
//     catch |err0| switch (err0) {
//         error.FileNotFound => {
//             return try std.fs.Dir.copyFile(source_dir, config_file_name, out_dir, config_file_name, .{});
//         },
//         else => return err0,
//     };
//     defer file.close();

//     const full_path = try std.fs.path.join(self.allocator, &.{out_dir_path, config_file_name});
//     defer self.allocator.free(full_path);

//     try self.logger.log(.warn, "Already exists: `{s}`", .{full_path});
// }