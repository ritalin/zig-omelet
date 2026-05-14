const std = @import("std");
const root = @import("../../root.zig");
const nnng = @import("nnng");

const types = root.types;

pub fn Server(comptime stage_name: types.Symbol) type {
// pub fn Server(comptime stage_name: types.Symbol, comptime WorkerType: type) type {
    // const Trace = Logger.TraceDirect(stage_name);

    return struct {
        reply_socket: nnng.Rep.Protocol(nnng.Transport.Listener, nnng.Pipe.Parallel),
        pull_socket: nnng.Pull.Protocol(nnng.Transport.Listener, nnng.Pipe.Sync),
        cmd_socket: nnng.Pub.Protocol(nnng.Transport.Dialer, nnng.Pipe.Sync),

        const Self = @This();
        const EventDispatcher = root.socket.EventDispatcher(stage_name);

        pub fn init(io: std.Io, allocator: std.mem.Allocator, endpoints: types.Endpoints) Self {
            const context = nnng.Context.init(io, allocator);
            var reply_socket = nnng.Rep.open(context).parallel(4).as_listener(endpoints.req_rep);
            errdefer reply_socket.close();
            var pull_socket = nnng.Pull.open(context).as_listener(endpoints.push_pull);
            errdefer pull_socket.close();
            var cmd_socket = nnng.Pub.open(context).as_dialer(endpoints.pub_sub);
            errdefer cmd_socket.close();

            return .{
                .reply_socket = reply_socket,
                .pull_socket = pull_socket,
                .cmd_socket = cmd_socket,
            };
        }

        pub fn deinit(self: *Self) void {
            self.reply_socket.close();
            self.pull_socket.close();
            self.cmd_socket.close();
        }

        pub fn bind(self: *Self) !void {
            try self.send_socket.start(.{ .nonblocking = true });
            try self.reply_socket.start(.{});
            try self.pull_socket.start(.{});
        }

        fn onDispatch(dispatcher: *EventDispatcher(stage_name)) !?EventDispatcher(stage_name).Entry {
            _ = dispatcher;
        //     while (true) {
        //         while (dispatcher.receive_queue.dequeue()) |entry| {
        //             return entry;
        //         }

        //         send_event: {
        //             while (dispatcher.send_queue.dequeue()) |entry| {
        //                 defer entry.deinit();

        //                 Trace.debug("{s}: {} from [{s}] to [{s}] ({}) ", .{
        //                     if (entry.kind == .reply) "Reply" else "Post",
        //                     std.meta.activeTag(entry.event),
        //                     entry.from,
        //                     if (entry.routing_id) |routing_id| routing_id else "all",
        //                     dispatcher.send_queue.count()
        //                 });

        //                 if (entry.routing_id) |routing_id| {
        //                     try events.sendRoutingId(dispatcher.allocator, entry.socket, routing_id);
        //                 }

        //                 events.sendEvent(
        //                     dispatcher.allocator, entry.socket,
        //                     .{ .kind = entry.kind, .from = stage_name, .event = entry.event }
        //                 )
        //                 catch |err| switch (err) {
        //                     else => {
        //                         // Logger.Server.traceLog.debug("Unexpected error on sending: {any}", .{err});
        //                         return err;
        //                     }
        //                 };
        //             }
        //             else if (dispatcher.state.level.done) {
        //                 return null;
        //             }
        //             break:send_event;
        //         }

        //         receive_event: {
        //             while (true) {
        //                 var it = try dispatcher.polling.poll();
        //                 defer it.deinit();

        //                 while (it.next()) |item| {
        //                     const routing_id = try events.receiveRoutingId(dispatcher.allocator, item.socket);

        //                     const packet = events.receiveEventWithPayload(dispatcher.allocator, item.socket) catch |err| switch (err) {
        //                         // error.InvalidResponse => {
        //                         //     try events.sendEvent(dispatcher.allocator, item.socket, .nack);
        //                         //     continue;
        //                         // },
        //                         else => return err,
        //                     };

        //                     Trace.debug("Received command: {} from [{s}]", .{
        //                         packet.event.tag(),
        //                         std.mem.sliceTo(packet.from, 0),
        //                     });

        //                     try dispatcher.receive_queue.enqueue(.{
        //                         .allocator = dispatcher.allocator,
        //                         .kind = .response,
        //                         .socket = item.socket,
        //                         .from = packet.from,
        //                         .event = packet.event,
        //                         .routing_id = routing_id,
        //                     });
        //                 }

        //                 if (dispatcher.receive_queue.count() > 0) break:receive_event;
        //             }
        //         }
        //     }

            return null;
        }
    };
}

test "Connection.Server test" {
    std.testing.refAllDecls(@This());
}

pub const tests = struct {
    const supports = @import("../../supports/test_support.zig");

    test "start connection" {
        var tmp_dir = std.testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        const ep = supports.createEndpoint(std.testing.io, std.testing.allocator, tmp_dir);
        defer supports.releaseEndpoint(std.testing.allocator, ep);
        
        var conn = Server("runner").init(std.testing.io, std.testing.allocator, ep);
        defer conn.deinit();

        try conn.bind();
    }
};
