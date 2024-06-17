const std = @import("std");
const zmq = @import("zmq");
const core = @import("core");
const Runner = @import("./Runner.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    var arena = std.heap.ArenaAllocator.init(gpa.allocator());
    defer arena.deinit();

    const allocator = arena.allocator();

    var runner = try Runner.init(allocator);
    defer runner.deinit();
    
    try mockRunner(allocator, runner);

    // var t = try std.Thread.spawn(.{}, mockStage, .{allocator});

    // t.join();
}

fn receive(allocator: std.mem.Allocator, conn: *core.sockets.Connection.Server) !core.Event {
    while (true) {
        var it = conn.dispatcher.polling.poll() catch |err| switch (err) {
            error.PollingTimeout => continue,
            else => return err,
        };
        defer it.deinit();

        while (it.next()) |item| {
            const event = try core.receiveEventWithPayload(allocator, item.socket);
            defer event.deinit();

            switch (event) {
                .end_watch_path, .finish_topic_body, .finish_generate => {
                    try core.sendEvent(allocator, item.socket, .quit);
                },
                .log => {
                    try core.sendEvent(allocator, item.socket, .ack);
                },
                else => {
                    try core.sendEvent(allocator, item.socket, .ack);
                    return event.clone(allocator);
                }
            }
        }
    }
}

fn mockRunner(allocator: std.mem.Allocator, runner: Runner) !void {
    std.debug.print("runner invoked\n", .{});
    std.debug.print("[MEM#1] {*}\n", .{allocator.vtable.free});

    const send_socket = runner.connection.send_socket;
    const receive_socket = runner.connection.reply_socket;
    
    var event: core.Event = undefined;
    var left_launching: i32 = 3;
    var left_launched: i32 = 3;

    var source: ?core.EventPayload.TopicBody = null;

    while (true) {
        event = try receive(allocator, runner.connection);
        defer event.deinit();

        std.debug.print("[R:Rec] {}\n", .{event.tag()});

        switch (event) {
            .launched => |v|{
                std.debug.print("[R] launcned: {s}\n", .{v.stage_name});

                if (left_launching > 0) {
                    left_launching -= 1;
                }
                if (left_launching == 0) {
                    std.debug.print("[R] All launched\n", .{});
                    try core.sendEvent(allocator, send_socket, .request_topic);
                }
            },
            .topic => |v| {
                for (v.names) |name| std.debug.print("[R] topic: {s}\n", .{name});
                try core.sendEvent(allocator, send_socket, .begin_watch_path);
            },
            .source_path => |v| {
                std.debug.print("[R]source_path name:{s}, path:{s}, hash:{s}\n", .{v.name, v.path, v.hash});
                const v2 = try v.clone(allocator);
                defer v2.deinit();
                try core.sendEvent(allocator, send_socket, .{.source_path = v2});
            },
            .topic_body => |v| {
                std.debug.print("[R]topic_body/h name:{s}, path:{s}, hash:{s}\n", .{v.header.name, v.header.path, v.header.hash});
                for (v.bodies) |b| std.debug.print("[R]topic_body/b topic:{s}, content:{s}\n", .{b.topic, b.content});
                source = try v.clone(allocator);
                try core.sendEvent(allocator, send_socket, .ready_topic_body);
           },
            .ready_topic_body => {
                if (source) |ss| {
                    try core.sendEvent(allocator, send_socket, .{.topic_body = ss});
                    source = null;
                }
                else {
                    try core.sendEvent(allocator, send_socket, .end_watch_path);
                }
            },
            .end_watch_path => {
                try core.sendEvent(allocator, receive_socket, .quit);
                // try core.sendEvent(allocator, send_socket, .end_watch_path);
            },
            .finish_topic_body => {
                try core.sendEvent(allocator, receive_socket, .quit);
            },
            .finish_generate => {
                try core.sendEvent(allocator, receive_socket, .quit);
            },
            .quit_accept => |v| {
                std.debug.print("[R] exited: {s}\n", .{v.stage_name});
                left_launched -= 1;
                if (left_launched <= 0) {
                    break;
                }
            },
            else => {}
        }
    }
}