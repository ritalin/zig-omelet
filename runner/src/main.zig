const std = @import("std");
const zmq = @import("zmq");

const IPC_SYNC_END_POINT: []const u8 = "ipc:///tmp/duckdb-extract-placeholder_sync";
const IPC_OUT_END_POINT: []const u8 = "ipc:///tmp/duckdb-extract-placeholder_pub";

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const _allocator = arena.allocator();

    const app_dir_path = try std.fs.selfExeDirPathAlloc(_allocator);
    var app_dir = try std.fs.openDirAbsolute(app_dir_path, .{});
    defer app_dir.close();
    std.debug.print("Runner/dir: {s}\n", .{app_dir_path});

    const stage_transfer_path = try app_dir.realpathAlloc(_allocator, "stage-transfer-placeholder");
    std.debug.print("Transfer/path: {s}\n", .{stage_transfer_path});

    std.debug.print("invoke transfer#2\n", .{});
    var stage_transfer = std.process.Child.init(
        &[_][]const u8 {
            stage_transfer_path
        }, 
        _allocator
    );
    stage_transfer.stderr_behavior = .Ignore;
    stage_transfer.stdout_behavior = .Ignore;

    _ = try stage_transfer.spawn();

    const thread_2 = try std.Thread.spawn(.{}, struct {
        fn execute(allocator: std.mem.Allocator) !void {
            var ctx = try zmq.ZContext.init(allocator);
            defer ctx.deinit();
            
            const sub_socket = try zmq.ZSocket.init(zmq.ZSocketType.Pull, &ctx);
            // const sub_socket = try zmq.ZSocket.init(zmq.ZSocketType.Sub, &ctx);
            // try sub_socket.setSocketOption(.{.ReceiveHighWaterMark = 11000});
            defer sub_socket.deinit();
            // try sub_socket.setSocketOption(.{.Subscribe = ""});
            try sub_socket.connect(IPC_OUT_END_POINT);

            const syncclient = try zmq.ZSocket.init(zmq.ZSocketType.Req, &ctx);
            defer syncclient.deinit();
            try syncclient.connect(IPC_SYNC_END_POINT);

            std.time.sleep(1);

            sync: {
                var msg = try zmq.ZMessage.init(allocator, "");
                defer msg.deinit();
                try syncclient.send(&msg, .{});
                break :sync;
            }
            std.debug.print("Send sync Req#1\n", .{});

            sync_ack: {
                var frame = try syncclient.receive(.{});
                defer frame.deinit();
                break :sync_ack;
            }
            std.debug.print("Receive sync Ack#1\n", .{});

            terminate: while (true) {
                std.debug.print("Begin receive topic \n", .{});
                topic: {
                    var frame = try sub_socket.receive(.{});
                    defer frame.deinit();

                    std.debug.print("topic: {s}\n", .{try frame.data()});
                    break :topic;
                }

                std.debug.print("Begin receive data \n", .{});
                data: {
                    var frame = try sub_socket.receive(.{});
                    defer frame.deinit();

                    const msg = try frame.data();
                    std.debug.print("data: {s}\n", .{msg});

                    if (std.mem.eql(u8, msg, ".exit")) { break :terminate; }

                    break :data;
                }
                std.debug.print("End receive\n", .{});
            }

            std.debug.print("Runner terminated\n", .{});
        }
    }.execute, .{_allocator});

    thread_2.join();

    _ = try stage_transfer.kill();
}
