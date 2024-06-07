const std = @import("std");
const zmq = @import("zmq");

// (control) Client -> Server
const CMD_C2S_END_POINT = "ipc:///tmp/duckdb-ext-ph_cmd_c2s";
// (control) Server -> Client
const CMD_S2C_END_POINT = "ipc:///tmp/duckdb-ext-ph_cmd_s2c";
// const CMD_S2C_END_POINT = "tcp://*:6001";


const APP_CONTEXT = "runner";

pub fn run(allocator: std.mem.Allocator, stage_count: struct {extract: usize, generate: usize}) !void {
    std.debug.print("({s}) Beginning\n", .{APP_CONTEXT});

    var ctx = try zmq.ZContext.init(allocator);
    defer ctx.deinit();

    const cmd_c2s_socket = try zmq.ZSocket.init(zmq.ZSocketType.Pull, &ctx);
    defer cmd_c2s_socket.deinit();
    try cmd_c2s_socket.bind(CMD_C2S_END_POINT);

    const cmd_s2c_socket = try zmq.ZSocket.init(zmq.ZSocketType.Pub, &ctx);
    defer cmd_s2c_socket.deinit();
    try cmd_s2c_socket.bind(CMD_S2C_END_POINT);
    
    // const sub_socket = try zmq.ZSocket.init(zmq.ZSocketType.Pull, &ctx);
    // defer sub_socket.deinit();
    // try sub_socket.bind(IPC_OUT_END_POINT);

    std.time.sleep(1);

    ack_launch: {
    var left_count = stage_count.extract + stage_count.generate;

        while (left_count > 0) {
            std.debug.print("({s}) Wait launching ({})\n", .{APP_CONTEXT, left_count});
            var frame = try cmd_c2s_socket.receive(.{});
            defer frame.deinit();

            if (std.mem.eql(u8, try frame.data(), ".launched")) {
                left_count -= 1;
            }
        }

        std.debug.print("({s}) End sync launch\n", .{APP_CONTEXT});
        break :ack_launch;
    }

    sync_topic: {
        var msg = try zmq.ZMessage.init(allocator, ".begin_topic");
        defer msg.deinit();
        try cmd_s2c_socket.send(&msg, .{});
        break :sync_topic;
    }

    var topics = std.BufSet.init(allocator);
    defer topics.deinit();

    ack_topic: {
        var left_count = stage_count.extract;

        // TODO handle .topic

        loop: while (true) {
            std.debug.print("({s}) Wait sync topic\n", .{APP_CONTEXT});

            var frame = try cmd_c2s_socket.receive(.{});
            defer frame.deinit();
            std.debug.print("({s}) Received message: {s}\n", .{APP_CONTEXT, try frame.data()});

            if (std.mem.eql(u8, try frame.data(), ".end_topic")) {
                left_count -= 1;
            }
            if (left_count <= 0) { break :loop; }
        }
        std.debug.print("({s}) End sync topic \n", .{APP_CONTEXT});
        break :ack_topic;
    }

    // TODO .start_session
    

    // loop: while (true) {
    //     topic: {
    //         var frame = try sub_socket.receive(.{});
    //         defer frame.deinit();

    //         std.debug.print("topic: {s}\n", .{try frame.data()});
    //         break :topic;
    //     }

    //     std.debug.print("Begin receive data \n", .{});
    //     data: {
    //         var frame = try sub_socket.receive(.{});
    //         defer frame.deinit();

    //         const msg = try frame.data();
    //         std.debug.print("data: {s}\n", .{msg});

    //         if (std.mem.eql(u8, msg, ".exit")) { break :loop; }

    //         break :data;
    //     }
    //     std.debug.print("End receive\n", .{});
    // }

    std.debug.print("Runner terminated\n", .{});
}