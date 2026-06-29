const std = @import("std");
const root = @import("../root.zig");

const types = root.types;

pub const Hasher = std.crypto.hash.sha2.Sha256;

pub fn makeFileHash(io: std.Io, hasher: *Hasher, base_dir: std.Io.Dir, file_path: types.FilePath) !void {
    hasher.update(file_path);

    var read_buf: [8192]u8 = undefined;
    var hash_block: [8192]u8 = undefined;

    var file = try base_dir.openFile(io, file_path, .{});
    defer file.close(io);

    var reader = file.readerStreaming(io, &read_buf);
    var hash = reader.interface.hashed(hasher, &hash_block);

    var size: usize = 0;
    while (true) {
        const len = hash.reader.discard(.unlimited) catch |err| switch (err) {
            error.EndOfStream => break,
            else => return err,
        };
        if (len == 0) break;
        size += len;
    }
}

pub fn makeDirHash(io: std.Io, hasher: *Hasher, dir_path_abs: types.FilePath) !void {
    hasher.update(dir_path_abs);

    const dir = try std.Io.Dir.openDirAbsolute(io, dir_path_abs, .{});
    defer dir.close(io);

    const stat = try dir.stat(io);
    var ts_buf: [12]u8 = undefined;
    std.mem.writeInt(@TypeOf(stat.mtime.nanoseconds), &ts_buf, stat.mtime.nanoseconds, .little);
    hasher.update(&ts_buf);
}