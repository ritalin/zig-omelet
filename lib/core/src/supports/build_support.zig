const std = @import("std");

pub const AssetFilePath = struct {
    path: std.Build.Cache.Path,

    pub fn from(path: std.Build.Cache.Path) AssetFilePath {
        return .{
            .path = path,
        };
    }

    pub fn fullPath(self: *const AssetFilePath, build: *std.Build) []const u8 {
        const root_dir_path = self.path.root_dir.handle.realPathFileAlloc(build.graph.io, ".", build.graph.arena) catch @panic("OOM");
        return build.pathJoin(&.{root_dir_path, self.path.sub_path});
    }

    pub fn format(self: *const AssetFilePath, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        try writer.print("AssetFilePath{{ .path = {f} }}\n", .{self.path});
    }
};
