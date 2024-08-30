const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const help = @import("../settings/help.zig");

pub fn BindMap(comptime SettingType: type) type {
    return struct {
        pub const Fn = *const fn (setting: SettingType, args: *std.ArrayList(core.Symbol)) anyerror!core.settings.LoadResult(void, help.ArgHelpSetting) ;
        pub const KV = struct {std.meta.FieldEnum(SettingType), Fn};

        pub const Entry = struct {
            name: core.Symbol,
            bind_fn: Fn,
        };

        pub fn init(comptime kvs: []const KV) std.StaticStringMap(Entry) {
            comptime var map_kvs: [kvs.len](struct {core.Symbol, Entry}) = undefined;
            for (kvs, 0..) |kv, i| {
                map_kvs[i] = .{ @tagName(kv[0]), kv[1] };
            }

            return std.StaticStringMap(Entry).initComptime(&map_kvs);
        }
    };
}

pub fn findDecl(comptime Id: type, comptime decls: []const clap.Param(Id), comptime id: Id) clap.Param(Id) {
    inline for (decls) |decl| {
        if (decl.id == id) {
            std.debug.assert(decl.names.long != null);
            return decl;
        }
    }

    @compileError(std.fmt.comptimePrint("Not contained CLI arg setting: {}",.{id}));
}
