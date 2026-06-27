const std = @import("std");
const core = @import("core");

const ArgHelp = @import("../help/ArgHelp.zig");
const mappings = @import("./bind_mappings.zig");
const BaseSetting = @import("../settings/commands/BaseSetting.zig");

pub fn apply(setting: BaseSetting, args: *std.ArrayList(core.Symbol)) !void {
    request_channel: {
        _ = try Binder.RequestChannel.bind(setting.stage_endpoints, args);
        break:request_channel;
    }
    pub_sub_channel: {
        _ = try Binder.SubscribeChannel.bind(setting.stage_endpoints, args);
        break:pub_sub_channel;
    }
    log_level: {
        _ = try Binder.LogLevel.bind(setting, args);
        break:log_level;
    }
}

const Binder = struct {
    const ArgId = BaseSetting.StageArgId(.{});
    const decls = ArgId.Decls;

    const RequestChannel = struct {
        const name = "--" ++ mappings.findDecl(ArgId, decls, .request_channel).names.long.?;
        fn bind(eps: core.Endpoints, args: *std.ArrayList(core.Symbol)) !core.settings.LoadResult(void, ArgHelp.Config)  {
            const decl = comptime mappings.findDecl(ArgId, decls, .request_channel);
            try args.append("--" ++ decl.names.long.?);
            try args.append(eps.req_rep);

            return .success;
        }
    };
    const SubscribeChannel = struct {
        const name = "--" ++ mappings.findDecl(ArgId, decls, .subscribe_channel).names.long.?;
        fn bind(eps: core.Endpoints, args: *std.ArrayList(core.Symbol)) !core.settings.LoadResult(void, ArgHelp.Config)  {
            const decl = comptime mappings.findDecl(ArgId, decls, .subscribe_channel);
            try args.append("--" ++ decl.names.long.?);
            try args.append(eps.pub_sub);

            return .success;
        }
    };
    const LogLevel = struct {
        const name = "--" ++ mappings.findDecl(ArgId, decls, .log_level).names.long.?;
        fn bind(setting: BaseSetting, args: *std.ArrayList(core.Symbol)) !core.settings.LoadResult(void, ArgHelp.Config)  {
            const decl = comptime mappings.findDecl(ArgId, decls, .log_level);
            try args.append("--" ++ decl.names.long.?);
            try args.append(@tagName(setting.log_level));

            return .success;
        }
    };
};
