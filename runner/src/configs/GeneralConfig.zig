const std = @import("std");
const core = @import("core");

const help = @import("../settings/help.zig");
const mappings = @import("./bind_mappings.zig");
const GeneralSetting = @import("../settings/commands/GeneralSetting.zig");

pub fn apply(setting: GeneralSetting, args: *std.ArrayList(core.Symbol)) !void {
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
    const ArgId = GeneralSetting.StageArgId(.{});
    const decls = ArgId.Decls;

    const RequestChannel = struct {
        const name = "--" ++ mappings.findDecl(ArgId, decls, .request_channel).names.long.?;
        fn bind(eps: core.Endpoints, args: *std.ArrayList(core.Symbol)) !core.settings.LoadResult(void, help.ArgHelpSetting)  {
            const decl = comptime mappings.findDecl(ArgId, decls, .request_channel);
            try args.append("--" ++ decl.names.long.?);
            try args.append(eps.req_rep);

            return .success;
        }
    };
    const SubscribeChannel = struct {
        const name = "--" ++ mappings.findDecl(ArgId, decls, .subscribe_channel).names.long.?;
        fn bind(eps: core.Endpoints, args: *std.ArrayList(core.Symbol)) !core.settings.LoadResult(void, help.ArgHelpSetting)  {
            const decl = comptime mappings.findDecl(ArgId, decls, .subscribe_channel);
            try args.append("--" ++ decl.names.long.?);
            try args.append(eps.pub_sub);

            return .success;
        }
    };
    const LogLevel = struct {
        const name = "--" ++ mappings.findDecl(ArgId, decls, .log_level).names.long.?;
        fn bind(setting: GeneralSetting, args: *std.ArrayList(core.Symbol)) !core.settings.LoadResult(void, help.ArgHelpSetting)  {
            const decl = comptime mappings.findDecl(ArgId, decls, .log_level);
            try args.append("--" ++ decl.names.long.?);
            try args.append(@tagName(setting.log_level));

            return .success;
        }
    };
};
