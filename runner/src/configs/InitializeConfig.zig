const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const log = core.Logger.TraceDirect(@import("build_options").app_context);

const help = @import("../settings/help.zig");
const mappings = @import("./bind_mappings.zig");
const InitializeSetting = @import("../settings/commands/Initialize.zig");

pub const ArgId = std.meta.FieldEnum(InitializeSetting);

pub fn applyValue(setting: InitializeSetting, arg_id: ArgId, args: *std.ArrayList(core.Symbol)) !core.settings.LoadResult(void, help.ArgHelpSetting) {
    return switch (arg_id) {
        .source_dir_path => Binder.SourceDir.bind(setting, args),
        .output_dir_path => Binder.OutputDir.bind(setting, args),
        .category => Binder.Category.bind(setting, args),
        .command => Binder.Subcommand.bind(setting, args),
    };
}

pub fn argName(arg_id: ArgId) core.Symbol {
    return switch (arg_id) {
        .source_dir_path => Binder.SourceDir.name,
        .output_dir_path => Binder.OutputDir.name,
        .category => Binder.Category.name,
        .command => Binder.Subcommand.name,
    };
}

const Binder = struct {
    const decls = ArgId.Decls;

    const SourceDir = struct {
        const name = "--source-dir";

        fn bind(setting: InitializeSetting, args: *std.ArrayList(core.Symbol)) !core.settings.LoadResult(void, help.ArgHelpSetting)  {
            try args.append(name);
            try args.append(setting.source_dir_path);

            return .success;
        }
    };
    const OutputDir = struct {
        const name = "--output-dir";

        fn bind(setting: InitializeSetting, args: *std.ArrayList(core.Symbol)) !core.settings.LoadResult(void, help.ArgHelpSetting)  {
            try args.append(name);
            try args.append(setting.output_dir_path);

            return .success;
        }
    };
    const Category = struct {
        const name = "--category";

        fn bind(setting: InitializeSetting, args: *std.ArrayList(core.Symbol)) !core.settings.LoadResult(void, help.ArgHelpSetting)  {
            try args.append(name);
            try args.append(@tagName(setting.category));

            return .success;
        }
    };
    const Subcommand = struct {
        const name = "--command";

        fn bind(setting: InitializeSetting, args: *std.ArrayList(core.Symbol)) !core.settings.LoadResult(void, help.ArgHelpSetting)  {
            try args.append(name);
            try args.append(@tagName(setting.command));

            return .success;
        }
    };
};