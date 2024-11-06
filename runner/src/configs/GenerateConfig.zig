const std = @import("std");
const clap = @import("clap");
const core = @import("core");

const log = core.Logger.TraceDirect(@import("build_options").app_context);

const help = @import("../settings/help.zig");
const mappings = @import("./bind_mappings.zig");
const GenerateSetting = @import("../settings/commands/Generate.zig");
const DufaultArg = @import("../settings/default_args.zig");

pub const ArgId = std.meta.FieldEnum(GenerateSetting);

pub fn applyValue(setting: GenerateSetting, arg_id: ArgId, args: *std.ArrayList(core.Symbol)) !core.settings.LoadResult(void, help.ArgHelpSetting) {
    return switch (arg_id) {
        .source_dir_set => Binder.SourceDir.bind(setting, args),
        .schema_dir_set => Binder.SchemaDir.bind(setting, args),
        .output_dir_path => Binder.OutputDir.bind(setting, args),
        .include_filter => Binder.IncludeFilter.bind(setting, args),
        .exclude_filter => Binder.ExcludeFilter.bind(setting, args),
        .watch => Binder.WatchMode.bind(setting, args),
    };
}

pub fn argName(arg_id: ArgId) core.Symbol {
    return switch (arg_id) {
        .source_dir_set => Binder.SourceDir.name,
        .schema_dir_set => Binder.SchemaDir.name,
        .output_dir_path => Binder.OutputDir.name,
        .include_filter => Binder.IncludeFilter.name,
        .exclude_filter => Binder.ExcludeFilter.name,
        .watch => Binder.WatchMode.name,
    };
}

const Binder = struct {
    const ArgId = GenerateSetting.ArgId(.{});
    const decls = Binder.ArgId.Decls;

    const SourceDir = struct {
        const name = "--" ++ mappings.findDecl(Binder.ArgId, decls, .source_dir).names.long.?;

        fn bind(setting: GenerateSetting, args: *std.ArrayList(core.Symbol)) !core.settings.LoadResult(void, help.ArgHelpSetting)  {
            for (setting.source_dir_set) |path| {
                try args.append(name);
                try args.append(path);
            }

            return .success;
        }
    };
    const SchemaDir = struct {
        const name = "--" ++ mappings.findDecl(Binder.ArgId, decls, .schema_dir).names.long.?;

        fn bind(setting: GenerateSetting, args: *std.ArrayList(core.Symbol)) !core.settings.LoadResult(void, help.ArgHelpSetting)  {
            for (setting.schema_dir_set) |path| {
                try args.append(name);
                try args.append(path);
            }
            return .success;
        }
    };
    const OutputDir = struct {
        const name = "--" ++ mappings.findDecl(Binder.ArgId, decls, .output_dir).names.long.?;

        fn bind(setting: GenerateSetting, args: *std.ArrayList(core.Symbol)) !core.settings.LoadResult(void, help.ArgHelpSetting)  {
            try args.append(name);
            try args.append(setting.output_dir_path);

            return .success;
        }
    };
    const WatchMode = struct {
        const name = "--" ++ mappings.findDecl(Binder.ArgId, decls, .watch).names.long.?;

        fn bind(setting: GenerateSetting, args: *std.ArrayList(core.Symbol)) !core.settings.LoadResult(void, help.ArgHelpSetting)  {
            if (setting.watch) {
                try args.append(name);
            }

            return .success;
        }
    };
    const IncludeFilter = struct {
        const name = "--" ++ mappings.findDecl(Binder.ArgId, decls, .include_filter).names.long.?;

        fn bind(setting: GenerateSetting, args: *std.ArrayList(core.Symbol)) !core.settings.LoadResult(void, help.ArgHelpSetting)  {
            for (setting.include_filter) |filter| {
                try args.append(name);
                try args.append(filter);
            }
            return .success;
        }
    };
    const ExcludeFilter = struct {
        const name = "--" ++ mappings.findDecl(Binder.ArgId, decls, .exclude_filter).names.long.?;

        fn bind(setting: GenerateSetting, args: *std.ArrayList(core.Symbol)) !core.settings.LoadResult(void, help.ArgHelpSetting)  {
            for (setting.exclude_filter) |filter| {
                try args.append(name);
                try args.append(filter);
            }
            return .success;
        }
    };
};
