const clap = @import("clap");

const root = @import("../root.zig");

const DescriptionMap = root.settings.types.DescriptionMap;
const DescriptionItem = root.settings.types.DescriptionItem;

pub const GuestBaseConfig = struct {
    pub fn ArgId(comptime descriptions: DescriptionMap) type {
        return enum {
            req_rep,
            pub_sub,
            push_pull,
            log_level,
            log_style,
            no_color,

            pub const Decls: []const clap.Param(ArgId(descriptions)) = &.{
                .{.id = .req_rep, .names = .{.long = "reqrep-channel"}, .takes_value = .one},
                .{.id = .pub_sub, .names = .{.long = "pubsub-channel"}, .takes_value = .one},
                .{.id = .push_pull, .names = .{.long = "pushpull-channel"}, .takes_value = .one},
                .{.id = .log_level, .names = .{.long = "log-level"}, .takes_value = .one},
                .{.id = .log_style, .names = .{.long = "log-style"}, .takes_value = .one},
                .{.id = .no_color, .names = .{.long = "no-color"}, .takes_value = .none},
            };
            const desc_view = root.settings.types.ArgHelp(@This(), descriptions);
            pub const description = desc_view.description;
            pub const value = desc_view.value;
        };
    }

    pub const DescMap = DescriptionMap.initComptime(.{
        .{@tagName(.req_rep), DescriptionItem{.desc = "Comminicate Req/Rep endpoint for nng", .value = "CHANNEL"}},
        .{@tagName(.pub_sub), DescriptionItem{.desc = "Comminicate Pub/Sub endpoint for nng", .value = "CHANNEL"}},
        .{@tagName(.push_pull), DescriptionItem{.desc = "Comminicate Push/Pull endpoint for nng", .value = "CHANNEL"}},
        .{@tagName(.log_level), DescriptionItem{.desc = "Pass through log level (err / warn / info / debug / trace). default: info", .value = "LEVEL",}},
        .{@tagName(.log_style), DescriptionItem{.desc = "Set log output style (stderr / integrated / discard). default: stderr", .value = "STYLE"}},
        .{@tagName(.no_color), DescriptionItem{.desc = "Disable colored log", .value = ""}},
    });
};

pub const GuestWatch = struct {
    pub fn ArgId(comptime descriptions: DescriptionMap) type {
        return enum {
            source_dir_set,
            schema_dir_set,
            include_filter_set,
            exclude_filter_set,
            watch,

            pub const Decls: []const clap.Param(ArgId(descriptions)) = & .{
                .{.id = .source_dir_set, .names = .{.long = "source-dir"}, .takes_value = .one},
                .{.id = .schema_dir_set, .names = .{.long = "schema-dir"}, .takes_value = .one},
                .{.id = .include_filter_set, .names = .{.long = "include-filter"}, .takes_value = .one},
                .{.id = .exclude_filter_set, .names = .{.long = "exclude-filter"}, .takes_value = .one},
                .{.id = .watch, .names = .{.long = "watch"}, .takes_value = .none},
            };

            const desc_view = root.settings.types.ArgHelp(@This(), descriptions);
            pub const description = desc_view.description;
            pub const value = desc_view.value;
        };
    }

    pub const DescMap = DescriptionMap.initComptime(.{
        .{@tagName(.source_dir_set), DescriptionItem{.desc = "Source SQL directores or files", .value = "PATH"}},
        .{@tagName(.schema_dir_set), DescriptionItem{.desc = "Schema SQL directores or files", .value = "PATH"}},
        .{@tagName(.include_filter_set), DescriptionItem{.desc = "Filter passing source/schema SQL directores or files satisfied", .value = "VALUE"}},
        .{@tagName(.exclude_filter_set), DescriptionItem{.desc = "Filter rejecting source/schema SQL directores or files satisfied", .value = "VALUE"}},
        .{@tagName(.watch), DescriptionItem{.desc = "Enter to watch-mode", .value = ""}},
    });
};

pub const GuestExtract = struct {
    pub fn ArgId(comptime descriptions: DescriptionMap) type {
        return enum {
            schema_dir_set,

            pub const Decls: []const clap.Param(ArgId(descriptions)) = &.{
                .{.id = .schema_dir_set, .names = .{.long = "schema-dir"}, .takes_value = .one},
            };

            const desc_view = root.settings.types.ArgHelp(@This(), descriptions);
            pub const description = desc_view.description;
            pub const value = desc_view.value;
        };
    }

    pub const DescMap = DescriptionMap.initComptime(.{
        .{@tagName(.schema_dir_set), DescriptionItem{.desc = "Schema SQL folder", .value = "PATH"}},
    });
};

pub const GuestGenerate = struct {
    pub fn ArgId(comptime descriptions: DescriptionMap) type {
        return enum {
            output_dir_path,

            pub const Decls: []const clap.Param(ArgId(descriptions)) = &.{
                .{.id = .output_dir_path, .names = .{.long = "output-dir"}, .takes_value = .one},
            };

            const desc_view = root.settings.types.ArgHelp(@This(), descriptions);
            pub const description = desc_view.description;
            pub const value = desc_view.value;
        };   
    }

    pub const DescMap = DescriptionMap.initComptime(.{
        .{@tagName(.output_dir_path), DescriptionItem{.desc = "Output folder", .value = "PATH"}},
    });
};

pub const GuestInitConfig = struct {
    pub fn ArgId(comptime descriptions: DescriptionMap) type {
        return enum {


            pub const Decls = &.{

            };

            const desc_view = root.settings.types.ArgHelp(@This(), descriptions);
            pub const description = desc_view.description;
            pub const value = desc_view.value;
        };        
    }
};
