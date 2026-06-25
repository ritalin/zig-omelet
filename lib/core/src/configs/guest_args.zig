const root = @import("../root.zig");

const DescriptionMap = root.settings.types.DescriptionMap;

pub const GuestBaseConfiig = struct {
    pub fn ArgId(comptime descriptions: DescriptionMap) type {
        return enum {
            req_rep,
            pub_sub,
            push_pull,
            log_level,
            log_style,
            no_color,

            pub const Decls = &.{
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
};

pub const GuestWatch = struct {
    pub fn ArgId(comptime descriptions: DescriptionMap) type {
        return enum {
            source_dir_set,
            schema_dir_set,
            include_filter_set,
            exclude_filter_set,
            watch,

            pub const Decls = & .{
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
};

pub const GuestExtract = struct {
    pub fn ArgId(comptime descriptions: DescriptionMap) type {
        return enum {
            schema_dir_set,

            pub const Decls = &.{
                .{.id = .schema_dir_set, .names = .{.long = "schema-dir"}, .takes_value = .one},
            };

            const desc_view = root.settings.ArgHelp(@This(), descriptions);
            pub const description = desc_view.description;
            pub const value = desc_view.value;
        };
    }
};

pub const GuestGenerate = struct {
    pub fn ArgId(comptime descriptions: DescriptionMap) type {
        return enum {
            output_dir_path,

            pub const Decls = &.{
                .{.id = .output_dir_path, .names = .{.long = "output-dir"}, .takes_value = .one},
            };

            const desc_view = root.settings.ArgHelp(@This(), descriptions);
            pub const description = desc_view.description;
            pub const value = desc_view.value;
        };   
    }
};

pub const GuestInitConfig = struct {
    pub fn ArgId(comptime descriptions: DescriptionMap) type {
        return enum {


            pub const Decls = &.{

            };

            const desc_view = root.settings.ArgHelp(@This(), descriptions);
            pub const description = desc_view.description;
            pub const value = desc_view.value;
        };        
    }
};
