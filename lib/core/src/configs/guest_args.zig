const root = @import("../root.zig");

const DescriptionMap = root.settings.types.DescriptionMap;

pub const GuestWatch = struct {
    pub fn ArgId(comptime descriptions: DescriptionMap) type {
        return enum {
            source_dir_set,
            schema_dir_set,
            include_filter_set,
            exclude_filter_set,
            watch,

            pub const Id = struct {

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

            pub const Id = struct {

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

            pub const Id = struct {

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


            pub const Id = struct {

            };

            const desc_view = root.settings.ArgHelp(@This(), descriptions);
            pub const description = desc_view.description;
            pub const value = desc_view.value;
        };        
    }
};
