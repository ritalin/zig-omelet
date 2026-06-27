const std = @import("std");
const clap = @import("clap");

pub const types = struct {
    pub const Descriptor = struct {
        name: []const u8,
        description: ?[]const u8 = null,
    };

    pub fn HelpConfig(comptime _Tag: type) type {
        return struct {
            tag: Tag,
            sections: []const HelpConfig(Tag),

            pub const Tag = _Tag;
        };
    }

    pub const GuestHelpTag = enum {
        toplevel,
        title,
        base_args,
        extra_args,
    };
};

pub fn ArgHelpRenderer(comptime Tag: type) type {
    return struct {
        on_render: Self.RenderhelpFn,

        const Self = @This();
        const Config = types.HelpConfig(Tag);
        const Map = std.enums.EnumMap(Tag, ?types.Descriptor);

        pub fn init(on_render: RenderhelpFn) Self {
            return .{
                .on_render = on_render,
            };
        }

        pub fn renderToStderr(self: *const Self, config: *const Self.Config) !void {
            var buffer: [1024]u8 = undefined;
            const t = std.debug.lockStderr(&buffer).terminal();
            defer std.debug.unlockStderr();

            try self.renderInternal(t.writer, config);
        }

        fn renderInternal(self: *const Self, writer: *std.Io.Writer, config: *const Self.Config) !void {
            try self.renderConfig(writer, config, 0);
            try writer.flush();
        }

        fn renderConfig(self: *const Self, writer: *std.Io.Writer, config: *const Self.Config, depth: usize) !void {
            for (config.sections) |section| {
                try writer.writeByte('\n');
                try (self.on_render)(writer, config, &section, depth + 1);

                try self.renderConfig(writer, &section, depth + 1);
            }
        }

        pub const RenderhelpFn = *const fn (writer: *std.Io.Writer, parent_config: *const Self.Config, section_config: *const Self.Config, depth: usize) anyerror!void;

        pub const Support = struct {
            pub fn renderTitle(writer: *std.Io.Writer, desc: ?types.Descriptor) !void {
                try renderDescriptor(writer, desc, .{ .name_after_colon = true });
            }

            pub fn renderDescriptor(writer: *std.Io.Writer, desc: ?types.Descriptor, options: HelpDecorateOptions) !void {
                if (desc == null) return;
                if (options.indent) |indent| {
                    try writer.splatByteAll(' ', indent);
                }
                if (options.name_width) |w| {
                    try writer.print("{s:<[w]}", .{.@"0" = desc.?.name, .w = w});
                }
                else {
                    try writer.writeAll(desc.?.name);
                }
                if (options.name_after_colon) {
                    try writer.writeByte(':');
                }

                try writer.splatByteAll(' ', options.description_indent);
                if (desc.?.description) |d| {
                    try writer.writeAll(d);
                }
                try writer.splatByteAll('\n', options.spacing_between_parameters + 1);
            }

            pub fn renderArgs(writer: *std.Io.Writer, comptime ArgId: type, depth: usize) !void {
                const options: clap.HelpOptions = .{.description_on_new_line = false, .spacing_between_parameters = 0, .description_indent = 0, .indent = 4 * depth};
                try clap.help(writer, ArgId, ArgId.Decls, options);
            }
        };
    };
}

pub const HelpDecorateOptions = struct {
    spacing_between_parameters: usize = 0,
    description_indent: usize = 1,
    name_width: ?usize = null,
    indent: ?usize = null,
    name_after_colon: bool = false,
};
