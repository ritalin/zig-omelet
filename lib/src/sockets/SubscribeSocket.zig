const std = @import("std");
const zmq = @import("zmq");

const types = @import("../types.zig");
const helpers = @import("../helpers.zig");

const Self = @This();
const EventTypeSet = std.enums.EnumSet(types.EventType);

allocator: std.mem.Allocator,
socket: *zmq.ZSocket,
filters: EventTypeSet,

pub fn init(allocator: std.mem.Allocator, context: *zmq.ZContext) !*Self {
    const subscriber = try zmq.ZSocket.init(zmq.ZSocketType.Sub, context);

    const self = try allocator.create(Self);
    self.* = .{
        .allocator = allocator,
        .socket = subscriber,
        .filters = EventTypeSet.initEmpty(),
    };

    return self;
}

pub fn deinit(self: *Self) void {
    self.socket.deinit();
    self.allocator.destroy(self);
    self.* = undefined;
}

pub fn connect(self: *Self, endpoint_channel: types.Symbol) !void {
    return self.socket.connect(endpoint_channel);
}

pub fn addFilters(self: *Self, filters: types.EventTypes) !void {
    try helpers.addSubscriberFilters(self.socket, filters);

    self.filters.setUnion(EventTypeSet.init(filters));
}

const SubscribeFilterList = struct {
    allocator: std.mem.Allocator,
    filters: EventTypeSet,

    pub fn format(self: SubscribeFilterList, comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
        var it = self.filters.iterator();
        var i: usize = 0;

        while (it.next()) |filter| {
            if (i > 0) { try writer.writeAll(", "); }

            try writer.writeAll("'");
            try writer.writeAll(@tagName(filter));
            try writer.writeAll("'");

            i += 1;
        }
    }
};

pub fn listFilters(self: Self) SubscribeFilterList {
    return .{
        .allocator = self.allocator,
        .filters = self.filters,
    };
}