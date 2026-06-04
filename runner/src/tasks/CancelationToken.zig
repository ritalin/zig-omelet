

pub const init: Self = .{};

state: bool = false,

const Self = @This();

pub fn cancel(self: *Self) void {
    @atomicStore(bool, &self.state, true, .release);
}

pub fn isCanceled(self: *const Self) bool {
    return @atomicLoad(bool, &self.state, .acquire);
}