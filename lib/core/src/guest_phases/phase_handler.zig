pub fn VTable(comptime GuestStage: type) type {
    return struct {
        on_prepare: ?*const fn (stage: *GuestStage) anyerror!void = null,
    };
}
