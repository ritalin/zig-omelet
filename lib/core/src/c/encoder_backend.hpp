#pragma once

#include <vector>
#include <span>

#include <nng/nng.h>

template<typename T>
concept WriterBackend =
requires(T& w, const void* ptr, size_t len)
{
    { w.write(ptr, len) } -> std::same_as<void>;
    { w.rawBuffer() } -> std::convertible_to<std::span<const std::byte>>;
    { w.flush() } -> std::same_as<void>;
};

class VectorBackend {
    std::vector<std::byte> buf;
public:
    auto write(const void* ptr, size_t len) -> void;
    auto rawBuffer() const -> std::span<const std::byte>;
    auto flush() -> void;
};

class NngBackend final {
    nng_msg *msg;
    std::span<std::byte> buffer;
    size_t end;
public:
    NngBackend(): NngBackend(1024) {}
    explicit NngBackend(size_t capacity);
    NngBackend(NngBackend&&) noexcept;
    NngBackend& operator=(NngBackend&&) noexcept;
    ~NngBackend();

    NngBackend(const NngBackend&) = delete;
    NngBackend& operator=(const NngBackend&) = delete;

    auto release() -> nng_msg*;

    auto write(const void* ptr, size_t len) -> void;
    auto rawBuffer() const -> std::span<const std::byte>;
    auto flush() -> void;
};
