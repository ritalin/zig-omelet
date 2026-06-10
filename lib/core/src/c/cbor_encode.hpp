#pragma once

#include <vector>
#include <optional>

#include "encoder_backend.hpp"

template <WriterBackend Backend>
class CborEncoder {
    std::optional<Backend> owned_;
    Backend& backend;
public:
    CborEncoder(Backend& backend): backend(backend) {}
    CborEncoder(Backend&& backend): owned_(std::move(backend)), backend(owned_.value()) {}
public:
    auto addUInt(uint64_t value) -> void;
    auto addString(std::string_view value) -> void;
    auto addBool(bool value) -> void;
    auto addNull() -> void;
    auto addArrayHeader(size_t len) -> void;
    auto addStringPair(const std::string& key, const std::string& value) -> void;
    auto addBinaryPair(const std::string& key, const std::span<const std::byte>& value) -> void;
    auto addUIntPair(const std::string& key, uint64_t value) -> void;
    auto addAggregateSlice(std::vector<CborEncoder<VectorBackend>>&& encoders) -> void;
public:
    auto rawBuffer() const -> std::span<const std::byte>;
    auto flush() -> void;
};
