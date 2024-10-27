#pragma once

#include <vector>

class CborEncoder {
    std::vector<char> buf;
public:
    CborEncoder(): buf() {}
public:
    auto addUInt(uint64_t value) -> void;
    auto addString(std::string_view value) -> void;
    auto addBool(bool value) -> void;
    auto addNull() -> void;
    auto addArrayHeader(size_t len) -> void;
    auto addStringPair(const std::string& key, const std::string& value) -> void;
    auto addBinaryPair(const std::string& key, const std::vector<char>& value) -> void;
    auto addUIntPair(const std::string& key, uint64_t value) -> void;
    auto concatBinary(const CborEncoder& encoder) -> void;
    auto concatBinary(const std::vector<char>& buffer) -> void;
public:
    auto static encodeUInt(uint64_t value) -> std::vector<char>;
    auto static encodeBool(bool value) -> std::vector<char>;
    auto static encodeNull() -> std::vector<char>;
public:
    auto rawBuffer() const -> std::vector<char>;
    auto build() const -> std::vector<char>;
};