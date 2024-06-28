#include <sstream>

class CborEncoder {
    std::vector<char> buf;
public:
    CborEncoder(): buf() {}
public:
    auto addUInt(uint64_t value) -> void;
    auto addString(std::string value) -> void;
    auto addArrayHeader(size_t len) -> void;
    auto addStringPair(const std::string& key, const std::string& value) -> void;
    auto addUIntPair(const std::string& key, uint64_t value) -> void;
    auto concatBinary(const CborEncoder& encoder) -> void;
public:
    auto static encodeUInt(uint64_t value) -> std::vector<char>;
public:
    auto rawBuffer() const -> std::string;
    auto build() const -> std::string;
};