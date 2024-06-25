#include <sstream>

class CborEncoder {
    std::vector<char> buf;
public:
    CborEncoder(): buf() {}
public:
    auto addUInt(uint64_t value) -> void;
    auto addArrayHeader(size_t len) -> void;
    auto addStringPair(const std::string& key, const std::string& value) -> void;
    auto addUIntPair(const std::string& key, uint64_t value) -> void;
    auto addBinary(const std::vector<char>& payload) -> void;
public:
    auto static encodeUInt(uint64_t value) -> std::vector<char>;
public:
    auto build() const -> std::string;
};