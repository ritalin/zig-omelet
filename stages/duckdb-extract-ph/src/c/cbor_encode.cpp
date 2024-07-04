#include "cbor_encode.hpp"
#include "cbor/encoder.h"

struct CborTypes {
    static const uint64_t PINTEGER = 0;
    static const uint64_t NINTEGER = 1;
    static const uint64_t BITES = 2;
    static const uint64_t STRING = 3;
    static const uint64_t ARRAY = 4;
    static const uint64_t MAP = 5;
};

const size_t MAX_HEADER_SIZE = 9;

auto cborHeader(uint32_t id, size_t len) -> std::string {
    cbor_writer_t writer;
    char buf[MAX_HEADER_SIZE] = {}; 
    cbor_writer_init(&writer, buf, MAX_HEADER_SIZE);   

    cbor_encode_unsigned_integer(&writer, len);

    writer.buf[0] |= ((id & 0b0111) << 5);

    return std::string(reinterpret_cast<char*>(writer.buf), writer.bufidx);
}

auto CborEncoder::encodeUInt(uint64_t value) -> std::vector<char> {
    cbor_writer_t writer;
    char buf[] = {0, 0}; 
    cbor_writer_init(&writer, buf, 2);   

    cbor_encode_unsigned_integer(&writer, value);

    auto b = reinterpret_cast<char*>(writer.buf);
    return std::vector<char>(b, b + writer.bufidx);
}

auto CborEncoder::addUInt(uint64_t value) -> void {
    auto data = std::move(CborEncoder::encodeUInt(value));
    std::move(data.begin(), data.end(), std::back_inserter(this->buf));
}

auto CborEncoder::addString(std::string value) -> void {
    auto inserter = std::back_inserter(this->buf);
    header: {
        auto header = std::move(cborHeader(CborTypes::STRING, value.size()));
        inserter = std::move(header.begin(), header.end(), inserter);
    }
    payload: {
        inserter = std::copy(value.begin(), value.end(), inserter);
    }
}

auto CborEncoder::addArrayHeader(size_t len) -> void {
    auto header = std::move(cborHeader(CborTypes::ARRAY, len));
    std::move(header.begin(), header.end(), std::back_inserter(this->buf));
}

auto CborEncoder::concatBinary(const CborEncoder& encoder) -> void {
    std::copy(encoder.buf.begin(), encoder.buf.end(), std::back_inserter(this->buf));
}

auto CborEncoder::addStringPair(const std::string& key, const std::string& value) -> void {
    auto inserter = std::back_inserter(this->buf);
    tuple: {
        auto header = std::move(cborHeader(CborTypes::ARRAY, 2));
        inserter = std::move(header.begin(), header.end(), inserter);
    }
    key: {
        auto header = std::move(cborHeader(CborTypes::STRING, key.size()));
        inserter = std::move(header.begin(), header.end(), inserter);
        inserter = std::copy(key.begin(), key.end(), inserter);
    }
    value: {
        auto header = std::move(cborHeader(CborTypes::STRING, value.size()));
        inserter = std::move(header.begin(), header.end(), inserter);
        inserter = std::copy(value.begin(), value.end(), inserter);
    }
}

auto CborEncoder::addUIntPair(const std::string& key, uint64_t value) -> void {
    auto inserter = std::back_inserter(this->buf);
    tuple: {
        auto header = std::move(cborHeader(CborTypes::ARRAY, 2));
        inserter = std::move(header.begin(), header.end(), inserter);
    }
    key: {
        auto header = std::move(cborHeader(CborTypes::STRING, key.size()));
        inserter = std::move(header.begin(), header.end(), inserter);
        inserter = std::copy(key.begin(), key.end(), inserter);
    }
    value: {
        auto data = std::move(CborEncoder::encodeUInt(value));
        inserter = std::move(data.begin(), data.end(), inserter);
    }
}

auto CborEncoder::rawBuffer() const -> std::string {
    return std::string(this->buf.begin(), this->buf.end());
}
auto CborEncoder::build() const -> std::string {
    CborEncoder encoder;
    auto inserter = std::back_inserter(encoder.buf);
    header: {
        auto header = std::move(cborHeader(CborTypes::STRING, this->buf.size()));
        inserter = std::copy(header.begin(), header.end(), inserter);
    }
    payload: {
        inserter = std::copy(this->buf.begin(), this->buf.end(), inserter);
    }

    return encoder.rawBuffer();
}
