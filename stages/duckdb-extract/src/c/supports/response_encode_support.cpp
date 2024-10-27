#include <format>

#include <magic_enum/magic_enum.hpp>

#include "cbor_encode.hpp"
#include "response_encode_support.hpp"

namespace worker {

auto encodeStatementCount(size_t count) -> std::vector<char> {
    CborEncoder payload_encoder;

    payload_encoder.addUInt(count);

    return payload_encoder.rawBuffer();
}

auto encodeStatementOffset(size_t offset) -> std::vector<char> {
    CborEncoder payload_encoder;

    payload_encoder.addUInt(offset);

    return payload_encoder.rawBuffer();
}

auto encodeTopicBody(const size_t offset, const std::unordered_map<std::string, std::vector<char>>& topic_bodies) -> std::vector<char> {
    CborEncoder payload_encoder;

    stmt_offset: {
        payload_encoder.addUInt(offset);
    }
    topic_body: {
        payload_encoder.addArrayHeader(topic_bodies.size());

        for (auto [topic, payload]: topic_bodies) {
            payload_encoder.addBinaryPair(topic, payload);
        }
    }

    return payload_encoder.rawBuffer();
}

auto encodeWorkerLog(LogLevel log_level, const std::string& id, const size_t offset, const std::string message) -> std::vector<char> {
    CborEncoder payload_encoder;

    log_level: {
        payload_encoder.addString(magic_enum::enum_name(log_level));
    }
    message: {
        payload_encoder.addString(std::format("message: {}, offset: {}, id: {}", message, offset, id));
    }

    return payload_encoder.rawBuffer();
}

}