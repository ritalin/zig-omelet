#include <format>

#include <magic_enum/magic_enum.hpp>

#include "cbor_encode.hpp"
#include "duckdb_worker.h"
#include "response_encode_support.hpp"

namespace worker {

static auto encodeResponseHeader(CborEncoder<NngBackend>& encoder, uint8_t tag, const std::string_view& worker_phase) -> void {
    encoder.addUInt(tag);
    encoder.addString(worker_phase);
}

static auto encodeSourceDescriptor(CborEncoder<NngBackend>& encoder, const SourceDescriptor& desc, size_t offset) -> void {
    encoder.addArrayHeader(4);
    category: {
        encoder.addUInt(desc.topic_category);
    }
    name: {
        encoder.addString(std::string{ desc.name.ptr, desc.name.len });
    }
    dialect: {
        encoder.addString(std::string{ desc.dialect.ptr, desc.dialect.len });
    }
    offset: {
        encoder.addUInt(offset);
    }
    hash: {
        encoder.addString(std::string{ desc.hash.ptr, desc.hash.len });
    }
}

auto encodeStatementCount(
    CborEncoder<NngBackend>& encoder, 
    const std::string_view& stage, 
    const SourceDescriptor& desc, 
    size_t count) -> void
{
    encodeResponseHeader(encoder, desc.response_event_tag, stage);
    encodeSourceDescriptor(encoder, desc, 0);
    
    stmt_name_alt: {
        encoder.addNull();
    }
    response: {
        encoder.addUInt(::worker_progress);
        encoder.addUInt(count);
    }
    encoder.flush();
}

auto encodeStatementOffset(
    CborEncoder<NngBackend>& encoder, 
    const std::string_view& stage, 
    const SourceDescriptor& desc, 
    size_t offset) -> void 
{
    encodeResponseHeader(encoder, desc.response_event_tag, stage);
    encodeSourceDescriptor(encoder, desc, offset);
    
    stmt_name_alt: {
        encoder.addNull();
    }
    response: {
        encoder.addUInt(::worker_skipped);
    }
    encoder.flush();
}

auto encodeTopicBody(
    CborEncoder<NngBackend>& encoder,
    const std::string_view& stage,
    const SourceDescriptor& desc,
    const size_t offset, 
    std::optional<std::string> name_alt, 
    const std::unordered_map<std::string_view, CborEncoder<VectorBackend>>& topic_bodies) -> void 
{
    encodeResponseHeader(encoder, desc.response_event_tag, stage);
    encodeSourceDescriptor(encoder, desc, offset);

    stmt_name_alt: {
        if (name_alt) {
            encoder.addString(name_alt.value());
        }
        else {
            encoder.addNull();
        }
    }
    response: {
        encoder.addUInt(::worker_result);
        encoder.addArrayHeader(topic_bodies.size());

        for (auto& [topic, payload]: topic_bodies) {
            encoder.addBinaryPair(std::string(topic), payload.rawBuffer());
        }
    }
    encoder.flush();
}

auto encodeWorkerLog(
    CborEncoder<NngBackend>& encoder, 
    const std::string_view& stage,
    const std::string_view& worker_phase, 
    const SourceDescriptor& desc,
    const size_t offset, 
    LogLevel log_level, 
    const std::string& message) -> void 
{
    encodeResponseHeader(encoder, desc.log_event_tag, stage);

    log_level: {
        encoder.addUInt(static_cast<uint64_t>(log_level));
    }
    message: {
        encoder.addString(std::format("message: {}, name: {}, offset: {}, phase: {}", message, std::string_view{desc.name.ptr, desc.name.len}, offset, worker_phase));
    }

    encoder.flush();
}

}