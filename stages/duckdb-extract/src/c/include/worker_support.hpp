#pragma once

#include <optional>
#include <string>

#include "duckdb_worker.h"
#include "cbor_encode.hpp"

namespace worker {

class NngChannel {
public:
    NngChannel(const SourceDescriptor& desc, const std::optional<size_t>& offset, std::string_view stage, std::string_view worker_phase);
    ~NngChannel();
public:
    static auto unitTestChannel() -> NngChannel;
public:
    auto makeWorkerResponse(std::function<void(CborEncoder<NngBackend>&, const std::string_view&, const SourceDescriptor&, size_t)> callback) -> void;
public:
    auto info(const std::string& message) -> void;
    auto warn(const std::string& message) -> void;
    auto err(const std::string& message) -> void;
public:
    auto collectInto(std::vector<nng_msg*>& messages) -> void;
private:
    const SourceDescriptor& desc;
    const std::optional<size_t> stmt_offset;
    const std::string_view stage;
    const std::string_view worker_phase;
    std::vector<nng_msg*> messages;
};

}