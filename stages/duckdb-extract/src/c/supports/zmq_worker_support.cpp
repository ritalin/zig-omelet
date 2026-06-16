#include <iostream>

#include <magic_enum/magic_enum.hpp>

#include "duckdb_worker.h"
#include "zmq_worker_support.hpp"
#include "cbor_encode.hpp"
#include "response_encode_support.hpp"
#include "omelet_c_types.h"

namespace worker {

NngChannel::NngChannel(const SourceDescriptor& desc, const std::optional<size_t>& offset, std::string&& worker_phase): 
    desc(desc),
    stmt_offset(offset), 
    worker_phase(worker_phase), 
    messages({})
{
}

NngChannel::~NngChannel() {
    for (auto* raw_msg: this->messages) {
        nng_msg_free(raw_msg);
    }
}

static const std::string_view TETS_SOURCE = "test";
static const std::string_view TEST_DIALECT = "duckdb";
static const std::string_view TETS_HASH = "deadbeef";
static const SourceDescriptor TEST_DESC = {
    .response_event_tag = 0,
    .log_event_tag = 0,
    .name = { .ptr = TETS_SOURCE.data(), .len = TETS_SOURCE.size() },
    .dialect = { .ptr = TEST_DIALECT.data(), .len = TEST_DIALECT.size() },
    .hash = { .ptr = TETS_HASH.data(), .len = TETS_HASH.size() },
};

auto NngChannel::unitTestChannel() -> NngChannel {
    return NngChannel(TEST_DESC, std::nullopt, "unittest");
}

auto NngChannel::info(const std::string& message) -> void {
    NngBackend backend;
    auto encoder = CborEncoder(backend);
    encodeWorkerLog(encoder, this->worker_phase, this->desc, this->stmt_offset.value_or(0), LogLevel::info, message);

    this->messages.emplace_back(std::move(backend.release()));
}

auto NngChannel::warn(const std::string& message) -> void {
    NngBackend backend;
    auto encoder = CborEncoder(backend);
    encodeWorkerLog(encoder, this->worker_phase, this->desc, this->stmt_offset.value_or(0), LogLevel::warn, message);

    this->messages.emplace_back(std::move(backend.release()));
}

auto NngChannel::err(const std::string& message) -> void {
    NngBackend backend;
    auto encoder = CborEncoder(backend);
    encodeWorkerLog(encoder, this->worker_phase, this->desc, this->stmt_offset.value_or(0), LogLevel::err, message);

    this->messages.emplace_back(std::move(backend.release()));
}

auto NngChannel::makeWorkerResponse(std::function<void(CborEncoder<NngBackend>&, const std::string&, const SourceDescriptor&, size_t)> callback) -> void {
    NngBackend backend;
    auto encoder = CborEncoder(backend);

    callback(encoder, this->worker_phase, this->desc, this->stmt_offset.value_or(0));

    this->messages.emplace_back(std::move(backend.release()));
}

auto NngChannel::collectInto(std::vector<nng_msg*>& messages) -> void {
    messages.reserve(messages.size() + this->messages.size());

    messages.insert(
        messages.end(),
        std::make_move_iterator(this->messages.begin()),
        std::make_move_iterator(this->messages.end())
    );
    this->messages.clear();
}

}