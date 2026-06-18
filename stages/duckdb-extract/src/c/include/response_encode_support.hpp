#include "omelet_c_types.h"
#include "cbor_encode.hpp"
#include "duckdb_worker.h"

namespace worker {

enum class LogLevel {
    err = ::log_level_err,
    warn = ::log_level_warn,
    info = ::log_level_info,
    debug = ::log_level_debug,
    trace = ::log_level_trace,
};

auto encodeStatementOffset(
    CborEncoder<NngBackend>& encoder, 
    const std::string_view& stage, 
    const SourceDescriptor& desc, 
    size_t offset) -> void;

auto encodeTopicBody(
    CborEncoder<NngBackend>& encoder, 
    const std::string_view& stage, 
    const SourceDescriptor& desc, 
    const size_t offset, 
    std::optional<std::string> name_alt, 
    const std::unordered_map<std::string_view, CborEncoder<VectorBackend>>& topic_bodies) -> void;

auto encodeWorkerLog(
    CborEncoder<NngBackend>& encoder, 
    const std::string_view& stage, 
    const std::string_view& worker_phase, 
    const SourceDescriptor& desc,
    const size_t offset, 
    LogLevel log_level, 
    const std::string& message) -> void;

}