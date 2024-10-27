#include "omelet_c_types.h"

namespace worker {

enum class LogLevel {
    err = ::log_level_err,
    warn = ::log_level_warn,
    info = ::log_level_info,
    debug = ::log_level_debug,
    trace = ::log_level_trace,
};

auto encodeStatementCount(const size_t count) -> std::vector<char>;
auto encodeStatementOffset(size_t offset) -> std::vector<char>;
auto encodeTopicBody(const size_t offset , const std::unordered_map<std::string, std::vector<char>>& topic_bodies) -> std::vector<char>;
auto encodeWorkerLog(LogLevel log_level, const std::string& id, const size_t offset, const std::string message) -> std::vector<char>;

}