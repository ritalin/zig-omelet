#pragma once

#include <optional>
#include <string>

#include "duckdb_worker.h"

namespace worker {

class ZmqChannel {
public:
    ZmqChannel(std::optional<void *> socket, const std::optional<size_t>& offset, const std::string& id, const std::string& from);
public:
    static auto unitTestChannel() -> ZmqChannel;
    auto clone() -> ZmqChannel;
public:
    auto sendWorkerResponse(CWorkerResponseTag event_tag, std::vector<char>&& content) -> void;
public:
    auto info(const std::string& message) -> void;
    auto warn(const std::string& message) -> void;
    auto err(const std::string& message) -> void;
private:
    std::optional<void *> socket;
    std::optional<size_t> stmt_offset;
    std::string id;
    std::string from;
};

}