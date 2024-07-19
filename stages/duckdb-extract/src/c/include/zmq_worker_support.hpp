#pragma once

#include <optional>
#include <string>

namespace worker {

class ZmqChannel {
public:
    ZmqChannel(std::optional<void *> socket, const std::string& id, const std::string& from);
public:
    static auto unitTestChannel() -> ZmqChannel;
public:
    auto sendWorkerResult(size_t stmt_offset, size_t stmt_count, std::vector<char> payload) -> void;
public:
    auto warn(std::string message) -> void;
    auto err(std::string message) -> void;
private:
    std::optional<void *> socket;
    std::string id;
    std::string from;
};

}