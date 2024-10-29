#include <iostream>
#include <zmq.h>

#include <magic_enum/magic_enum.hpp>

#include "duckdb_worker.h"
#include "zmq_worker_support.hpp"
#include "cbor_encode.hpp"
#include "response_encode_support.hpp"
#include "omelet_c_types.h"

namespace worker {

ZmqChannel::ZmqChannel(std::optional<void *> socket, const std::optional<size_t>& offset, const std::string& id, const std::string& from)
    : socket(socket), stmt_offset(offset), id(id), from(from)
{

}

static auto sendInternal(void *socket, const CWorkerResponseTag event_tag, const std::string& id, const std::string& from, const std::vector<char>& content) -> void;

auto ZmqChannel::unitTestChannel() -> ZmqChannel {
    return ZmqChannel(std::nullopt, std::nullopt, "", "unittest");
}

auto ZmqChannel::clone() -> ZmqChannel {
    return ZmqChannel(this->socket, this->stmt_offset, this->id, this->from);
}

auto ZmqChannel::info(const std::string& message) -> void {
    if (! this->socket) {
        std::cout << std::format("log/level: info, message: {}, offset: {}", message, this->stmt_offset.value_or(0)) << std::endl;
    }
    else {
        sendInternal(this->socket.value(), ::worker_log, this->id, this->from, encodeWorkerLog(LogLevel::info, this->id, this->stmt_offset.value_or(0), message));
    }
}

auto ZmqChannel::warn(const std::string& message) -> void {
    if (! this->socket) {
        std::cout << std::format("log/level: warn, message: {}, offset: {}", message, this->stmt_offset.value_or(0)) << std::endl;
    }
    else {
        sendInternal(this->socket.value(), ::worker_log, this->id, this->from, encodeWorkerLog(LogLevel::warn, this->id, this->stmt_offset.value_or(0), message));
    }
}

auto ZmqChannel::err(const std::string& message) -> void {
    if (! this->socket) {
        std::cout << std::format("log/level: err, message: {}, offset: {}", message, this->stmt_offset.value_or(0)) << std::endl;
    }
    else {
        sendInternal(this->socket.value(), ::worker_log, this->id, this->from, encodeWorkerLog(LogLevel::err, this->id, this->stmt_offset.value_or(0), message));
    }
}

auto ZmqChannel::sendWorkerResponse(CWorkerResponseTag event_tag, std::vector<char>&& content) -> void {
    if (! this->socket) {
        std::cout << std::format("worker_result/payload: `{}`", std::string(content.begin(), content.end())) << std::endl;
    }
    else {
        sendInternal(this->socket.value(), event_tag, this->id, this->from, content);
    }
}

static auto sendInternal(void *socket, const CWorkerResponseTag event_tag, const std::string& id, const std::string& from, const std::vector<char>& content) -> void {
    event_type: {
        auto event_type = std::string("worker_response");
        ::zmq_send(socket, event_type.data(), event_type.length(), ZMQ_SNDMORE);    
    }
    packet_kind: {
        ::zmq_send(socket, &::CPostPacketKind, 1, ZMQ_SNDMORE);    
    }
    from: {
        ::zmq_send(socket, from.data(), from.length(), ZMQ_SNDMORE);    
    }
    payload: {
        CborEncoder payload_encoder;

        event_tag: {
            payload_encoder.addString(magic_enum::enum_name(event_tag));
        }
        work_id: {
            payload_encoder.addString(id);
        }
        content: {
            payload_encoder.concatBinary(content);
        }

        auto encode_result = payload_encoder.build();
        ::zmq_send(socket, encode_result.data(), encode_result.size(), ZMQ_SNDMORE);    
        ::zmq_send(socket, "", 0, 0);    
    }
}

}