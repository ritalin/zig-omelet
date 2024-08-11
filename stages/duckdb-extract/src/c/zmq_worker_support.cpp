#include <iostream>
#include <zmq.h>

#include "duckdb_worker.h"
#include "zmq_worker_support.hpp"
#include "cbor_encode.hpp"

namespace worker {

ZmqChannel::ZmqChannel(std::optional<void *> socket, const std::string& id, const std::string& from): socket(socket), id(id), from(from) {}

static auto sendWorkerLog(void *socket, const std::string& id, const std::string& from, const std::string& log_level, const std::string& message) -> void;

auto ZmqChannel::unitTestChannel() -> ZmqChannel {
    return ZmqChannel(std::nullopt, "", "unittest");
}

auto ZmqChannel::warn(std::string message) -> void {
    if (! this->socket) {
        std::cout << std::format("log/level: warn, message: {}", message) << std::endl;
        return;
    }
    else {
        sendWorkerLog(this->socket.value(), this->id, this->from, "warn", message);
    }
}

auto ZmqChannel::err(std::string message) -> void {
    if (! this->socket) {
        std::cout << std::format("log/level: err, message: {}", message) << std::endl;
        return;
    }
    else {
        sendWorkerLog(this->socket.value(), this->id, this->from, "err", message);
    }
}

auto ZmqChannel::sendWorkerResult(size_t stmt_offset, size_t stmt_count, const std::unordered_map<std::string, std::vector<char>>& topic_bodies) -> void {
    if (! this->socket) {
        for (auto [topic, payload]: topic_bodies) {
            std::cout << std::format("worker_result/topic: {}, payload: {}", topic, std::string(payload.begin(), payload.end())) << std::endl;
        }
        return;
    }

    auto socket = this->socket.value();

    event_type: {
        auto event_type = std::string("worker_result");
        ::zmq_send(socket, event_type.data(), event_type.length(), ZMQ_SNDMORE);    
    }
    from: {
        ::zmq_send(socket, this->from.data(), this->from.length(), ZMQ_SNDMORE);    
    }
    payload: {
        std::vector<char> buf;

        CborEncoder payload_encoder;
        result_tag: {
            payload_encoder.addString("topic_body");
        }
        work_id: {
            payload_encoder.addString(this->id);
        }
        stmt_count: {
            payload_encoder.addUInt(stmt_count);
        }
        stmt_offset: {
            payload_encoder.addUInt(stmt_offset);
        }
        topic_body: {
            payload_encoder.addUInt(topic_bodies.size());

            for (auto [topic, payload]: topic_bodies) {
                payload_encoder.addBinaryPair(topic, payload);
            }
        }

        auto encode_result = payload_encoder.build();

        ::zmq_send(socket, encode_result.data(), encode_result.size(), ZMQ_SNDMORE);
        ::zmq_send(socket, "", 0, 0);    
    }
}

static auto sendWorkerLog(void *socket, const std::string& id, const std::string& from, const std::string& log_level, const std::string& message) -> void {
    event_type: {
        auto event_type = std::string("worker_result");
        ::zmq_send(socket, event_type.data(), event_type.length(), ZMQ_SNDMORE);    
    }
    from: {
        ::zmq_send(socket, from.data(), from.length(), ZMQ_SNDMORE);    
    }
    payload: {
        CborEncoder payload_encoder;

        event_tag: {
            payload_encoder.addString("log");
        }
        source_path: {
            payload_encoder.addString(id);
        }
        log_level: {
            payload_encoder.addString(log_level);
        }
        log_content: {
            payload_encoder.addString(std::format("{} ({})", message, id));
        }

        auto encode_result = payload_encoder.build();
        ::zmq_send(socket, encode_result.data(), encode_result.size(), ZMQ_SNDMORE);    
        ::zmq_send(socket, "", 0, 0);    
    }
}

}