#include <string_view>
#include <span>
#include <string>
#include <vector>
#include <cstring>
#include <cassert>

#include "cbor_encode.hpp"
#include "cbor/encoder.h"

struct CborTypes {
    static const uint64_t PINTEGER = 0;
    static const uint64_t NINTEGER = 1;
    static const uint64_t BITES = 2;
    static const uint64_t STRING = 3;
    static const uint64_t ARRAY = 4;
    static const uint64_t MAP = 5;
};

const size_t MAX_BUFFER_SIZE = 9;

template <WriterBackend Backend>
static auto cborHeader(Backend& backend, uint32_t id, size_t len) {
    cbor_writer_t writer;
    char buf[MAX_BUFFER_SIZE] = {}; 
    ::cbor_writer_init(&writer, buf, MAX_BUFFER_SIZE);   

    ::cbor_encode_unsigned_integer(&writer, len);

    writer.buf[0] |= ((id & 0b0111) << 5);

    backend.write(buf, writer.bufidx);
}

template <WriterBackend Backend>
static auto encodeUInt(Backend& backend, uint64_t value) -> void {
    cbor_writer_t writer;
    std::byte buf[MAX_BUFFER_SIZE] = {};

    ::cbor_writer_init(&writer, buf, MAX_BUFFER_SIZE);
    ::cbor_encode_unsigned_integer(&writer, value);

    backend.write(buf, writer.bufidx);
}

template <WriterBackend Backend>
static auto encodeBool(Backend& backend, bool value) -> void {
    cbor_writer_t writer;
    std::byte buf[1] = {}; 

    ::cbor_writer_init(&writer, buf, 1);
    ::cbor_encode_bool(&writer, value);

    backend.write(buf, writer.bufidx);
}

template <WriterBackend Backend>
static auto encodeNull(Backend& backend) -> void {
    cbor_writer_t writer;
    char buf[] = {0};

    ::cbor_writer_init(&writer, buf, 1);
    ::cbor_encode_null(&writer);

    backend.write(buf, writer.bufidx);
}

/// --------------------------------------------------------------------------------
/// CborEncoder
/// --------------------------------------------------------------------------------

template <WriterBackend Backend>
auto CborEncoder<Backend>::addUInt(uint64_t value) -> void {
    ::encodeUInt(this->backend, value);
}

template <WriterBackend Backend>
auto CborEncoder<Backend>::addString(std::string_view value) -> void {
    cborHeader(this->backend, CborTypes::STRING, value.size());
    this->backend.write(value.data(), value.size());
}

template <WriterBackend Backend>
auto CborEncoder<Backend>::addBool(bool value) -> void {
    encodeBool(this->backend, value);
}

template <WriterBackend Backend>
auto CborEncoder<Backend>::addNull() -> void {
    encodeNull(this->backend);
}

template <WriterBackend Backend>
auto CborEncoder<Backend>::addArrayHeader(size_t len) -> void {
    cborHeader(this->backend, CborTypes::ARRAY, len);
}

template <WriterBackend Backend>
auto CborEncoder<Backend>::addAggregateSlice(std::vector<CborEncoder<VectorBackend>>&& encoders) -> void {
    // TODO:
    // std::copy(buffer.begin(), buffer.end(), std::back_inserter(this->buf));

    
    this->addArrayHeader(encoders.size());
    for (auto& e: encoders) {
        auto source = e.rawBuffer();
        this->backend.write(source.data(), source.size());
    }
}

template <WriterBackend Backend>
auto CborEncoder<Backend>::addStringPair(const std::string& key, const std::string& value) -> void {
    tuple: {
        cborHeader(this->backend, CborTypes::ARRAY, 2);
    }
    key: {
        cborHeader(this->backend, CborTypes::STRING, key.size());
        this->backend.write(key.data(), key.size());
    }
    value: {
        cborHeader(this->backend, CborTypes::STRING, value.size());
        this->backend.write(value.data(), value.size());
    }
}

template <WriterBackend Backend>
auto CborEncoder<Backend>::addBinaryPair(const std::string& key, const std::span<const std::byte>& value) -> void {
    tuple: {
        cborHeader(this->backend, CborTypes::ARRAY, 2);
    }
    key: {
        cborHeader(this->backend, CborTypes::STRING, key.size());
        this->backend.write(key.data(), key.size());
    }
    value: {
        cborHeader(this->backend, CborTypes::BITES, value.size());
        this->backend.write(value.data(), value.size());
    }
}

template <WriterBackend Backend>
auto CborEncoder<Backend>::addUIntPair(const std::string& key, uint64_t value) -> void {
    tuple: {
        cborHeader(this->backend, CborTypes::ARRAY, 2);
    }
    key: {
        cborHeader(this->backend, CborTypes::STRING, key.size());
        this->backend.write(key.data(), key.size());
    }
    value: {
        encodeUInt(this->backend, value);
    }
}

template <WriterBackend Backend>
auto CborEncoder<Backend>::rawBuffer() const -> std::span<const std::byte> {
    return this->backend.rawBuffer();
}

template <WriterBackend Backend>
auto CborEncoder<Backend>::flush() -> void {
    this->backend.flush();
}

/// --------------------------------------------------------------------------------
/// VectorBackend
/// --------------------------------------------------------------------------------

auto VectorBackend::write(const void* ptr, size_t len) -> void {
    auto begin = static_cast<const std::byte*>(ptr);

    this->buf.insert(this->buf.end(), begin, begin + len);
}

auto VectorBackend::rawBuffer() const -> std::span<const std::byte> {
    return {
        this->buf.data(),
        this->buf.size()
    };
}

auto VectorBackend::flush() -> void {
    // no-op
}

/// --------------------------------------------------------------------------------
/// NngBackend
/// --------------------------------------------------------------------------------

NngBackend::NngBackend(size_t capacity) {
    auto err = nng_msg_alloc(&this->msg, capacity);
    assert(err == 0);

    auto p = nng_msg_body(this->msg);

    this->buffer = { static_cast<std::byte*>(p), capacity };
    this->end = 0;
}

NngBackend::NngBackend(NngBackend&& other) noexcept 
    : msg(other.msg), buffer(other.buffer), end(other.end)
{
    other.msg = nullptr;
    other.buffer = {};
    other.end = 0;
    assert(this->end <= this->buffer.size());
}

NngBackend& NngBackend::operator=(NngBackend&& other) noexcept {
    if (this != &other) {
        if (this->msg) {
            nng_msg_free(this->msg);
        }
        this->msg = other.msg;
        this->buffer = other.buffer;
        this->end = other.end;
        
        other.msg = nullptr;
        other.buffer = {};
        other.end = 0;
    }

    return *this;
}

NngBackend::~NngBackend() {
    if (this->msg) {
        nng_msg_free(this->msg);
    }
}

auto NngBackend::release() -> nng_msg* {
    auto msg = this->msg;
    this->msg = nullptr;
    this->buffer = {};
    this->end = 0;

    return msg;
}

auto NngBackend::write(const void* ptr, size_t len) -> void {
    auto required = this->end + len;

    if (this->buffer.size() < required) {
        auto new_size = required << 1;
        auto err = nng_msg_realloc(this->msg, new_size);
        assert(err == 0);

        auto p = nng_msg_body(this->msg);
        this->buffer = { static_cast<std::byte*>(p), new_size };
    }

    std::memcpy(this->buffer.data() + this->end, ptr, len);
    this->end += len;
    assert(this->end <= this->buffer.size());
}

auto NngBackend::rawBuffer() const -> std::span<const std::byte> {
    return { this->buffer.data(), this->end };
}

auto NngBackend::flush() -> void {
    if (this->msg) {
        auto err = nng_msg_realloc(this->msg, this->end);
        assert(err == 0);
        
        auto p = nng_msg_body(this->msg);
        this->buffer = { static_cast<std::byte*>(p), this->end };
    }
}

#ifndef DISABLE_CATCH2_TEST

// -------------------------
// Unit tests
// -------------------------

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

using namespace Catch::Matchers;

template <std::integral... T>
auto as_bytes(T... v) -> std::vector<std::byte> {
    return { std::byte{static_cast<unsigned char>(v)}... };
}

auto as_sequence(std::uint8_t start, size_t len) -> std::vector<std::byte> {
    std::vector<std::byte> v;
    v.reserve(len);

    for (auto i = 0; i < len; ++i) {
        v.emplace_back(std::byte{static_cast<std::uint8_t>((i + start) % 256)});
    }

    return v;
}

auto to_vector(std::span<const std::byte> s) -> std::vector<std::byte> {
    return {s.begin(), s.end()};
}

TEST_CASE("Encode small uint to vector") {
    case_1: {
        auto encoder = CborEncoder(VectorBackend());
        encoder.addUInt(0);
        encoder.flush();

        auto actual = encoder.rawBuffer();
        auto expect = as_bytes(0x00);
        REQUIRE(to_vector(actual) == expect);
    }
    case_2: {
        auto encoder = CborEncoder(VectorBackend());
        encoder.addUInt(1);
        encoder.flush();

        auto actual = encoder.rawBuffer();
        auto expect = as_bytes(0x01);
        REQUIRE(to_vector(actual) == expect);
    }
    case_3: {
        auto encoder = CborEncoder(VectorBackend());
        encoder.addUInt(23);
        encoder.flush();

        auto actual = encoder.rawBuffer();
        auto expect = as_bytes(0x17);
        REQUIRE(to_vector(actual) == expect);
    }
}

TEST_CASE("Encode medium uint to vector") {
    case_1: {
        auto encoder = CborEncoder(VectorBackend());
        encoder.addUInt(24);
        encoder.flush();

        auto actual = encoder.rawBuffer();
        auto expect = as_bytes(0x18, 0x18);
        REQUIRE(to_vector(actual) == expect);
    }
    case_2: {
        auto encoder = CborEncoder(VectorBackend());
        encoder.addUInt(42);
        encoder.flush();

        auto actual = encoder.rawBuffer();
        auto expect = as_bytes(0x18, 0x2A);
        REQUIRE(to_vector(actual) == expect);
    }
    case_3: {
        auto encoder = CborEncoder(VectorBackend());
        encoder.addUInt(255);
        encoder.flush();

        auto actual = encoder.rawBuffer();
        auto expect = as_bytes(0x18, 0xFF);
        REQUIRE(to_vector(actual) == expect);
    }
}

TEST_CASE("Encode large uint to vector") {
    case_1: {
        auto encoder = CborEncoder(VectorBackend());
        encoder.addUInt(256);
        encoder.flush();

        auto actual = encoder.rawBuffer();
        auto expect = as_bytes(0x19, 0x01, 0x00);
        REQUIRE(to_vector(actual) == expect);
    }
    case_2: {
        auto encoder = CborEncoder(VectorBackend());
        encoder.addUInt(65535);
        encoder.flush();

        auto actual = encoder.rawBuffer();
        auto expect = as_bytes(0x19, 0xFF, 0xFF);
        REQUIRE(to_vector(actual) == expect);
    }
    case_3: {
        auto encoder = CborEncoder(VectorBackend());
        encoder.addUInt(65536);
        encoder.flush();

        auto actual = encoder.rawBuffer();
        auto expect = as_bytes(0x1A, 0x00, 0x01, 0x00, 0x00);
        REQUIRE(to_vector(actual) == expect);
    }
    case_4: {
        auto encoder = CborEncoder(VectorBackend());
        encoder.addUInt(1ULL << 32);
        encoder.flush();

        auto actual = encoder.rawBuffer();
        auto expect = as_bytes(0x1B, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00);
        REQUIRE(to_vector(actual) == expect);
    }
}

TEST_CASE("Encode string to vector") {
    case_1: {
        auto encoder = CborEncoder(VectorBackend());
        encoder.addString("");
        encoder.flush();

        auto actual = encoder.rawBuffer();
        auto expect = as_bytes(0x60);
        REQUIRE(to_vector(actual) == expect);
    }
    case_2: {
        auto encoder = CborEncoder(VectorBackend());
        encoder.addString("abc");
        encoder.flush();

        auto actual = encoder.rawBuffer();
        auto expect = as_bytes(0x63, 0x61, 0x62, 0x63);
        REQUIRE(to_vector(actual) == expect);
    }
}

TEST_CASE("Encode bool to vector") {
    case_1: {
        auto encoder = CborEncoder(VectorBackend());
        encoder.addBool(false);
        encoder.flush();

        auto actual = encoder.rawBuffer();
        auto expect = as_bytes(0xF4);
        REQUIRE(to_vector(actual) == expect);
    }
    case_2: {
        auto encoder = CborEncoder(VectorBackend());
        encoder.addBool(true);
        encoder.flush();

        auto actual = encoder.rawBuffer();
        auto expect = as_bytes(0xF5);
        REQUIRE(to_vector(actual) == expect);
    }
}

TEST_CASE("Encode null to vector") {
    case_1: {
        auto encoder = CborEncoder(VectorBackend());
        encoder.addNull();
        encoder.flush();

        auto actual = encoder.rawBuffer();
        auto expect = as_bytes(0xF6);
        REQUIRE(to_vector(actual) == expect);
    }
}

TEST_CASE("Encode pair uint to vector") {
    case_1: {
        auto encoder = CborEncoder(VectorBackend());
        encoder.addUIntPair("x", 42);
        encoder.flush();

        auto actual = encoder.rawBuffer();
        auto expect = as_bytes(0x82, 0x61, 0x78, 0x18, 0x2A);
        REQUIRE(to_vector(actual) == expect);
    }
}

TEST_CASE("Encode pair string to vector") {
    case_1: {
        auto encoder = CborEncoder(VectorBackend());
        encoder.addStringPair("y", "hello");
        encoder.flush();

        auto actual = encoder.rawBuffer();
        auto expect = as_bytes(0x82, 0x61, 0x79, 0x65, 0x68, 0x65, 0x6C, 0x6C, 0x6F);
        REQUIRE(to_vector(actual) == expect);
    }
}

TEST_CASE("Encode pair small binary to vector") {
    case_1: {
        auto encoder = CborEncoder(VectorBackend());
        encoder.addBinaryPair("z", std::span<std::byte>{});
        encoder.flush();

        auto actual = encoder.rawBuffer();
        auto expect = as_bytes(0x82, 0x61, 0x7A, 0x40);
        REQUIRE(to_vector(actual) == expect);
    }
    case_2: {
        auto encoder = CborEncoder(VectorBackend());
        encoder.addBinaryPair("z", as_bytes(0x00));
        encoder.flush();

        auto actual = encoder.rawBuffer();
        auto expect = as_bytes(0x82, 0x61, 0x7A, 0x41, 0x00);
        REQUIRE(to_vector(actual) == expect);
    }
    case_3: {
        auto encoder = CborEncoder(VectorBackend());
        encoder.addBinaryPair("z", as_bytes(0xDE, 0xAD, 0xBE, 0xEF));
        encoder.flush();

        auto actual = encoder.rawBuffer();
        auto expect = as_bytes(0x82, 0x61, 0x7A, 0x44, 0xDE, 0xAD, 0xBE, 0xEF);
        REQUIRE(to_vector(actual) == expect);
    }
    case_4: {
        auto encoder = CborEncoder(VectorBackend());
        encoder.addBinaryPair("z", as_bytes(0x00, 0xFF, 0x10, 0x7F));
        encoder.flush();

        auto actual = encoder.rawBuffer();
        auto expect = as_bytes(0x82, 0x61, 0x7A, 0x44, 0x00, 0xFF, 0x10, 0x7F);
        REQUIRE(to_vector(actual) == expect);
    }
    case_5: {
        auto encoder = CborEncoder(VectorBackend());
        encoder.addBinaryPair("z", as_bytes(0x00, 0x00, 0x00, 0x00));
        encoder.flush();

        auto actual = encoder.rawBuffer();
        auto expect = as_bytes(0x82, 0x61, 0x7A, 0x44, 0x00, 0x00, 0x00, 0x00);
        REQUIRE(to_vector(actual) == expect);
    }
}

TEST_CASE("Encode pair bonded binary to vector") {
    case_1: {
        auto encoder = CborEncoder(VectorBackend());
        encoder.addBinaryPair("z", as_sequence(0, 255));
        encoder.flush();

        auto actual = encoder.rawBuffer();
        auto expect_header = as_bytes(0x82, 0x61, 0x7A, 0x58, 0xFF);
        auto expect_sequence = as_sequence(0, 255);

        REQUIRE(actual.size() == expect_header.size() + expect_sequence.size());
        REQUIRE(actual.size() > expect_header.size());
        REQUIRE(std::equal(actual.begin(), actual.begin() + expect_header.size(), expect_header.begin(), expect_header.end()));
        REQUIRE(std::equal(actual.begin() + expect_header.size(), actual.end(), expect_sequence.begin(), expect_sequence.end()));
    }
    case_2: {
        auto encoder = CborEncoder(VectorBackend());
        encoder.addBinaryPair("z", as_sequence(0, 256));
        encoder.flush();

        auto actual = encoder.rawBuffer();
        auto expect_header = as_bytes(0x82, 0x61, 0x7A, 0x59, 0x01, 0x00);
        auto expect_sequence = as_sequence(0, 256);

        REQUIRE(actual.size() == expect_header.size() + expect_sequence.size());
        REQUIRE(actual.size() > expect_header.size());
        REQUIRE(std::equal(actual.begin(), actual.begin() + expect_header.size(), expect_header.begin(), expect_header.end()));
        REQUIRE(std::equal(actual.begin() + expect_header.size(), actual.end(), expect_sequence.begin(), expect_sequence.end()));
    }
}

TEST_CASE("Aggreate encoded batch to vector") {
    case_1: {
        auto encoder_a = CborEncoder(VectorBackend());
        encoder_a.addBinaryPair("z", as_bytes(0x01, 0x02));
        encoder_a.flush();
        auto actual_a = encoder_a.rawBuffer();
        auto expect_a = as_bytes(0x82, 0x61, 0x7A, 0x42, 0x01, 0x02);
        REQUIRE(to_vector(actual_a) == expect_a);

        auto encoder_b = CborEncoder(VectorBackend());
        encoder_b.addBinaryPair("z", as_bytes(0x03, 0x04));
        encoder_b.flush();
        auto actual_b = encoder_b.rawBuffer();
        auto expect_b = as_bytes(0x82, 0x61, 0x7A, 0x42, 0x03, 0x04);
        REQUIRE(to_vector(actual_b) == expect_b);

        auto encoder = CborEncoder(VectorBackend());
        encoder.addAggregateSlice({ encoder_a, encoder_b });
        encoder.flush();

        auto actual = encoder.rawBuffer();
        auto expect = as_bytes(0x82, 0x82, 0x61, 0x7A, 0x42, 0x01, 0x02, 0x82, 0x61, 0x7A, 0x42, 0x03, 0x04);
        REQUIRE(to_vector(actual) == expect);
    }
}

TEST_CASE("Write to nng backend") {
    case_1: {
        NngBackend backend(0);
        REQUIRE(backend.rawBuffer().size() == 0);
        backend.write("abc", 3);

        auto buffer = backend.rawBuffer();
        auto actual = std::string_view(reinterpret_cast<const char*>(buffer.data()), buffer.size());
        REQUIRE(buffer.size() == 3);
        REQUIRE(actual == "abc");
    }
}

TEST_CASE("Write to nng backend twice") {
    case_1: {
        NngBackend backend(0);
        REQUIRE(backend.rawBuffer().size() == 0);
        backend.write("abc", 3);
        backend.write("def", 3);

        auto buffer = backend.rawBuffer();
        auto actual = std::string_view(reinterpret_cast<const char*>(buffer.data()), buffer.size());
        REQUIRE(buffer.size() == 6);
        REQUIRE(actual == "abcdef");
    }
}

TEST_CASE("Release nng message") {
    case_1: {
        NngBackend backend(0);
        REQUIRE(backend.rawBuffer().size() == 0);
        backend.write("abc", 3);

        auto msg = backend.release();
        auto p = static_cast<const char*>(nng_msg_body(msg));
        REQUIRE(std::string_view(p, 3) == "abc");

        REQUIRE(backend.release() == nullptr);

        auto buffer = backend.rawBuffer();
        REQUIRE(buffer.size() == 0);
    }
}

TEST_CASE("Flush buffer for nng backend") {
    case_1: {
        NngBackend backend(0);
        REQUIRE(backend.rawBuffer().size() == 0);
        backend.write("abc", 3);
        backend.flush();

        auto msg = backend.release();
        auto actual_size = nng_msg_len(msg);
        REQUIRE(actual_size == 3);

        auto p = static_cast<const char*>(nng_msg_body(msg));
        REQUIRE(std::string_view(p, 3) == "abc");

        REQUIRE(backend.release() == nullptr);

        auto buffer = backend.rawBuffer();
        REQUIRE(buffer.size() == 0);
    }    
}

TEST_CASE("Write after flush buffer for nng backend") {
    case_1: {
        NngBackend backend(0);
        REQUIRE(backend.rawBuffer().size() == 0);
        backend.write("abc", 3);
        backend.flush();
        backend.write("def", 3);

        auto buffer = backend.rawBuffer();
        auto actual = std::string_view(reinterpret_cast<const char*>(buffer.data()), buffer.size());
        REQUIRE(buffer.size() == 6);
        REQUIRE(actual == "abcdef");
    }    
}

TEST_CASE("Encode for nng backend") {
    case_1: {
        NngBackend backend;
        auto encoder = CborEncoder(backend);

        encoder.addUInt(42);
        encoder.flush();

        auto msg = backend.release();
        auto actual_size = nng_msg_len(msg);
        REQUIRE(actual_size == 2);

        auto p = reinterpret_cast<const std::byte*>(nng_msg_body(msg));
        auto actual = std::span<const std::byte>{ p, actual_size };
        REQUIRE(to_vector(actual) == as_bytes(0x18, 0x2A));
    }
}

#endif
