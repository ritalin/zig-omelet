#pragma once

#include <duckdb.hpp>

class NullableLookup {
public:
    struct Column {
        duckdb::idx_t table_index;
        duckdb::idx_t column_index;

        auto operator<(const Column& other) const -> bool {
            return std::tie(this->table_index, this->column_index) < std::tie(other.table_index, other.column_index);
        }
    };
public:
    NullableLookup() {}

    template<std::input_iterator InputIterator>
    requires std::same_as<std::iter_value_t<InputIterator>, std::pair<Column, bool>>
    NullableLookup(InputIterator begin, InputIterator end): entries(begin, end) {}
public:
    auto operator[] (const duckdb::ColumnBinding& binding) -> bool& {
        return (*this)[Column{.table_index = binding.table_index, .column_index = binding.column_index}];
    }
    auto operator[] (const Column& binding) -> bool& {
        return this->entries[binding];
    }
    auto operator[] (const duckdb::ColumnBinding& binding) const -> bool {
        auto key = Column{.table_index = binding.table_index, .column_index = binding.column_index};
        return (*this)[key];
    }
    auto operator[] (const Column& binding) const -> bool {
        auto it = this->entries.find(binding);

        if (it != this->entries.end()) {
            return it->second;
        } 
        else {
            return false;
        }
    }
public:
    template<std::input_iterator InputIterator>
    requires std::same_as<std::iter_value_t<InputIterator>, std::pair<const Column, bool>>
    auto insert(InputIterator begin, InputIterator end) -> void {
        for (auto it = begin; it != end; ++it) {
            this->entries.insert(*it);
        }
    }
    inline auto empty() -> bool { return this->entries.empty(); }
    inline auto size() -> size_t { return this->entries.size(); }
    inline auto contains(duckdb::idx_t table_index, duckdb::idx_t column_index) -> bool { 
        return this->entries.contains(Column{ .table_index = table_index, .column_index = column_index }); 
    }
public:
    // std::range requirement
    auto begin() { return this->entries.begin(); }
    auto end() { return this->entries.end(); }
    auto begin() const { return this->entries.begin(); }
    auto end() const { return this->entries.end(); }
private:
    std::map<Column, bool> entries;
};
