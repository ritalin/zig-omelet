#pragma once

#include <duckdb.hpp>

namespace worker {

struct Column {
    duckdb::idx_t table_index;
    duckdb::idx_t column_index;

    auto operator<(const Column& other) const -> bool {
        return std::tie(this->table_index, this->column_index) < std::tie(other.table_index, other.column_index);
    }
    auto operator==(const Column& other) const -> bool {
        return std::tie(this->table_index, this->column_index) == std::tie(other.table_index, other.column_index);
    }
public:
    static auto from(const duckdb::ColumnBinding binding) -> Column {
        return {
            .table_index = binding.table_index,
            .column_index = binding.column_index,
        };
    }
};

template<typename T>
class GenericNullableLookup {
public:
    using Item = T;
    using Column = Column;
public:
    GenericNullableLookup() {}

    template<std::input_iterator InputIterator>
    requires std::same_as<std::iter_value_t<InputIterator>, std::pair<Column, Item>>
    GenericNullableLookup(InputIterator begin, InputIterator end): entries(begin, end) {}
public:
    auto operator[] (const duckdb::ColumnBinding& binding) -> Item& {
        return (*this)[Column{.table_index = binding.table_index, .column_index = binding.column_index}];
    }
    auto operator[] (const Column& binding) -> Item& {
        return this->entries[binding];
    }
    auto operator[] (const duckdb::ColumnBinding& binding) const -> Item {
        auto key = Column{.table_index = binding.table_index, .column_index = binding.column_index};
        return (*this)[key];
    }
    auto operator[] (const Column& binding) const -> Item {
        auto it = this->entries.find(binding);

        if (it != this->entries.end()) {
            return it->second;
        } 
        else {
            return {};
        }
    }
public:
    template<std::input_iterator InputIterator>
    requires std::same_as<std::iter_value_t<InputIterator>, std::pair<const Column, Item>>
    auto insert(InputIterator begin, InputIterator end) -> void {
        for (auto it = begin; it != end; ++it) {
            this->entries.insert(*it);
        }
    }
    auto insert(const Column& column, Item& item) -> void { 
        this->entries.insert(std::pair<Column, Item>(column, item)); 
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
    std::map<Column, Item> entries;
};

}