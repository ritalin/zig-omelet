#pragma once

#include <duckdb.hpp>

namespace worker {
    
class NullableLookup {
public:
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
    struct Nullability {
        bool from_field;
        bool from_join;
    public:
        auto shouldNulls() -> bool { return this->from_join || this->from_field; }
    };
public:
    NullableLookup() {}

    template<std::input_iterator InputIterator>
    requires std::same_as<std::iter_value_t<InputIterator>, std::pair<Column, Nullability>>
    NullableLookup(InputIterator begin, InputIterator end): entries(begin, end) {}
public:
    auto operator[] (const duckdb::ColumnBinding& binding) -> Nullability& {
        return (*this)[Column{.table_index = binding.table_index, .column_index = binding.column_index}];
    }
    auto operator[] (const Column& binding) -> Nullability& {
        return this->entries[binding];
    }
    auto operator[] (const duckdb::ColumnBinding& binding) const -> Nullability {
        auto key = Column{.table_index = binding.table_index, .column_index = binding.column_index};
        return (*this)[key];
    }
    auto operator[] (const Column& binding) const -> Nullability {
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
    requires std::same_as<std::iter_value_t<InputIterator>, std::pair<const Column, Nullability>>
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
    std::map<Column, Nullability> entries;
};

}