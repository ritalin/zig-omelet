#include <duckdb.hpp>

#include <duckdb/planner/expression/list.hpp>
#include <duckdb/common/extra_type_info.hpp>
#include <duckdb/common/types/vector.hpp>

#include "duckdb_binder_support.hpp"

namespace worker {

auto pickEnumUserType(const duckdb::LogicalType& ty, const std::string& type_name) -> UserTypeEntry {
    auto *ext_info = ty.AuxInfo();

    std::vector<UserTypeEntry::Member> fields;
    if (ext_info->type == duckdb::ExtraTypeInfoType::ENUM_TYPE_INFO) {
        auto& enum_ext_info = ext_info->Cast<duckdb::EnumTypeInfo>();
        auto values = duckdb::FlatVector::GetData<duckdb::string_t>(enum_ext_info.GetValuesInsertOrder());
        auto size = enum_ext_info.GetDictSize();;
        
        for (auto iter = values; iter != values + size; ++iter) {
            fields.push_back({.field_name = iter->GetString()});
        }
    }

    return {
        .kind = UserTypeKind::Enum,
        .name = type_name,
        .fields = fields,
    };
}

static auto userTypeKindAsText(UserTypeKind kind) -> std::string {
    switch (kind) {
    case UserTypeKind::Enum: return std::move(std::to_string('enum'));
    }
}

auto encodeUserType(CborEncoder& encoder, const UserTypeEntry& entry) -> void {
    encoder.addArrayHeader(2);
    type_header: {
        type_kind: {
            encoder.addString(userTypeKindAsText(entry.kind));
        }
        type_name: {
            encoder.addString(entry.name);
        }
    }
    type_bodies: {
        encoder.addArrayHeader(entry.fields.size());
        for (auto& field: entry.fields) {
            encoder.addArrayHeader(2);
            encoder.addString(field.field_name);
            if (field.field_type) {
                encoder.addString(field.field_type.value());
            }
            else {
                encoder.addNull();
            }
        }
    }
}
}