#include <duckdb.hpp>

#include <duckdb/planner/expression/list.hpp>
#include <duckdb/common/extra_type_info.hpp>
#include <duckdb/common/types/vector.hpp>

#include <magic_enum/magic_enum.hpp>

#include "user_type_support.hpp"

namespace worker {

auto isEnumUserType(const duckdb::LogicalType &ty) -> bool {
    return ty.id() == duckdb::LogicalTypeId::ENUM;
}

auto isStructUserType(const duckdb::LogicalType &ty) -> bool {
    return ty.id() == duckdb::LogicalTypeId::STRUCT;
}

auto isArrayUserType(const duckdb::LogicalType &ty) -> bool {
    return (
        (ty.id() == duckdb::LogicalTypeId::LIST)
        || (ty.id() == duckdb::LogicalTypeId::ARRAY)
    );
}

auto isAliasUserType(const duckdb::LogicalType &ty) -> bool {
    auto *ext_info = ty.AuxInfo();
    if (!ext_info) return false;

    return ext_info->type == duckdb::ExtraTypeInfoType::GENERIC_TYPE_INFO;
}

auto isPredefinedUserType(const duckdb::LogicalType &ty) -> bool {
    return ty.id() == duckdb::LogicalTypeId::USER;
}

auto userTypeName(const duckdb::LogicalType& ty) -> std::string {
    return ty.GetAlias();
}

auto userTypeNameAnonymous(UserTypeKind type_kind, size_t index) -> std::string {
    return std::format("Anon::{}#{}", magic_enum::enum_name(type_kind), index);
}

auto resolveNameAnonymous(const std::optional<std::string>& field_name_opt, UserTypeKind type_kind, std::ranges::iterator_t<AnonymousCounter>& index) -> std::string {
    if (field_name_opt) return field_name_opt.value();

    return userTypeNameAnonymous(type_kind, *index++);
}

auto pickEnumUserType(const duckdb::LogicalType& ty, const std::string& type_name) -> UserTypeEntry {
    auto *ext_info = ty.AuxInfo();

    std::vector<UserTypeEntry::Member> fields;
    if (ext_info->type == duckdb::ExtraTypeInfoType::ENUM_TYPE_INFO) {
        auto& enum_ext_info = ext_info->Cast<duckdb::EnumTypeInfo>();
        auto values = duckdb::FlatVector::GetData<duckdb::string_t>(enum_ext_info.GetValuesInsertOrder());
        auto size = enum_ext_info.GetDictSize();;
        
        for (auto iter = values; iter != values + size; ++iter) {
            fields.push_back(UserTypeEntry::Member(iter->GetString()));
        }
    }

    return {
        .kind = UserTypeKind::Enum,
        .name = type_name,
        .fields = std::move(fields),
    };
}

// static auto pickAnonymousUserTypeMember(const duckdb::LogicalType &ty, std::vector<std::string>& user_type_names, size_t index) -> UserTypeEntry::Member;
static auto pickNestedUserTypeMember(const std::optional<std::string> field_name_opt, const duckdb::LogicalType &ty, std::vector<std::string>& user_type_names, std::vector<UserTypeEntry>& nested_anon_types, std::ranges::iterator_t<AnonymousCounter>& index) -> UserTypeEntry::Member;

auto pickStructUserType(const duckdb::LogicalType& ty, const std::string& type_name, std::vector<std::string>& nested_user_types, std::vector<UserTypeEntry>& nested_anon_types, std::ranges::iterator_t<AnonymousCounter>& index) -> UserTypeEntry {
    auto *ext_info = ty.AuxInfo();

    std::vector<UserTypeEntry::Member> fields;
    if (ext_info->type == duckdb::ExtraTypeInfoType::STRUCT_TYPE_INFO) {
        auto& struct_ext_info = ext_info->Cast<duckdb::StructTypeInfo>();

        for (auto& [field_name, child_type]: struct_ext_info.child_types) {
            fields.push_back(pickNestedUserTypeMember(field_name, child_type, nested_user_types, nested_anon_types, index));
        }
    }

    return {
        .kind = UserTypeKind::Struct,
        .name = type_name,
        .fields = std::move(fields),
    };
}

static auto pickNestedUserTypeMember(
    const std::optional<std::string> field_name_opt, const duckdb::LogicalType &ty, 
    std::vector<std::string>& user_type_names, 
    std::vector<UserTypeEntry>& nested_anon_types, 
    std::ranges::iterator_t<AnonymousCounter>& index) -> UserTypeEntry::Member 
{
    auto member_type_name = userTypeName(ty);
    std::string member_field_name;

    if (member_type_name != "") {
        // when predefined type (except for create type)
        member_field_name = resolveNameAnonymous(field_name_opt, UserTypeKind::User, index);

        user_type_names.push_back(member_type_name);

        return UserTypeEntry::Member(
            member_field_name,
            std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::User, .name = member_type_name, .fields = {}})
        );   
    }

    if (isEnumUserType(ty)) {
        member_type_name = userTypeNameAnonymous(UserTypeKind::Enum, *index++);
        member_field_name = field_name_opt.value_or(std::string(member_type_name));
        auto member_type = pickEnumUserType(ty, member_type_name);

        nested_anon_types.push_back(member_type);

        return UserTypeEntry::Member(
            member_field_name,
            std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = member_type.kind, .name = member_type.name, .fields = {}})
        );
    }
    if (isArrayUserType(ty)) {
        auto member_name = userTypeNameAnonymous(UserTypeKind::Array, *index++);
        auto member_type = pickArrayUserType(ty, member_name, user_type_names, nested_anon_types, index);

        nested_anon_types.push_back(member_type);

        return UserTypeEntry::Member(
            field_name_opt.value_or(std::string(member_type.name)),
            std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = member_type.kind, .name = member_type.name, .fields = {}})
        );
    }
    if (isStructUserType(ty)) {
        auto member_name = userTypeNameAnonymous(UserTypeKind::Struct, *index++);
        auto member_type = pickStructUserType(ty, member_name, user_type_names, nested_anon_types, index);

        nested_anon_types.push_back(member_type);

        return UserTypeEntry::Member(
            field_name_opt.value_or(std::string(member_type.name)),
            std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = member_type.kind, .name = member_type.name, .fields = {}})
        );
    }
    else if (isPredefinedUserType(ty)) {
        // when create member of list/struct
        auto& info = ty.AuxInfo()->Cast<duckdb::UserTypeInfo>();

        user_type_names.push_back(info.user_type_name);

        return UserTypeEntry::Member(
            resolveNameAnonymous(field_name_opt, UserTypeKind::User, index),
            std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::User, .name = info.user_type_name, .fields = {}})
        );
    }
    else {
        return UserTypeEntry::Member(
            resolveNameAnonymous(field_name_opt, UserTypeKind::Primitive, index),
            std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = ty.ToString(), .fields = {}})
        );
    }
}

auto pickArrayUserType(
    const duckdb::LogicalType &ty, const std::string& type_name, 
    std::vector<std::string>& user_type_names, 
    std::vector<UserTypeEntry>& anon_types,
    std::ranges::iterator_t<AnonymousCounter>& index) -> UserTypeEntry {
    auto *ext_info = ty.AuxInfo();
        
    UserTypeEntry::Member field;

    if (ext_info->type == duckdb::ExtraTypeInfoType::LIST_TYPE_INFO) {
        auto& member_ext_info = ext_info->Cast<duckdb::ListTypeInfo>();
        field = pickNestedUserTypeMember(std::nullopt, member_ext_info.child_type, user_type_names, anon_types, index);
    }
    else if (ext_info->type == duckdb::ExtraTypeInfoType::ARRAY_TYPE_INFO) {
        auto& member_ext_info = ext_info->Cast<duckdb::ArrayTypeInfo>();
        field = pickNestedUserTypeMember(std::nullopt, member_ext_info.child_type, user_type_names, anon_types, index);
    }

    if (field.field_type) {
        const std::unordered_set<UserTypeKind> anon_field_types{UserTypeKind::Enum, UserTypeKind::Struct, UserTypeKind::Array};
        if (anon_field_types.contains(field.field_type->kind)) {
            field.field_type = nullptr;
        }
    }

    return {
        .kind = UserTypeKind::Array,
        .name = type_name,
        .fields{std::move(field)},
    };
}

auto pickAliasUserType(
    const duckdb::LogicalType &ty, const std::string& type_name, 
    std::vector<std::string>& user_type_names, 
    std::ranges::iterator_t<AnonymousCounter>& index) -> UserTypeEntry 
{
    std::vector<UserTypeEntry> anon_types{};

    std::vector<UserTypeEntry::Member> fields{
        pickNestedUserTypeMember(std::nullopt, ty, user_type_names, anon_types, index)
    };

    return {
        .kind = UserTypeKind::Alias,
        .name = type_name,
        .fields = std::move(fields),
    };
}

auto userTypeKindAsText(UserTypeKind kind) -> std::string {
    switch (kind) {
    case UserTypeKind::Enum: 
        return std::string("enum");
    case UserTypeKind::Struct: 
        return std::string("struct");
    case UserTypeKind::Array: 
        return std::string("array");
    case UserTypeKind::Alias: 
        return std::string("alias");
    case UserTypeKind::Primitive: 
        return std::string("primitive");
    case UserTypeKind::User: 
        return std::string("user");
    default:
        return std::string("unknown");
    }
}

auto encodeUserType(CborEncoder& encoder, const UserTypeEntry& entry) -> void {
    encoder.addArrayHeader(2);
    type_header: {
        type_kind: {
            encoder.addUInt(static_cast<uint64_t>(entry.kind));
        }
        type_name: {
            encoder.addString(entry.name);
        }
    }
    type_bodies: {
        encoder.addArrayHeader(entry.fields.size());
        for (auto& field: entry.fields) {
            encoder.addArrayHeader(2);
            field_name: {
                encoder.addString(field.field_name);
            }
            field_type: {
                if (field.field_type) {
                    encodeUserType(encoder, *field.field_type);
                }
                else {
                    encoder.addNull();
                }
            }
        }
    }
}

}