#pragma once

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    Enum = 1, 
    Struct, 
    Array, 
    Primitive, 
    User,
    Alias, 
} CUserTypeKind;

#ifdef __cplusplus
}
#endif
