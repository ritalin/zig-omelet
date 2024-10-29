#pragma once

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    log_level_err,
    log_level_warn,
    log_level_info,
    log_level_debug,
    log_level_trace,
} CLogLevel;

typedef enum {
    Enum = 1, 
    Struct, 
    Array, 
    Primitive, 
    User,
    Alias, 
} CUserTypeKind;

const unsigned char CPostPacketKind = 1;

#ifdef __cplusplus
}
#endif
