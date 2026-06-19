#pragma once

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    log_level_err = 0,
    log_level_warn,
    log_level_info,
    log_level_debug,
    log_level_trace,
} CLogLevel;

typedef enum {
    category_source = 1,
    category_schema,
} CSourceCategory;

typedef enum {
    Enum = 1,
    Struct,
    Array,
    Primitive,
    User,
    Alias,
} CUserTypeKind;

const unsigned char CPostPacketKind = 1;

// worker event tag
typedef enum {
    worker_progress,
    worker_result,
    worker_skipped,
} CWorkerResponseTag;

#ifdef __cplusplus
}
#endif
