#pragma once

#include <stdint.h>
#include <stddef.h>

typedef enum {
    no_error = 0,
    schema_dir_not_found,
    schema_load_failed,
    invalid_schema_catalog,
    invalid_sql,
    describe_filed,
} WorkerResultCode;

// query payload topic
#define topic_query "query"
#define topic_placeholder "placeholder"
#define topic_placeholder_order "placeholder-order"
#define topic_select_list "select-list"
#define topic_bound_user_type "bound-user-type"
#define topic_anon_user_type "anon-user-type"
// user type schema topic
#define topic_user_type "user-type"

// worker event tag
typedef enum {
    worker_progress,
    worker_result,
    worker_finished,
    worker_log,
    worker_skipped,
} CWorkerResponseTag;

#ifdef __cplusplus
extern "C" {
#endif

typedef struct OpaqueDatabase *DatabaseRef;
typedef struct OpaqueCollector *CollectorRef;

int32_t initDatabase(DatabaseRef *handle);
void deinitDatabase(DatabaseRef handle);
WorkerResultCode loadSchema(DatabaseRef handle, const char *schema_dir_path, size_t schema_dir_len);
WorkerResultCode retainUserTypeName(DatabaseRef handle);

int32_t initSourceCollector(DatabaseRef db_ref, const char *id, size_t id_len, const char *name, size_t name_len, void *socket, CollectorRef *handle);
void deinitSourceCollector(CollectorRef handle);
WorkerResultCode executeDescribe(CollectorRef handle, const char *query, size_t query_len);

int32_t initUserTypeCollector(DatabaseRef db_ref, const char *id, size_t id_len, const char *name, size_t name_len, void *socket, CollectorRef *handle);
void deinitUserTypeCollector(CollectorRef handle);
WorkerResultCode describeUserType(CollectorRef handle, const char *query, size_t query_len);

#ifdef __cplusplus
}
#endif