#pragma once

#include <stdint.h>
#include <stddef.h>

typedef enum {
    no_error = 0,
    schema_dir_not_found,
    schema_load_failed,
    invalid_sql,
    describe_filed,
} WorkerResultCode;

#define topic_query "query"
#define topic_placeholder "placeholder"
#define topic_select_list "select-list"

#define topic_user_type "user-type"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct Database *DatabaseRef;
typedef struct Collector *CollectorRef;

int32_t initDatabase(DatabaseRef *handle);
void deinitDatabase(DatabaseRef handle);
int32_t loadSchema(DatabaseRef handle, const char *schema_dir_path, size_t schema_dir_len);

int32_t initSourceCollector(DatabaseRef db_ref, const char *id, size_t id_len, void *socket, CollectorRef *handle);
void deinitSourceCollector(CollectorRef handle);
WorkerResultCode executeDescribe(CollectorRef handle, const char *query, size_t query_len);

int32_t initUserTypeCollector(DatabaseRef db_ref, const char *id, size_t id_len, void *socket, CollectorRef *handle);
void deinitUserTypeCollector(CollectorRef handle);
WorkerResultCode describeUserType(CollectorRef handle, const char *query, size_t query_len);

#ifdef __cplusplus
}
#endif