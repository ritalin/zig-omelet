#pragma once

#include <stdint.h>
#include <stddef.h>
#include <nng/nng.h>

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

#ifdef __cplusplus
extern "C" {
#endif

typedef struct OpaqueDatabase *DatabaseRef;
typedef struct OpaqueCollector *CollectorRef;

typedef struct Slice_ {
    const char* ptr;
    size_t len;
} Slice;

typedef struct SourceDescriptor_ {
    uint8_t response_event_tag;
    uint8_t log_event_tag;
    Slice name;
    Slice dialect;
    Slice hash;
} SourceDescriptor;


int32_t initDatabase(DatabaseRef *handle);
void deinitDatabase(DatabaseRef handle);
WorkerResultCode loadSchema(DatabaseRef handle, Slice schema_dir_path);
WorkerResultCode retainUserTypeName(DatabaseRef handle);

int32_t initSourceCollector(DatabaseRef db_ref, Slice stage, SourceDescriptor desc, CollectorRef *handle);
void deinitSourceCollector(CollectorRef handle);
WorkerResultCode executeDescribe(CollectorRef handle, Slice query);
size_t getDescribeResultCount(CollectorRef handle);
nng_msg* getDescribeResult(CollectorRef handle, size_t index);

int32_t initUserTypeCollector(DatabaseRef db_ref, Slice stage, SourceDescriptor desc, CollectorRef *handle);
void deinitUserTypeCollector(CollectorRef handle);
WorkerResultCode describeUserType(CollectorRef handle, Slice query);
size_t getUserTypeResultCount(CollectorRef handle);
nng_msg* getUserTypeResult(CollectorRef handle, size_t index);

#ifdef __cplusplus
}
#endif