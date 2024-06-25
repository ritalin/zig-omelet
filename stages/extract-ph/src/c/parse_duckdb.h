#pragma once

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TOPIC_ID "id"
#define TOPIC_OFFSET "offset"
#define TOPIC_QUERY "query"
#define TOPIC_PH "placeholder"
#define TOPIC_LOGLEVEL "log"
#define TOPIC_CONTENT "content"

typedef struct Collector *CollectorRef;

int32_t initCollector(const char *id, size_t id_len, void *socket, CollectorRef *handle);
void deinitCollector(CollectorRef handle);
void parseDuckDbSQL(CollectorRef handle, const char *query, size_t query_len);

#ifdef __cplusplus
}
#endif