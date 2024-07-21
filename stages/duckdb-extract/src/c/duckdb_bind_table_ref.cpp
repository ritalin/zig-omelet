


#include "duckdb_binder_support.hpp"





auto resolveColumnType(duckdb::ClientContext context, duckdb::SQLStatement& stmt) -> duckdb::unique_ptr<duckdb::BoundTableRef> {
    return duckdb::make_uniq(nullptr);
}