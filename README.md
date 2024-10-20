# zig-omelet

this tool has following fetues:

* can extract named placeholder in SQL to convert into positional
* can extract select list in SQL to convert result set type definition

Currently, extracting is only from `duckdb`, code generationg is only `typescript`.

## Requirement

* zig (https://ziglang.org) - 0.14.0 or latter
* libduckdb (https://duckdb.org) - 1.1.0 or latter
* libzmq (https://zeromq.org) - 4.3.5 or latter
* libcatch2 (https://github.com/catchorg/Catch2) 3.6.0 or latter

This product has tested on MacOS Ventura 13.6.7.

## Build

```
zig build
```

## Usage (Run using example query/schema)

```
./zig-out/bin/omelet generate \
    --source-dir=./_sql-examples
    --schema-dir=./_schema-examples/user_types
    --schema-dir=./_schema-examples/tables
    --exclude-filter=tables
    --output-dir=./_dump/ts
```

Note that if the schema includes user-defined types, all of them must be specified before the table definitions.

## Source/Schema file encoding

Source/Schema file encoding is supported UTF8 only.

## Supported statement

* CREATE TYPE (Enum, List and Struct)
* SELECT
* DELETE
