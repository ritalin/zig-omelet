# duckdb-extract-placeholder

this tool can extract named placeholder in SQL to convert into positional 

## Requirement

* zig (https://ziglang.org) - 0.14.0 or latter
* libduckdb (https://duckdb.org) - 0.10.1 or latter
* libzmq (https://zeromq.org) - 4.3.5 or latter
* libcatch2 (https://github.com/catchorg/Catch2) 3.6.0 or latter

This product has tested on MacOS Ventura 13.6.7.

## Build

```
zig build
```

## Usage (Test fright)

```
zig build test-run
```
