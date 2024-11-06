# zig-omelet

Micro object Relation mapping for Zig.

this tool has following fetues:

* can extract named placeholder in SQL to convert into positional
* can extract select list in SQL to convert result set type definition

Currently, extracting is only from `duckdb` and code generation is only `typescript`.

## Requirement

* zig (https://ziglang.org) - 0.14.0 or latter
* libduckdb (https://duckdb.org) - 1.1.2 or latter
* libzmq (https://zeromq.org) - 4.3.5 or latter
* libcatch2 (https://github.com/catchorg/Catch2) 3.6.0 or latter

This product has tested on MacOS Ventura 13.6.7.

## Build

1. Clone the repository

```
git clone --recursive https://github.com/ritalin/zig-omelet.git $YOUR_PROJECT
```

Note that `recursive` option is required because of containing submodule.


2. Do build

```
cd $YOUR_PROJECT
zig build
```

## Usage (Run using example query/schema)

### Run as one-shot

```
./zig-out/bin/omelet generate \
    --source-dir=./_sql-examples \
    --schema-dir=./_schema-examples/user_types \
    --schema-dir=./_schema-examples/tables \
    --exclude-filter=tables \
    --output-dir=./_dump/ts \
```

Note that if the schema includes user-defined types, all of them must be specified before the table definitions.

### Run with watch mode

you can use `--watch` option to track the file content change.

```
./zig-out/bin/omelet generate \
    --source-dir=./_sql-examples \
    --schema-dir=./_schema-examples/user_types \
    --schema-dir=./_schema-examples/tables \
    --exclude-filter=tables \
    --output-dir=./_dump/ts \
    --watch
```

### Init default value environment

Following command is generate a default value environment specified command (scope = `default`).

A environment is generated at `.omelet` directory in current directory.

> [!INFO]
> Note that `init-default` and `init-config` command is not supported the default value env.

```
./zig-out/bin/omelet init-default --command generate
```

> [!INFO]
> you can use `-global` option to `.omelet` directory in user home directory.
> ```
> ./zig-out/bin/omelet init-default --command generate --global
> ```

If a scope is also specified, run following command.

```
./zig-out/bin/omelet init-default --command generate --scope my_scope
```

In general option, using `--use-scope` option result in applying the custom scope.

```
./zig-out/bin/omelet \
    --use-scope=my_scope \
    generate \
    --source-dir=./_sql-examples ‘
    --schema-dir=./_schema-examples/user_types \
    --schema-dir=./_schema-examples/tables \
    --exclude-filter=tables \
    --output-dir=./_dump/ts \
    --watch

```

#### Default value environment formats

> [!INFO]
> A value variant `.default` is always applied values from CLI arg input.

`generate` command

|         key         |   value variant   |                 note                  |                         example                          |
| ------------------- | ----------------- | ------------------------------------- | -------------------------------------------------------- |
| .source_dir_set     | .value (multiple) | Source query folder path(s).          | .source_dir_set = .{ .values = .{"/path/from", ...} }    |
| .schema_dir_set     | .value (multiple) | Schema query folder path(s).          | .schema_dir_set = .{ .values = .{"/path/from", ...} }    |
| .include_filter_set | .value (multiple) | Source/Schema path include filter(s). | .include_filter_set = .{.values = .{ "PATTERN1", ... } } |
| .exclude_filter_set | .value (multiple) | Source/Schema path exclude filter(s). | .exclude_filter_set = .{.values = .{ "PATTERN1", ... } } |
| .output_dir_path    | .value (single)   | Destination folder path               | .output_dir_path = .{ .values = .{"/path/to"} }          |
| .watch              | .enabled          | Enable/Disable watch mode             | .watch = .{.enabled = true}                              |

`generate` command example:

```zig
.{
    .source_dir_set = .{.values = .{
        "./_sql-examples"
    } },
    .schema_dir_set = .{.values = .{
        "./_schema-examples/user_types", 
        "./_schema-examples/tables",
    } },
    .include_filter_set = .default,
    .exclude_filter_set = .{ .values = .{"tables"} },
    .output_dir_path = .{ .values = .{"./_dump/ts"} },
    .watch = .{.enabled = true}, 
}
```

### Init subcommand configuration environment

Following command is generate a subcommand configuration environment specified command (scope = `default`).

A environment is generated at `.omelet` directory in current directory.

```
./zig-out/bin/omelet init-config --command generate
```

This command is also can use `--scope` option to specify a custom scope.

`--global` option also supports.

```
./zig-out/bin/omelet init-config --command generate --scope my_scope
```

In general option, using `--use-scope` option result in applying the custom scope.

#### Subcommand configuration environment formats

> [!INFO]
> A value variant `.default` is always applied values from CLI arg input.

Top leven configuration

|       key       |                      note                      |
| --------------- | ---------------------------------------------- |
| .stage_watch    | This stage spplies sources.                    |
| .stage_extract  | This stage extracts data from source.          |
| .stage_generate | This stage generates code from extracted data. |

Stage configuration

> [!INFO]
> top level key in stage configuration indicates executable file name.

| key         | value variant | note                                                              |
| .location   | string        | Path of executable file name. `.default` is same as `omelet` app. |
| .extra_args | list          | Extra arguments for a stage.                                      |
| .managed    | bool          | Manage auto launch of a stage.                                    |

> [!INFO]
> A key of extra argument is same as default value environment of a stage.

## Source/Schema file encoding

Source/Schema file encoding is supported UTF8 only.

## Supported statement

* CREATE TYPE (Enum, List and Struct)
* SELECT
* INSERT
* UPDATE
* DELETE

## Limitation

* When arguments for Table function contain correlective column, select list will be nullable.
* Currently, an alias of `RETURNING` clause is not supported.