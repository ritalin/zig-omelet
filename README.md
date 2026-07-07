# zig-omelet

Micro object Relation mapping for Zig.

this tool has following fetues:

* can extract named placeholder in SQL to convert into positional
* can extract select list in SQL to convert result set type definition

> [!NOTE]
> Currently, extracting is only from `duckdb` and code generation is only `typescript`.

## Requirement

* zig (https://ziglang.org) - 0.16.0 or latter
* libduckdb (https://duckdb.org) - 1.5.3 or latter
* libcatch2 (https://github.com/catchorg/Catch2) 3.15.0 or latter

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
omelet generate \
    --source-dir=./_sql-examples \
    --schema-dir=./_schema-examples/user_types \
    --schema-dir=./_schema-examples/tables \
    --exclude-filter=tables \
    --output-dir=./_dump/ts \
```

> [!NOTE]
> if the schema includes user-defined types, all of them must be specified before the table definitions.

### Run with watch mode

you can use `--watch` option to track the file content change.

```
omelet generate \
    --source-dir=./_sql-examples \
    --schema-dir=./_schema-examples/user_types \
    --schema-dir=./_schema-examples/tables \
    --exclude-filter=tables \
    --output-dir=./_dump/ts \
    --watch
```

### Init default setting environment scope

Following command is generate a new scope specified environment scope (`default`).

A environment is generated at `.omelet` directory in current directory.

```
omelet init-default --target-scope foo
```

if you want to specify the scope name explicitly, you can type following command.

```
omelet init-default --target-scope foo --from-scope test
```

> [!NOTE]
> you can use `--global` option to `.omelet` directory in local configuration directory on your machine.
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

> [!NOTE]
> A value variant `.default` is always applied values from CLI arg input.

`generate` command

|         key         |   value variant   |                 note                  |                         example                          |
| ------------------- | ----------------- | ------------------------------------- | -------------------------------------------------------- |
| .source_dir_set     | .value (multiple) | Source query folder path(s).          | .source_dir_set = .{ .values = .{"/path/from", ...} }    |
| .schema_dir_set     | .value (multiple) | Schema query folder path(s).          | .schema_dir_set = .{ .values = .{"/path/from", ...} }    |
| .include_filter_set | .value (multiple) | Source/Schema path include filter(s). | .include_filter_set = .{.values = .{ "PATTERN1", ... } } |
| .exclude_filter_set | .value (multiple) | Source/Schema path exclude filter(s). | .exclude_filter_set = .{.values = .{ "PATTERN1", ... } } |
| .output_dir_path    | .value (single)   | Destination folder path               | .output_dir_path = .{ .values = .{"/path/to"} }          |
| .interactive        | .enabled          | Enable/Disable watch mode             | .interactive = .{.enabled = true}                        |

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
    .interactive = .{.enabled = true}, 
}
```

### Init subcommand configuration environment

Following command is generate a subcommand configuration environment specified command (scope = `default`).

A environment is generated at `.omelet` directory in current directory.

```
./zig-out/bin/omelet init-default --target-scope foo
```

If a scope is also specified, run following command.

```
./zig-out/bin/omelet init-config --target-scope my_scope --from-scope test
```

`--global` option also supports.

```
./zig-out/bin/omelet init-config --target-scope my_scope --global
```

In general option, using `--use-config-scope` option result in applying the custom scope.

#### Subcommand configuration environment formats

> [!NOTE]
> A value variant `.default` is always applied values from CLI arg input.

Top leven configuration of the `generate` subcommand:

|       key       |                      note                      |
| --------------- | ---------------------------------------------- |
| .stage_watch    | This stage spplies sources.                    |
| .stage_extract  | This stage extracts data from source.          |
| .stage_generate | This stage generates code from extracted data. |

Stage configuration

> [!NOTE]
> top level key in stage configuration indicates executable file name.

| key                | value variant | note  
| .name              | string        | Guest name. If .default, field name is used.                      |
| .location          | string        | Path of executable file name. `.default` is same as `omelet` app. |
| .enable_managed    | bool          | Manage auto launch of a stage.                                    |
| .extra_args        | list          | Extra arguments for a stage.                                      |

> [!NOTE]
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

## Credits

- DuckDB: https://duckdb.org/
- magic_enum: https://github.com/Neargye/magic_enum
