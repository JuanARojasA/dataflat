# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Obsidian context
@E:\Obsidian\Claude\Github\dataflat\Overview.md
@E:\Obsidian\Claude\Github\dataflat\Architecture.md
@E:\Obsidian\Claude\Github\dataflat\Memory.md


## Important
Do not reread files that you have already loaded/readed into context on the current session, minimize the usage of tools

## Commands

The project uses `uv` as the package manager. Always prefix Python commands with `uv run`.

```bash
# Run all non-slow tests (no Docker/Spark required)
uv run python -m pytest -m "not slow"

# Run a single test
uv run python -m pytest tests/dicitonary_test/flattener_test.py::test_flatten_camel_to_snake

# Run all tests (requires Java 17 for PySpark)
uv run python -m pytest

# Run with coverage
uv run coverage run -m pytest && uv run coverage xml

# Lint
uv run ruff check .
uv run pyrefly check
uv run pylint dataflat/ --disable=all --enable=duplicate-code --min-similarity-lines=6

# Install dev dependencies
uv sync --group dev
```

> PySpark tests (`tests/pyspark_test/`) require Java 17 and are marked `@pytest.mark.slow`. No Docker needed — PySpark runs in local mode. Skip them with `-m "not slow"` during local development.

## Architecture

`dataflat` flattens nested data (dicts, Pandas DataFrames, Polars DataFrames, PyArrow Tables, PySpark DataFrames) into a set of relational tables. The public API is exposed from `dataflat/__init__.py` (`handler`, `FlattenerOptions`, `CaseTranslatorOptions`, `CustomCaseTranslator`).

### Source layout

```
dataflat/
├── __init__.py          # public API re-exports
├── _core.py             # FlattenerOptions, handler(), FlatteningException
├── base_flattener.py    # BaseFlattener abstract dataclass
├── commons.py           # backward-compat shim → re-exports init_logger from utils.logger
├── dictionary/          # flattener for Python dicts
├── pandas/              # flattener for Pandas DataFrames
├── polars/              # flattener for Polars DataFrames
├── pyarrow/             # flattener for PyArrow Tables
├── pyspark/             # flattener for PySpark DataFrames
└── utils/
    ├── case_translator.py
    ├── logger.py        # init_logger canonical location
    └── string.py
```

### Core pattern

`BaseFlattener` (in `dataflat/base_flattener.py`) defines the shared interface and state defaults. Five concrete implementations extend it:

| Module                  | Input type            |
|-------------------------|-----------------------|
| `dataflat/dictionary/`  | Python `dict`         |
| `dataflat/pandas/`      | Pandas `DataFrame`    |
| `dataflat/polars/`      | Polars `DataFrame`    |
| `dataflat/pyarrow/`     | PyArrow `Table`       |
| `dataflat/pyspark/`     | PySpark `DataFrame`   |

Each `CustomFlattener.flatten()` returns `dict[str, <entity>]` where keys are dot-joined paths (e.g., `"data"`, `"data.orders"`, `"data.orders.items"`) representing the hierarchy level. Each subpackage also exports `CustomFlattener` from its `__init__.py`.

### PyArrow and Pandas flattener internals

Both `dataflat/pyarrow/flattener.py` and `dataflat/pandas/flattener.py` inherit from `_PyArrowBaseFlattener` in `dataflat/pyarrow/_base.py`, which holds all shared struct-expansion and list-explosion logic.

`dataflat/pandas/flattener.py` accepts a `pd.DataFrame` and converts it to a `pa.Table` via `pa.Table.from_pandas(data, preserve_index=False)` before processing. This is the "change dtype_backend to pyarrow" step: the `numpy_nullable` backend marks `str`, `dict`, and `list` columns all as `object`, while PyArrow correctly infers `struct` and `list` types.

After conversion the pandas metadata is stripped with `table.replace_schema_metadata({})`. Without this, `to_pandas(types_mapper=pd.ArrowDtype)` on derived tables would try to restore the original complex type from its string representation (e.g. `struct<...>[pyarrow]`) and fail for deeply nested types.

Results are converted back to `pd.DataFrame` with `.to_pandas(types_mapper=pd.ArrowDtype)`, returning all columns with the PyArrow dtype backend. `pd.errors.Pandas4Warning` is suppressed locally around that call.

### PySpark flattener internals

`dataflat/pyspark/flattener.py` stores the result of each `spark.sql(select_query)` directly into `_flattened_dataframes` — do **not** convert it through `.rdd` and back with `createDataFrame(rdd, schema)`. That roundtrip creates a Python-RDD-backed DataFrame, which forces a Python worker subprocess during collection and crashes with PySpark 4.x + PyArrow 23.x.

### `_core.py` dispatch

`handler()` uses `_get_flattener_class()` which defers the import of each concrete flattener so optional dependencies (pyspark, polars) are only loaded when actually requested. Add new flatteners there, not via dynamic string imports.

### Naming conventions for flattened output

- Root entity keeps its raw primary key (e.g. `id`, or the auto-generated UUID column) and partition key columns (`date`) with their original names.
- Child entities receive prefixed versions: root `id` → `data.id`, root `date` → `data.date`.
- Each list/array explosion adds an `index` column (0-based, per-parent) to the child entity.
- When that child entity produces its own children, its `index` is renamed to `<entity_name>.index` (e.g., `data.orders.index`) before propagation.
- Nested struct/object fields are dot-expanded inline: `client.clientId`, `payment.method`, etc.
- **Lists of objects (structs)** become separate child entities (dot-joined path, e.g. `data.orders.items`), with a positional `index` column.
- **Lists of scalars** (strings, ints, floats, booleans) are joined with `"|"` into a single string column in the parent entity (e.g. `tags: ["A","B","C"]` → `tags: "A|B|C"`). No child entity is created.

### White list

`white_list` (list of dot-joined relative paths, default `[]`) filters the flattened output **after all unnesting and before case translation**. `entity_name` is automatically prepended to each entry.

- **Entity-level**: entry matches an existing entity key → that entity and all its descendants are kept with all columns.
- **Column-level**: entry does not match any entity key → the longest entity key that is a prefix of the entry is found; that entity is kept narrowed to inherited join columns + the specified column; all its child entities are dropped.
- Multiple column entries for the same entity are additive.
- Entity-level overrides column-level for the same entity.
- **Inherited join columns** (primary key, partition keys, index columns) are always preserved under column-level filtering.

`_compute_white_list_plan(result_keys, entity_inherited_columns)` in `BaseFlattener` computes the shared plan (`dict[str, Optional[set[str]]]`: `None` = keep all columns, `set` = keep only those columns). Each flattener calls this from its own `_apply_white_list()` / `__apply_white_list()` method, called after processing and before `_apply_column_translate()`.

Each flattener also maintains `_entity_inherited_columns: dict[str, list[str]]` populated during traversal:
- Root entity: `[primary_key] + partition_keys`
- Non-root entities: `inherited_cols_from_parent + ["index"]`

For PySpark, `_entity_inherited_columns` is inferred post-hoc by `__build_entity_inherited_columns()` from the DataFrame column names (columns matching `<entity_name>.<pk>`, `<entity_name>.<partition_key>`, `*.index`, or `"index"`).

### Shared helpers in `BaseFlattener`

`BaseFlattener` holds shared state defaults: `case_translator`, `entity_name`, `primary_key` (default `None`), `partition_keys`, `black_list`, `white_list`. `_process_strings(string)` is also defined here and used by all subclasses — it applies case translation; the separator is always `"."`. Do **not** redefine it in subclasses.

`_dataflat_id_col_name` (property) returns the auto-generated primary-key column name based on `case_translator.to_case`: `dataflat_id_column` (SNAKE/default), `dataflat-id-column` (KEBAB), `dataflatIdColumn` (CAMEL), `DataflatIdColumn` (PASCAL), `Dataflat id column` (HUMAN), `dataflatidcolumn` (LOWER).

When `flatten()` is called with `primary_key=None`, each flattener generates a UUID column named by `_dataflat_id_col_name` and assigns it as the effective primary key for that run.

Each subclass uses `_setup(...)` as the internal state-initialiser called at the start of `flatten()`.

### Case translation

`CustomCaseTranslator` (Pydantic `BaseModel` in `dataflat/utils/case_translator.py`) translates key/column names between SNAKE, KEBAB, CAMEL, PASCAL, HUMAN, and LOWER cases. Each enum member has a unique string value (SNAKE=`"_"`, KEBAB=`"-"`, CAMEL/PASCAL/HUMAN/LOWER use descriptive strings).

All five backends apply translation as a **single final pass** after all unnesting is complete — never during traversal. This ensures internal logic (blacklist matching, inherited column tracking, child entity naming) always uses original untranslated names:

- **DataFrames** (PyArrow, Pandas, Polars, PySpark): `_apply_column_translate()` / `__apply_column_translate()` renames all columns and entity keys in `_result`.
- **Dictionary**: `__apply_column_translate()` rebuilds `__flatten_dict` with translated entity names and field keys, including heritable fields (e.g. `data.id`, `data.date`) which are assembled during `__fix_nested_list` in original case.

Translation is skipped when `case_translator` is `None`, `from_case == to_case`, or either case is `None`.

### Logging

`init_logger` lives in `dataflat/utils/logger.py`. All internal imports use `from dataflat.utils.logger import init_logger`. `dataflat/commons.py` is a thin re-export shim kept for backward compatibility only.

### Test layout

```
tests/
├── conftest.py
├── base_test/           # tests for BaseFlattener
├── dicitonary_test/     # tests for dictionary flattener (typo in name — intentional)
├── pandas_test/         # tests for Pandas flattener
├── polars_test/         # tests for Polars flattener
├── pyarrow_test/        # tests for PyArrow flattener
├── pyspark_test/        # tests for PySpark flattener (requires Java 17, marked slow)
├── utils_test/
└── resources/           # golden NDJSON files + order_schema.yaml
```

### Test resources

Golden files in `tests/resources/{camel,snake,black_list}/` store expected NDJSON output (one JSON object per line, sorted keys, no nulls). Tests use SHA-256 hash comparison via the `compare_result` fixture. The fixture normalizes CRLF→LF before hashing to handle Windows checkouts (`core.autocrlf=true`).

Scalar list fields appear as `"|"`-joined string columns in their parent entity's golden file — there is no separate child golden file for them. For example, `data.orders.json` includes a `notes` column and `data.orders.items.json` includes a `tags` column; `data.orders.notes.json` and `data.orders.items.tags.json` do **not** exist.

The `nested_order_data` session fixture generates 10 realistic, deeply-nested order records from `tests/resources/order_schema.yaml` using Faker. The Polars flattener tests load data via NDJSON round-trip (`pl.read_ndjson`) to support nullable struct columns — `pl.from_dicts()` does not handle those. The Pandas flattener tests load data via `pa_json.read_json` → `.to_pandas(types_mapper=pd.ArrowDtype)` for the same reason.

### Dependency groups

`pyspark` is included in the `dev` group (`uv sync --group dev` installs it). It can also be installed standalone with `uv sync --group pyspark`.

`pyarrow` has its own standalone group (`uv sync --group pyarrow`) but is also included in `dev` (and transitively through `pandas`). `pyarrow-stubs` and `pandas-stubs` are included in the `lint` group. Some `pyarrow.compute` call sites in `dataflat/pyarrow/_base.py` carry `# pyrefly: ignore[...]` suppressions for stubs limitations (e.g. `pc.greater` overload resolution, `BooleanArray` assignment).

### Folder name typo

The dictionary test folder is named `dicitonary_test` (missing the 'd'). This is intentional/established — do not rename it.
