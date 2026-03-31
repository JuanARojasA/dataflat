## v3.0.0 (2026-03-30)

### BREAKING CHANGE

- `primary_key` default changed from `"id"` to `None`; callers that relied on the implicit `"id"` primary key must now pass `primary_key="id"` explicitly to `flatten()`
- `replace_string` parameter removed from `BaseFlattener`, `handler()`, and all flatteners; the nested-key separator is always `"."`
- `dataflat/flattener_handler.py` renamed to `dataflat/_core.py`; import paths change accordingly
- `dataflat/base/flattener.py` moved to `dataflat/base_flattener.py`
- `dataflat/pyspark_df/` package renamed to `dataflat/pyspark/`
- `dataflat/exceptions.py` removed; `FlatteningException` now lives in `dataflat/_core.py`
- `CaseTranslatorOptions` enum values changed from dict objects (`{"id": N, "split_string": "..."}`) to plain strings (`"_"`, `"-"`, `"camel"`, etc.); `CustomCaseTranslator` migrated from a `@typechecked` class to a Pydantic `BaseModel`
- `dataflat/commons.py` deprecated (kept as a backward-compat shim); canonical logger location is now `dataflat/utils/logger.py`
- Public API (`handler`, `FlattenerOptions`, `CaseTranslatorOptions`, `CustomCaseTranslator`) now importable directly from `dataflat` via the new `dataflat/__init__.py`
- **Scalar lists are no longer exploded into child entities.** Lists of primitive values (strings, integers, floats, booleans) are now joined with `"|"` and stored as a single string column in the parent entity. Only lists of structs/objects produce child entities. Golden files `data.orders.notes.json` and `data.orders.items.tags.json` no longer exist; `notes` and `tags` now appear as columns in `data.orders` and `data.orders.items` respectively.
- **`typeguard` removed as a dependency.** Runtime type validation is now handled by `pydantic.validate_call`. Invalid `data` arguments to `flatten()` now raise `pydantic.ValidationError` instead of `typeguard.TypeCheckError`.

### Feat

- **FlattenerOptions**: added `POLARS_DF`, `PYARROW_TABLE`, and `PANDAS_DF` options
- **polars**: new `CustomFlattener` for Polars DataFrames (`dataflat/polars/flattener.py`)
- **pyarrow**: new `CustomFlattener` for PyArrow Tables (`dataflat/pyarrow/flattener.py`)
- **pandas**: `CustomFlattener` for Pandas DataFrames re-implemented on top of PyArrow (`dataflat/pandas/flattener.py`); converts input `pd.DataFrame` to `pa.Table` for reliable struct/list type inference, returns result with `pd.ArrowDtype` backend
- **base_flattener**: when `primary_key` is `None`, auto-generate a UUID column whose name follows the configured `to_case` (`dataflat_id_column` / `dataflat-id-column` / `dataflatIdColumn` / `DataflatIdColumn` / `Dataflat id column` / `dataflatidcolumn`)
- **case_translator**: added `HUMAN` and `LOWER` cases to `CaseTranslatorOptions`; improved pre-processing for mixed alphanumeric strings
- **handler()**: now uses deferred imports so optional dependencies (pyspark, polars, pyarrow) are only loaded when the corresponding flattener is requested
- **validate_call**: all five `CustomFlattener.flatten()` methods are now decorated with `@validate_call` (using `arbitrary_types_allowed=True` for DataFrame/Table types); passing an unsupported type raises `pydantic.ValidationError`

### Refactor

- **pyarrow**: extracted shared struct-expansion and list-explosion logic into `dataflat/pyarrow/_base.py` (`_PyArrowBaseFlattener`); both PyArrow and Pandas flatteners inherit from it
- **utils**: moved `init_logger` to `dataflat/utils/logger.py`; `dot_join_args` lives in `dataflat/utils/string.py`
- **build**: migrated from `[project.optional-dependencies]` to `uv` dependency groups (`dev`, `pandas`, `polars`, `pyarrow`, `pyspark`, `lint`); added `pydantic` as a core dependency; added `ruff`, `pyrefly`, `pylint` to `lint` group
- **tests**: added full test suites for Polars, PyArrow, Pandas, and case translator (`polars_test/`, `pyarrow_test/`, `pandas_test/`, `utils_test/`); renamed `pyspark_df_test/` → `pyspark_test/`; added `tests/resources/order_schema.yaml` for Faker-based heavy test data generation; SHA-256 hash comparison for golden-file tests with CRLF normalisation
- **lint**: added `pandas-stubs` and `pyarrow-stubs` to the `lint` dependency group; resolved all pyrefly type errors (0 errors, 2 suppressed for pyarrow-stubs limitations)
- **typeguard → pydantic**: replaced `@typechecked` decorator (on `handler()`, `init_logger()`, and the dictionary/pyspark `CustomFlattener` classes) with `@validate_call` from pydantic; removed `typeguard` from `pyproject.toml` dependencies

### Fix

- **pyproject.toml**: suppress `pandas.errors.Pandas4Warning` in pytest `filterwarnings`
- **pyspark**: store `spark.sql()` results directly in `_flattened_dataframes` instead of round-tripping through RDD — avoids Python worker subprocess crash with PySpark 4.x + PyArrow 23.x
- **dictionary**: replaced per-entity `__apply_translate` + `_process_strings(entity_name)` calls inside `__fix_nested_list` with a single final-pass `__apply_column_translate` invoked after all unnesting is complete; fixes a bug where heritable field keys (e.g. `data.id`, `data.date`) were not translated when a case translator was active; aligns the Dictionary flattener with the single-pass pattern used by all DataFrame backends

### Tests

- **invalid-type**: added `test_flatten_invalid_type` to each of the five flattener test suites (`dicitonary_test`, `pyarrow_test`, `pandas_test`, `polars_test`, `pyspark_test`) to assert that passing a non-supported type to `flatten()` raises `pydantic.ValidationError`


## v2.0.0 (2024-09-06)

### BREAKING CHANGE

- deprecate pandas flattener,change variable names and delete unused variables from function calls

### Fix

- **dataflat**: fix error with spark and dictionary flattener, deprecate pandas flattener

## v1.1.2 (2024-09-03)

### Fix

- **base/flattener.py**: fix problem with flatten method, it should be abstract
- **commons.py**: a fix was made for a problem with "| None"

## v1.1.1 (2024-09-02)

### Refactor

- **dictionary/flattener.py**: fix sonarcloud issue with function complexity