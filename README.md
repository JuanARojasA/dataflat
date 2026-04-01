# dataflat

A library to flatten nested keys and columns from Python Dictionaries, Pandas DataFrames, Polars DataFrames, PyArrow Tables, and PySpark DataFrames into a set of relational tables.

### Installation

```bash
pip install dataflat
```

For Polars support:
```bash
pip install dataflat polars
```

For Pandas support:
```bash
pip install dataflat pandas pyarrow
```

For PyArrow support:
```bash
pip install dataflat pyarrow
```

For PySpark support:
```bash
pip install dataflat pyspark
```

### Get started

Import `handler`, `FlattenerOptions`, and optionally `CaseTranslatorOptions` from `dataflat`:

```python
from dataflat import handler, FlattenerOptions, CaseTranslatorOptions
```

**FlattenerOptions** selects the backend:

| Option | Input type |
|--------|------------|
| `FlattenerOptions.DICTIONARY` | Python `dict` |
| `FlattenerOptions.PANDAS_DF` | Pandas `DataFrame` |
| `FlattenerOptions.POLARS_DF` | Polars `DataFrame` |
| `FlattenerOptions.PYARROW_TABLE` | PyArrow `Table` |
| `FlattenerOptions.PYSPARK_DF` | PySpark `DataFrame` |

**CaseTranslatorOptions** (optional) translates key / column names after flattening:

| Option | Example output |
|--------|---------------|
| `SNAKE` | `order_id` |
| `CAMEL` | `orderId` |
| `PASCAL` | `OrderId` |
| `KEBAB` | `order-id` |
| `HUMAN` | `Order id` |
| `LOWER` | `orderid` |

### Instantiate a flattener

Use `handler()` to get a configured flattener instance:

```python
from dataflat import handler, FlattenerOptions, CaseTranslatorOptions

flattener = handler(
    custom_flattener=FlattenerOptions.DICTIONARY,  # or POLARS_DF / PYSPARK_DF
    from_case=CaseTranslatorOptions.CAMEL,         # optional
    to_case=CaseTranslatorOptions.SNAKE,           # optional
    remove_special_chars=False,                    # strip non-alphanumeric chars (default False)
)
```

Or import the concrete class directly if you prefer:

```python
from dataflat.dictionary import CustomFlattener   # dict
from dataflat.pandas import CustomFlattener       # Pandas
from dataflat.polars import CustomFlattener       # Polars
from dataflat.pyarrow import CustomFlattener      # PyArrow
from dataflat.pyspark import CustomFlattener      # PySpark
```

### Flatten data

All flatteners share the same `flatten()` signature:

```python
flatten_data = flattener.flatten(
    data=data,                                      # dict / Polars / PySpark DataFrame
    primary_key="id",                               # optional; auto-generates UUID column when omitted
    entity_name="data",                             # root entity name, default "data"
    partition_keys=["date"],                        # columns propagated to all children
    black_list=["keys.to", "be.ignored"],           # fields excluded from output
    white_list=["orders.items", "orders.items.name"],  # entities/columns to retain (see below)
)
```

**Parameters**

- `primary_key` — identifies each root record; propagated to all child entities as `<entity_name>.<primary_key>` (e.g. `data.id`). When `None` (the default), a UUID column is auto-generated with a name that matches `to_case`: `dataflat_id_column` (SNAKE), `dataflat-id-column` (KEBAB), `dataflatIdColumn` (CAMEL), `DataflatIdColumn` (PASCAL), `Dataflat id column` (HUMAN), `dataflatidcolumn` (LOWER).
- `partition_keys` — additional root columns (e.g. `["date"]`) inherited by every child entity.
- `black_list` — dot-joined field paths excluded from all output (e.g. `["summary.totalClients"]`).
- `white_list` — dot-joined paths that select which entities and/or columns to retain after flattening (see [White list](#white-list)).

**Return value**

`flatten()` returns `dict[str, entity]` — one key per entity, named by dot-joined path.

Nested fields are handled as follows:

- **Struct / object fields** are expanded inline using dot-notation (`payment.method`, `client.address.city`).
- **Lists of objects** produce a child entity (e.g. `data.orders.products`) with a positional `index` column (0-based, per-parent).
- **Lists of scalars** (strings, ints, floats, booleans) are joined with `"|"` into a single string column in the parent entity — no child entity is created.

```json
{
  "data": [{"id": 1, "date": "2024-01-01", "tags": "featured|sale", "total": 1900}],
  "data.orders": [
    {"id": "abc123", "total": 700, "notes": "fragile|urgent", "data.id": 1, "data.date": "2024-01-01", "index": 0},
    {"id": "dfg456", "total": 1200, "notes": "gift", "data.id": 1, "data.date": "2024-01-01", "index": 1}
  ],
  "data.orders.products": [
    {"id": "ab", "price": 200, "data.id": 1, "data.date": "2024-01-01", "data.orders.index": 0, "index": 0},
    {"id": "cd", "price": 500, "data.id": 1, "data.date": "2024-01-01", "data.orders.index": 0, "index": 1},
    {"id": "fg", "price": 1200, "data.id": 1, "data.date": "2024-01-01", "data.orders.index": 1, "index": 0}
  ]
}
```

Produced from:

```json
{
  "id": 1,
  "date": "2024-01-01",
  "orders": [
    {
      "id": "abc123",
      "products": [
        {"id": "ab", "price": 200},
        {"id": "cd", "price": 500}
      ],
      "total": 700
    },
    {
      "id": "dfg456",
      "products": [
        {"id": "fg", "price": 1200}
      ],
      "total": 1200
    }
  ],
  "total": 1900
}
```

### White list

`white_list` filters the flattened output after all unnesting and before case translation. Entries are relative paths — `entity_name` (default `"data"`) is prepended automatically.

**Entity-level** — if the entry matches an existing entity key, that entity and all its descendants are kept with every column:

```python
results = flattener.flatten(
    data, primary_key="id", partition_keys=["date"],
    white_list=["orders.items", "orders.client.addresses"],
)
# Keeps: data.orders.items, data.orders.items.attributes, data.orders.client.addresses
# Drops: data, data.orders, data.orders.client, ...
```

**Column-level** — if the entry does not match any entity key, the flattener finds the entity whose path is a prefix of the entry, retains that entity narrowed to the specified column plus all inherited join columns (pk, partition keys, index columns), and drops all child entities of it:

```python
results = flattener.flatten(
    data, primary_key="id", partition_keys=["date"],
    white_list=["orders.items.name", "orders.items.price", "summary.total_revenue"],
)
# Keeps: data (narrowed to id + date + summary.total_revenue)
#        data.orders.items (narrowed to data.id + data.date + data.orders.index + index + name + price)
# Drops: data.orders, data.orders.items.attributes, data.orders.client, ...
```

**Rules:**
- Multiple column entries for the same entity are additive.
- Entity-level overrides column-level for the same entity (full entity is kept).
- Inherited join columns (pk, partition keys, index columns) are **always** preserved, even under column-level filtering.
- An empty `white_list` (the default) keeps everything.

### Recommendations

1. For `PYSPARK_DF` it is recommended to enable case-sensitive mode in Spark:
    ```python
    spark.conf.set("spark.sql.caseSensitive", True)
    ```

2. For `POLARS_DF` with nullable struct columns, load data via `pl.read_ndjson` instead of `pl.from_dicts` — the latter does not handle nullable structs correctly.
    ```python
    import polars as pl
    df = pl.read_ndjson("data.ndjson")
    ```

3. For `PANDAS_DF`, the flattener internally converts the DataFrame to a PyArrow Table to reliably distinguish `str`, `dict`, and `list` columns (the `numpy_nullable` dtype backend marks all three as `object`). The result is returned with the `pyarrow` dtype backend (`pd.ArrowDtype`), giving properly nullable typed columns.

4. For `PYARROW_TABLE` with single JSON objects (not NDJSON), use `pa.Table.from_pylist`:
    ```python
    import json
    import pyarrow as pa
    with open("data.json") as f:
        data = json.load(f)
    table = pa.Table.from_pylist([data])
    ```
    For NDJSON or multiple records use `pyarrow.json.read_json`:
    ```python
    import pyarrow.json as pa_json
    table = pa_json.read_json("data.ndjson")
    ```
