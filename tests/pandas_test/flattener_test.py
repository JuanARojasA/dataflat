# pylint: disable=duplicate-code
import io
import json

import pandas as pd
import pyarrow as pa
import pyarrow.json as pa_json
import pytest

from dataflat.pandas.flattener import CustomFlattener
from dataflat.utils.case_translator import CaseTranslatorOptions


def _df_from_json_path(path: str) -> pd.DataFrame:
    """Load a single JSON-object file as a one-row Pandas DataFrame with ArrowDtype."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    table = pa.Table.from_pylist([data])
    return table.to_pandas(types_mapper=pd.ArrowDtype)


def _df_from_dicts(records: list[dict]) -> pd.DataFrame:
    """Convert a list of dicts to a Pandas DataFrame via NDJSON round-trip (ArrowDtype)."""
    ndjson_bytes = "\n".join(
        json.dumps(record, default=str) for record in records
    ).encode()
    table = pa_json.read_json(io.BytesIO(ndjson_bytes))
    return table.to_pandas(types_mapper=pd.ArrowDtype)


def _is_arrow_list(dtype: object) -> bool:
    return isinstance(dtype, pd.ArrowDtype) and pa.types.is_list(dtype.pyarrow_dtype)


def _is_arrow_struct(dtype: object) -> bool:
    return isinstance(dtype, pd.ArrowDtype) and pa.types.is_struct(dtype.pyarrow_dtype)


def _entity_to_string(df: pd.DataFrame) -> str:
    """Serialise a Pandas DataFrame to a sorted, null-free NDJSON string."""
    sorted_cols = sorted(df.columns)
    rows = df[sorted_cols].to_dict(orient="records")
    return "\n".join(
        json.dumps(
            {k: v for k, v in sorted(row.items()) if v is not None and v is not pd.NA},
            separators=(",", ":"),
        )
        for row in rows
    )


def test_flattener():
    base = CustomFlattener()
    assert base.case_translator is None
    assert base.replace_string == "."
    assert base.entity_name == "data"
    assert base.primary_key == "id"


def test_flatten_camel_to_snake(get_custom_flattener, get_full_path, compare_result):
    from_case = CaseTranslatorOptions.CAMEL
    to_case = CaseTranslatorOptions.SNAKE
    flattener: CustomFlattener = get_custom_flattener(
        CustomFlattener, from_case, to_case
    )
    data = _df_from_json_path(get_full_path(from_case.name.lower(), "original"))
    results = flattener.flatten(data, partition_keys=["date"])

    for entity, result in results.items():
        string_result = _entity_to_string(result)
        assert compare_result(
            string_result, get_full_path(to_case.name.lower(), entity)
        )


def test_flatten_snake_to_camel(get_custom_flattener, get_full_path, compare_result):
    from_case = CaseTranslatorOptions.SNAKE
    to_case = CaseTranslatorOptions.CAMEL
    flattener: CustomFlattener = get_custom_flattener(
        CustomFlattener, from_case, to_case
    )
    data = _df_from_json_path(get_full_path(from_case.name.lower(), "original"))
    results = flattener.flatten(data, partition_keys=["date"])

    for entity, result in results.items():
        string_result = _entity_to_string(result)
        assert compare_result(
            string_result, get_full_path(to_case.name.lower(), entity)
        )


def test_flatten_black_list(get_custom_flattener, get_full_path, compare_result):
    from_case = CaseTranslatorOptions.SNAKE
    to_case = CaseTranslatorOptions.SNAKE
    flattener: CustomFlattener = get_custom_flattener(
        CustomFlattener, from_case, to_case
    )
    data = _df_from_json_path(get_full_path("black_list", "original"))
    results = flattener.flatten(
        data, black_list=["total_orders", "summary.total_clients"]
    )

    for entity, result in results.items():
        string_result = _entity_to_string(result)
        assert compare_result(string_result, get_full_path("black_list", entity))


@pytest.mark.slow
def test_flatten_heavy(nested_order_data):
    """Flatten 10 deeply-nested order records and verify structural correctness."""
    flattener = CustomFlattener()
    data = _df_from_dicts(nested_order_data)
    results = flattener.flatten(data, primary_key="batchId")

    # Root entity must be present.
    assert "data" in results
    root_df = results["data"]

    # Root keeps the pk column with its original name.
    assert "batchId" in root_df.columns

    # No list columns should remain in any entity.
    for entity_name, df in results.items():
        list_cols = [c for c in df.columns if _is_arrow_list(df[c].dtype)]
        assert list_cols == [], (
            f"Entity '{entity_name}' still has list columns: {list_cols}"
        )

    # No struct columns should remain in any entity.
    for entity_name, df in results.items():
        struct_cols = [c for c in df.columns if _is_arrow_struct(df[c].dtype)]
        assert struct_cols == [], (
            f"Entity '{entity_name}' still has struct columns: {struct_cols}"
        )

    # Every non-root entity must carry the root pk column (data.batchId).
    root_pk = "data.batchId"
    non_root_entities = {k: v for k, v in results.items() if k != "data"}
    for entity_name, df in non_root_entities.items():
        assert root_pk in df.columns, (
            f"Entity '{entity_name}' is missing root pk '{root_pk}'"
        )

    # Every non-root entity must have its own 'index' column.
    for entity_name, df in non_root_entities.items():
        assert "index" in df.columns, (
            f"Entity '{entity_name}' is missing its own 'index' column"
        )

    # The 'index' column is 0-based per-parent.
    for entity_name, df in non_root_entities.items():
        assert df["index"].min() == 0, (
            f"Entity '{entity_name}' index does not start at 0"
        )
