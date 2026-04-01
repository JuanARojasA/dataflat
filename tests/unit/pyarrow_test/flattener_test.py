import io
import json

import pyarrow as pa
import pyarrow.json as pa_json
import pytest
from pydantic import ValidationError

from dataflat.pyarrow.flattener import CustomFlattener
from dataflat.utils.case_translator import CaseTranslatorOptions
from tests.unit.conftest import _records_to_ndjson


def _table_from_json_path(path: str) -> pa.Table:
    """Load a single JSON-object file as a one-row PyArrow Table."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return pa.Table.from_pylist([data])


def _table_from_dicts(records: list[dict]) -> pa.Table:
    """Convert a list of dicts to a PyArrow Table via NDJSON round-trip."""
    ndjson_bytes = "\n".join(
        json.dumps(record, default=str) for record in records
    ).encode()
    return pa_json.read_json(io.BytesIO(ndjson_bytes))


def _entity_to_string(table: pa.Table) -> str:
    """Serialise a PyArrow Table to a sorted, null-free NDJSON string."""
    return _records_to_ndjson(table.select(sorted(table.schema.names)).to_pylist())


def test_flattener():
    base = CustomFlattener()
    assert base.case_translator is None
    assert base.entity_name == "data"
    assert base.primary_key is None


def test_flatten_invalid_type():
    flattener = CustomFlattener()
    with pytest.raises(ValidationError):
        flattener.flatten("not_a_table")  # type: ignore[arg-type]


def test_flatten_camel_to_snake(get_custom_flattener, get_full_path, compare_result):
    from_case = CaseTranslatorOptions.CAMEL
    to_case = CaseTranslatorOptions.SNAKE
    flattener: CustomFlattener = get_custom_flattener(
        CustomFlattener, from_case, to_case
    )
    data = _table_from_json_path(get_full_path(from_case.name.lower(), "original"))
    results = flattener.flatten(data, primary_key="id", partition_keys=["date"])

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
    data = _table_from_json_path(get_full_path(from_case.name.lower(), "original"))
    results = flattener.flatten(data, primary_key="id", partition_keys=["date"])

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
    data = _table_from_json_path(get_full_path("black_list", "original"))
    results = flattener.flatten(
        data, primary_key="id", black_list=["total_orders", "summary.total_clients"]
    )

    for entity, result in results.items():
        string_result = _entity_to_string(result)
        assert compare_result(string_result, get_full_path("black_list", entity))


@pytest.mark.slow
def test_flatten_heavy(nested_order_data):
    """Flatten 10 deeply-nested order records and verify structural correctness."""
    flattener = CustomFlattener()
    data = _table_from_dicts(nested_order_data)
    results = flattener.flatten(data, primary_key="batchId")

    # Root entity must be present.
    assert "data" in results
    root_table = results["data"]

    # Root keeps the pk column with its original name.
    assert "batchId" in root_table.schema.names

    # No list columns should remain in any entity.
    for entity_name, table in results.items():
        list_cols = [
            name
            for name in table.schema.names
            if pa.types.is_list(table.schema.field(name).type)
        ]
        assert list_cols == [], (
            f"Entity '{entity_name}' still has list columns: {list_cols}"
        )

    # No struct columns should remain in any entity.
    for entity_name, table in results.items():
        struct_cols = [
            name
            for name in table.schema.names
            if pa.types.is_struct(table.schema.field(name).type)
        ]
        assert struct_cols == [], (
            f"Entity '{entity_name}' still has struct columns: {struct_cols}"
        )

    # Every non-root entity must carry the root pk column (data.batchId).
    root_pk = "data.batchId"
    non_root_entities = {k: v for k, v in results.items() if k != "data"}
    for entity_name, table in non_root_entities.items():
        assert root_pk in table.schema.names, (
            f"Entity '{entity_name}' is missing root pk '{root_pk}'"
        )

    # Every non-root entity must have its own 'index' column.
    for entity_name, table in non_root_entities.items():
        assert "index" in table.schema.names, (
            f"Entity '{entity_name}' is missing its own 'index' column"
        )

    # The 'index' column is 0-based per-parent.
    for entity_name, table in non_root_entities.items():
        min_index = [x for x in table.column("index").to_pylist() if x is not None]
        assert min(min_index) == 0, f"Entity '{entity_name}' index does not start at 0"
