import json

import pyarrow as pa

from dataflat.pyarrow.flattener import CustomFlattener
from tests.unit.conftest import _records_to_ndjson


def _table_from_json_path(path: str) -> pa.Table:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return pa.Table.from_pylist([data])


def _entity_to_string(table: pa.Table) -> str:
    return _records_to_ndjson(table.select(sorted(table.schema.names)).to_pylist())


def test_flatten_white_list_entity(get_full_path, compare_result):
    """Entity-level: two independent branches retained with all descendants."""
    flattener = CustomFlattener()
    data = _table_from_json_path(get_full_path("snake", "original"))
    results = flattener.flatten(
        data,
        primary_key="id",
        partition_keys=["date"],
        white_list=["orders.items", "orders.client.addresses"],
    )
    assert set(results.keys()) == {
        "data.orders.items",
        "data.orders.items.attributes",
        "data.orders.client.addresses",
    }
    for entity, result in results.items():
        string_result = _entity_to_string(result)
        assert compare_result(string_result, get_full_path("snake", entity))


def test_flatten_white_list_column(get_full_path, compare_result):
    """Column-level: multiple columns on same child entity, plus a root column."""
    flattener = CustomFlattener()
    data = _table_from_json_path(get_full_path("snake", "original"))
    results = flattener.flatten(
        data,
        primary_key="id",
        partition_keys=["date"],
        white_list=["orders.items.name", "orders.items.price", "summary.total_revenue"],
    )
    assert set(results.keys()) == {"data", "data.orders.items"}
    for entity, result in results.items():
        string_result = _entity_to_string(result)
        assert compare_result(string_result, get_full_path("white_list", entity))
