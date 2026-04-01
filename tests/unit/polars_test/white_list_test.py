# pylint: disable=duplicate-code
import polars as pl

from dataflat.polars.flattener import CustomFlattener
from tests.unit.conftest import _records_to_ndjson


def _df_from_ndjson_path(path: str) -> pl.DataFrame:
    return pl.read_ndjson(path)


def _entity_to_string(df: pl.DataFrame) -> str:
    return _records_to_ndjson(df.select(sorted(df.columns)).to_dicts())


def test_flatten_white_list_entity(get_full_path, compare_result):
    """Entity-level: two independent branches retained with all descendants."""
    flattener = CustomFlattener()
    data = _df_from_ndjson_path(get_full_path("snake", "original"))
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
    data = _df_from_ndjson_path(get_full_path("snake", "original"))
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
