# pylint: disable=duplicate-code
import json
from collections import OrderedDict

from dataflat.dictionary.flattener import CustomFlattener


def test_flatten_white_list_entity(get_full_path, compare_result):
    """Entity-level: two independent branches retained with all descendants."""
    flattener = CustomFlattener()
    with open(get_full_path("snake", "original")) as f:
        data = json.load(f)
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
        string_result = "\n".join(
            json.dumps(OrderedDict(sorted(item.items())), separators=(",", ":"))
            for item in result
        )
        assert compare_result(string_result, get_full_path("snake", entity))


def test_flatten_white_list_column(get_full_path, compare_result):
    """Column-level: multiple columns on same child entity, plus a root column."""
    flattener = CustomFlattener()
    with open(get_full_path("snake", "original")) as f:
        data = json.load(f)
    results = flattener.flatten(
        data,
        primary_key="id",
        partition_keys=["date"],
        white_list=["orders.items.name", "orders.items.price", "summary.total_revenue"],
    )
    assert set(results.keys()) == {"data", "data.orders.items"}
    for entity, result in results.items():
        string_result = "\n".join(
            json.dumps(OrderedDict(sorted(item.items())), separators=(",", ":"))
            for item in result
        )
        assert compare_result(string_result, get_full_path("white_list", entity))
