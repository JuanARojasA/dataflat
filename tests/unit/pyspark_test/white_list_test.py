# pylint: disable=duplicate-code
import json

import pytest
from pyspark.sql import SparkSession

from dataflat.pyspark.flattener import CustomFlattener


@pytest.mark.slow
def test_flatten_white_list_entity(
    spark: SparkSession, get_full_path, compare_result, ignore_null_keys
):
    """Entity-level: two independent branches retained with all descendants."""
    flattener = CustomFlattener()
    data = spark.read.json(get_full_path("snake", "original"))
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
        sorted_columns = [f"`{col}`" for col in sorted(result.columns)]
        string_result = (
            result.select(sorted_columns).toPandas().to_json(orient="records")
        )
        filtered_json = "\n".join(
            json.dumps(ignore_null_keys(record), separators=(",", ":"))
            for record in json.loads(string_result)
        )
        assert compare_result(filtered_json, get_full_path("snake", entity))


@pytest.mark.slow
def test_flatten_white_list_column(
    spark: SparkSession, get_full_path, compare_result, ignore_null_keys
):
    """Column-level: multiple columns on same child entity, plus a root column."""
    flattener = CustomFlattener()
    data = spark.read.json(get_full_path("snake", "original"))
    results = flattener.flatten(
        data,
        primary_key="id",
        partition_keys=["date"],
        white_list=["orders.items.name", "orders.items.price", "summary.total_revenue"],
    )
    assert set(results.keys()) == {"data", "data.orders.items"}
    for entity, result in results.items():
        sorted_columns = [f"`{col}`" for col in sorted(result.columns)]
        string_result = (
            result.select(sorted_columns).toPandas().to_json(orient="records")
        )
        filtered_json = "\n".join(
            json.dumps(ignore_null_keys(record), separators=(",", ":"))
            for record in json.loads(string_result)
        )
        assert compare_result(filtered_json, get_full_path("white_list", entity))
