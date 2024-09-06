import json

import pytest
from pyspark.sql import SparkSession

from dataflat.pyspark_df.flattener import CustomFlattener
from dataflat.utils.case_translator import CaseTranslatorOptions


@pytest.mark.slow
def test_flattener():
    base = CustomFlattener()
    assert base.case_translator is None
    assert base.replace_string == "."
    assert base.entity_name == "data"
    assert base.primary_key == "id"


@pytest.mark.slow
def test_flatten_camel_to_snake(
    spark: SparkSession,
    get_custom_flattener,
    get_full_path,
    compare_result,
    ignore_null_keys,
):
    from_case = CaseTranslatorOptions.CAMEL
    to_case = CaseTranslatorOptions.SNAKE
    flattener: CustomFlattener = get_custom_flattener(
        CustomFlattener, from_case, to_case
    )
    data = spark.read.json(get_full_path(from_case.name.lower(), "original"))
    results = flattener.flatten(data, partition_keys=["date"])

    for entity, result in results.items():
        sorted_columns = [f"`{col}`" for col in sorted(result.columns)]
        string_result = (
            result.select(sorted_columns).toPandas().to_json(orient="records")
        )
        filtered_json = "\n".join(
            [
                json.dumps(ignore_null_keys(record), separators=(",", ":"))
                for record in json.loads(string_result)
            ]
        )
        assert compare_result(
            filtered_json, get_full_path(to_case.name.lower(), entity)
        )


@pytest.mark.slow
def test_flatten_snake_to_camel(
    spark, get_custom_flattener, get_full_path, compare_result, ignore_null_keys
):
    from_case = CaseTranslatorOptions.SNAKE
    to_case = CaseTranslatorOptions.CAMEL
    flattener: CustomFlattener = get_custom_flattener(
        CustomFlattener, from_case, to_case
    )
    data = spark.read.json(get_full_path(from_case.name.lower(), "original"))
    results = flattener.flatten(data, partition_keys=["date"])

    for entity, result in results.items():
        sorted_columns = [f"`{col}`" for col in sorted(result.columns)]
        string_result = (
            result.select(sorted_columns).toPandas().to_json(orient="records")
        )
        filtered_json = "\n".join(
            [
                json.dumps(ignore_null_keys(record), separators=(",", ":"))
                for record in json.loads(string_result)
            ]
        )
        assert compare_result(
            filtered_json, get_full_path(to_case.name.lower(), entity)
        )


@pytest.mark.slow
def test_flatten_black_list(
    spark, get_custom_flattener, get_full_path, compare_result, ignore_null_keys
):
    from_case = CaseTranslatorOptions.SNAKE
    to_case = CaseTranslatorOptions.SNAKE
    flattener: CustomFlattener = get_custom_flattener(
        CustomFlattener, from_case, to_case
    )
    data = spark.read.json(get_full_path("black_list", "original"))
    results = flattener.flatten(
        data, black_list=["total_orders", "summary.total_clients"]
    )

    for entity, result in results.items():
        sorted_columns = [f"`{col}`" for col in sorted(result.columns)]
        string_result = (
            result.select(sorted_columns).toPandas().to_json(orient="records")
        )
        filtered_json = "\n".join(
            [
                json.dumps(ignore_null_keys(record), separators=(",", ":"))
                for record in json.loads(string_result)
            ]
        )
        assert compare_result(filtered_json, get_full_path("black_list", entity))
