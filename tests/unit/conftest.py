import hashlib
import json
import os
import random
from typing import Any

import pytest
import yaml
from faker import Faker
from pytest import fixture

from dataflat.utils.case_translator import CaseTranslatorOptions, CustomCaseTranslator


def _records_to_ndjson(records: list[dict]) -> str:
    """Serialise a list of records to a sorted, null-free NDJSON string."""
    return "\n".join(
        json.dumps(
            {k: v for k, v in sorted(row.items()) if v is not None},
            separators=(",", ":"),
        )
        for row in records
    )


def _records_to_ndjson_pandas(records: list[dict]) -> str:
    import pandas as pd

    return "\n".join(
        json.dumps(
            {k: v for k, v in sorted(row.items()) if v is not None and v is not pd.NA},
            separators=(",", ":"),
        )
        for row in records
    )


_fake = Faker()
_SCHEMA_PATH = os.path.join(
    os.path.dirname(__file__), "../resources", "order_schema.yaml"
)


def _generate(node: dict, seq_index: int = 0) -> Any:
    t = node["type"]

    if t == "faker":
        method = getattr(_fake, node["method"])
        result = method(*node.get("args", []), **node.get("kwargs", {}))
        return (
            str(result)
            if hasattr(result, "date") or type(result).__name__ == "Decimal"
            else result
        )

    if t == "choice":
        return random.choice(node["options"])

    if t == "dict":
        result: dict[str, Any] = {}
        deferred: dict[str, Any] = {}
        for key, field in node["fields"].items():
            if field["type"] == "ref_count":
                deferred[key] = field
            else:
                result[key] = _generate(field, seq_index)
        for key, field in deferred.items():
            result[key] = len(result[field["ref"]])
        return result

    if t == "list":
        size = random.randint(node["min"], node["max"])
        return [_generate(node["items"], i) for i in range(size)]

    if t == "nullable":
        if random.random() < node.get("nullable_chance", 0.5):
            return None
        return _generate(node["inner"], seq_index)

    if t == "sequence":
        return seq_index + 1

    return None


@fixture(scope="session")
def nested_order_data():
    with open(_SCHEMA_PATH) as f:
        schema = yaml.safe_load(f)
    return [_generate(schema) for _ in range(10)]


@fixture(scope="function")
def get_custom_flattener():
    def _get_custom_flattener(
        flattener_class,
        from_case: CaseTranslatorOptions,
        to_case: CaseTranslatorOptions,
    ):
        case_translator = CustomCaseTranslator(
            from_case=from_case,
            to_case=to_case,
            remove_special_chars=False,
        )
        return flattener_class(case_translator=case_translator)

    return _get_custom_flattener


@fixture(scope="function")
def get_full_path():
    def _get_full_path(case: str, entity: str) -> str:
        file_path = os.path.join(
            os.path.dirname(__file__), "../resources", case, f"{entity}.json"
        )
        return file_path

    return _get_full_path


@fixture(scope="function")
def compare_result():
    def _compare_result(result: str, expected_result_filepath: str):
        result_md5 = hashlib.sha256(result.encode("utf-8")).hexdigest()
        with open(expected_result_filepath, "rb") as f:
            content = f.read().replace(b"\r\n", b"\n")
            expected_result_md5 = hashlib.sha256(content).hexdigest()
        return result_md5 == expected_result_md5

    return _compare_result


@fixture(scope="function")
def ignore_null_keys():
    def _ignore_null_keys(d):
        return {k: v for k, v in d.items() if v is not None}

    return _ignore_null_keys


@fixture(scope="session")
def spark():
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        pytest.skip("PySpark is not installed — skipping PySpark tests")

    spark = SparkSession.builder.appName("TestClient").master("local[*]").getOrCreate()
    yield spark
    spark.stop()
