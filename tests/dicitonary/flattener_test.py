import pytest
import json

from dataflat.dictionary.flattener import CustomFlattener
from dataflat.utils.case_translator import CaseTranslatorOptions, CustomCaseTranslator


def test_flattener():
    base = CustomFlattener()
    assert base.case_translator is None
    assert base.replace_string == "."
    assert base.entity_name == "data"
    assert base.primary_key == "id"


@pytest.mark.slow
def test_flatten_camel_to_snake(get_custom_flattener, get_full_path, compare_result):
    from_case = CaseTranslatorOptions.CAMEL
    to_case = CaseTranslatorOptions.SNAKE
    flattener: CustomFlattener = get_custom_flattener(
        CustomFlattener, from_case, to_case
    )
    with open(get_full_path(from_case.name.lower(), "original")) as f:
        data = json.load(f)
    results = flattener.flatten(data, partition_keys=["date"])

    for entity, result in results.items():
        string_result = "\n".join(json.dumps(item) for item in result)
        assert compare_result(
            string_result, get_full_path(to_case.name.lower(), entity)
        )


@pytest.mark.slow
def test_flatten_snake_to_camel(get_custom_flattener, get_full_path, compare_result):
    from_case = CaseTranslatorOptions.SNAKE
    to_case = CaseTranslatorOptions.CAMEL
    flattener: CustomFlattener = get_custom_flattener(
        CustomFlattener, from_case, to_case
    )
    with open(get_full_path(from_case.name.lower(), "original")) as f:
        data = json.load(f)
    results = flattener.flatten(data, partition_keys=["date"])

    for entity, result in results.items():
        string_result = "\n".join(json.dumps(item) for item in result)
        assert compare_result(
            string_result, get_full_path(to_case.name.lower(), entity)
        )


@pytest.mark.slow
def test_flatten_black_list(get_custom_flattener, get_full_path, compare_result):
    from_case = CaseTranslatorOptions.SNAKE
    to_case = CaseTranslatorOptions.SNAKE
    flattener: CustomFlattener = get_custom_flattener(
        CustomFlattener, from_case, to_case
    )
    with open(get_full_path("black_list", "original")) as f:
        data = json.load(f)
    results = flattener.flatten(
        data, black_list=["total_orders", "summary.total_clients"]
    )

    for entity, result in results.items():
        string_result = "\n".join(json.dumps(item) for item in result)
        assert compare_result(string_result, get_full_path("black_list", entity))
