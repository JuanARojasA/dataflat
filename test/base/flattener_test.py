from dataflat.base.flattener import BaseFlattener
from dataflat.utils.case_translator import CaseTranslatorOptions


def test_flattener():
    base = BaseFlattener()
    assert base.case_translator is None
    assert base.replace_string == "."
    assert base.entity_name == "data"
    assert base.primary_key == "id"


def test_flattener_with_case_translator(get_custom_flattener):
    from_case = CaseTranslatorOptions.CAMEL_CASE
    to_case = CaseTranslatorOptions.SNAKE_CASE
    base: BaseFlattener = get_custom_flattener(BaseFlattener, from_case, to_case)
    assert base.case_translator is not None
    assert base.case_translator.from_case == from_case
    assert base.case_translator.to_case == to_case


def test_flattener_with_parameters():
    base = BaseFlattener(replace_string=",")
    assert base.replace_string == ","


def test_flattener_with_entity_name():
    entity_name = "test"
    base = BaseFlattener(entity_name=entity_name)
    assert base.entity_name == entity_name


def test_flattener_with_primary_key():
    primary_key = "test"
    base = BaseFlattener(primary_key=primary_key)
    assert base.primary_key == primary_key
