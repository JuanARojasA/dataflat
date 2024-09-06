import os
import hashlib

from pytest import fixture

from dataflat.utils.case_translator import CaseTranslatorOptions, CustomCaseTranslator


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
        return flattener_class(case_translator, replace_string=".")

    return _get_custom_flattener


@fixture(scope="function")
def get_full_path():
    def _get_full_path(case: str, entity: str) -> str:
        file_path = os.path.join(
            os.path.dirname(__file__), "resources", case, f"{entity}.json"
        )
        return file_path

    return _get_full_path


@fixture(scope="function")
def compare_result():
    def _compare_result(result: str, expected_result_filepath: str):
        print(result)
        result_md5 = hashlib.md5(result.encode("utf-8")).hexdigest()
        with open(expected_result_filepath, "rb") as f:
            expected_result_md5 = hashlib.md5(f.read()).hexdigest()
        return result_md5 == expected_result_md5

    return _compare_result
