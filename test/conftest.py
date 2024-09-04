import os

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
    def _get_full_path(case: str, entity_name: str) -> str:
        base_dir = os.path.join(os.path.dirname(__file__), "resources", case)
        return os.path.join(base_dir, f"{entity_name}.json")

    return _get_full_path
