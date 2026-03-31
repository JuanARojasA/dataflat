import itertools

import pytest

from dataflat.utils.case_translator import CaseTranslatorOptions, CustomCaseTranslator

# ──────────────────────────────────────────────────────────────────────────────
# Truth tables
#
# FROM_CASE_INPUT  – canonical two-word input for every from_case that
#                    normalises to the token list ["hello", "world"].
#
# TO_CASE_EXPECTED – expected output for ["hello", "world"] in every to_case.
#
# LOWER as a from_case uses split_string=" ", so the only multi-word form that
# can be correctly split is space-separated ("hello world").
# ──────────────────────────────────────────────────────────────────────────────
FROM_CASE_INPUT: dict[CaseTranslatorOptions, str] = {
    CaseTranslatorOptions.SNAKE: "hello_world",
    CaseTranslatorOptions.KEBAB: "hello-world",
    CaseTranslatorOptions.CAMEL: "helloWorld",
    CaseTranslatorOptions.PASCAL: "HelloWorld",
    CaseTranslatorOptions.HUMAN: "Hello world",
    CaseTranslatorOptions.LOWER: "hello world",
}

TO_CASE_EXPECTED: dict[CaseTranslatorOptions, str] = {
    CaseTranslatorOptions.SNAKE: "hello_world",
    CaseTranslatorOptions.KEBAB: "hello-world",
    CaseTranslatorOptions.CAMEL: "helloWorld",
    CaseTranslatorOptions.PASCAL: "HelloWorld",
    CaseTranslatorOptions.HUMAN: "Hello world",
    CaseTranslatorOptions.LOWER: "helloworld",
}

ALL_COMBINATIONS = [
    pytest.param(
        from_case,
        FROM_CASE_INPUT[from_case],
        to_case,
        TO_CASE_EXPECTED[to_case],
        id=f"{from_case.name}_to_{to_case.name}",
    )
    for from_case, to_case in itertools.product(
        CaseTranslatorOptions, CaseTranslatorOptions
    )
]


class TestCustomCaseTranslatorInit:
    def test_fields_are_set(self):
        translator = CustomCaseTranslator(
            from_case=CaseTranslatorOptions.CAMEL,
            to_case=CaseTranslatorOptions.SNAKE,
            remove_special_chars=False,
        )
        assert translator.from_case == CaseTranslatorOptions.CAMEL
        assert translator.to_case == CaseTranslatorOptions.SNAKE
        assert translator.remove_special_chars is False

    def test_pydantic_validation_rejects_wrong_type(self):
        with pytest.raises(Exception):
            CustomCaseTranslator(
                from_case="not_a_valid_case",  # type: ignore[arg-type]
                to_case=CaseTranslatorOptions.SNAKE,
                remove_special_chars=False,
            )

    def test_all_enum_options_instantiate(self):
        for case in CaseTranslatorOptions:
            translator = CustomCaseTranslator(
                from_case=case,
                to_case=case,
                remove_special_chars=False,
            )
            assert translator.from_case == case


class TestCustomCaseTranslatorAllCombinations:
    @pytest.mark.parametrize("from_case,input_str,to_case,expected", ALL_COMBINATIONS)
    def test_translate(self, from_case, input_str, to_case, expected):
        translator = CustomCaseTranslator(
            from_case=from_case,
            to_case=to_case,
            remove_special_chars=False,
        )
        assert translator.translate(input_str) == expected


class TestCustomCaseTranslatorEdgeCases:
    # ── single word ──────────────────────────────────────────────────────────

    @pytest.mark.parametrize(
        "to_case,expected",
        [
            (CaseTranslatorOptions.SNAKE, "hello"),
            (CaseTranslatorOptions.KEBAB, "hello"),
            (CaseTranslatorOptions.CAMEL, "hello"),
            (CaseTranslatorOptions.PASCAL, "Hello"),
            (CaseTranslatorOptions.HUMAN, "Hello"),
            (CaseTranslatorOptions.LOWER, "hello"),
        ],
    )
    def test_single_word(self, to_case, expected):
        translator = CustomCaseTranslator(
            from_case=CaseTranslatorOptions.SNAKE,
            to_case=to_case,
            remove_special_chars=False,
        )
        assert translator.translate("hello") == expected

    # ── three words ──────────────────────────────────────────────────────────

    @pytest.mark.parametrize(
        "from_case,input_str",
        [
            (CaseTranslatorOptions.SNAKE, "foo_bar_baz"),
            (CaseTranslatorOptions.KEBAB, "foo-bar-baz"),
            (CaseTranslatorOptions.CAMEL, "fooBarBaz"),
            (CaseTranslatorOptions.PASCAL, "FooBarBaz"),
            (CaseTranslatorOptions.HUMAN, "Foo bar baz"),
            (CaseTranslatorOptions.LOWER, "foo bar baz"),
        ],
    )
    def test_three_words_to_snake(self, from_case, input_str):
        translator = CustomCaseTranslator(
            from_case=from_case,
            to_case=CaseTranslatorOptions.SNAKE,
            remove_special_chars=False,
        )
        assert translator.translate(input_str) == "foo_bar_baz"

    # ── numbers ──────────────────────────────────────────────────────────────

    def test_camel_digit_between_words(self):
        """hello2World → pre-process splits digit boundaries → hello_2_world"""
        translator = CustomCaseTranslator(
            from_case=CaseTranslatorOptions.CAMEL,
            to_case=CaseTranslatorOptions.SNAKE,
            remove_special_chars=False,
        )
        assert translator.translate("hello2World") == "hello_2_world"

    def test_snake_digit_token(self):
        """Digit tokens in snake_case are preserved as-is."""
        translator = CustomCaseTranslator(
            from_case=CaseTranslatorOptions.SNAKE,
            to_case=CaseTranslatorOptions.CAMEL,
            remove_special_chars=False,
        )
        assert translator.translate("hello_2_world") == "hello2World"

    # ── acronyms ─────────────────────────────────────────────────────────────

    def test_pascal_acronym_prefix(self):
        """XMLParser → xml_parser — consecutive caps followed by mixed case."""
        translator = CustomCaseTranslator(
            from_case=CaseTranslatorOptions.PASCAL,
            to_case=CaseTranslatorOptions.SNAKE,
            remove_special_chars=False,
        )
        assert translator.translate("XMLParser") == "xml_parser"

    def test_pascal_acronym_suffix(self):
        """ParseXML — trailing acronym is kept as a single token."""
        translator = CustomCaseTranslator(
            from_case=CaseTranslatorOptions.PASCAL,
            to_case=CaseTranslatorOptions.SNAKE,
            remove_special_chars=False,
        )
        assert translator.translate("ParseXML") == "parse_xml"

    # ── special characters ───────────────────────────────────────────────────

    def test_remove_special_chars_true_strips_before_split(self):
        translator = CustomCaseTranslator(
            from_case=CaseTranslatorOptions.SNAKE,
            to_case=CaseTranslatorOptions.SNAKE,
            remove_special_chars=True,
        )
        assert translator.translate("hello_world!") == "hello_world"

    def test_remove_special_chars_false_preserves_token(self):
        """With remove_special_chars=False, '!' survives as part of the last token."""
        translator = CustomCaseTranslator(
            from_case=CaseTranslatorOptions.SNAKE,
            to_case=CaseTranslatorOptions.SNAKE,
            remove_special_chars=False,
        )
        assert translator.translate("hello_world!") == "hello_world!"

    def test_camel_special_char_spacing(self):
        """From CAMEL with remove_special_chars=False: dots get spaces around them."""
        translator = CustomCaseTranslator(
            from_case=CaseTranslatorOptions.CAMEL,
            to_case=CaseTranslatorOptions.SNAKE,
            remove_special_chars=False,
        )
        # "hello.World" → pre-process adds spaces → "hello . World" → normalize → ["hello", ".", "world"]
        assert translator.translate("hello.World") == "hello_._world"

    def test_remove_special_chars_true_camel(self):
        translator = CustomCaseTranslator(
            from_case=CaseTranslatorOptions.CAMEL,
            to_case=CaseTranslatorOptions.SNAKE,
            remove_special_chars=True,
        )
        # Strips non-word chars first, then splits camel
        assert translator.translate("hello.World") == "hello_world"
