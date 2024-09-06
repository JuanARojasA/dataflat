"""
dataflat/flattener_handler.py - a module handler for dataflat lib

Copyright (C) 2024 Juan ROJAS
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Authors:
    Juan ROJAS <jarojasa97@gmail.com>
"""

import enum
from importlib import import_module

from typeguard import typechecked

from dataflat.commons import init_logger
from dataflat.utils.case_translator import CaseTranslatorOptions, CustomCaseTranslator

logger = init_logger(__name__)


class FlattenerOptions(enum.Enum):
    DICTIONARY = 1
    PYSPARK_DF = 2


@typechecked
def handler(
    custom_flattener: FlattenerOptions,
    from_case: CaseTranslatorOptions = None,
    to_case: CaseTranslatorOptions = None,
    replace_string: str = ".",
    remove_special_chars: bool = False,
):
    """Return the selected flattener class from FlattenerOptions

    Parameters
    ----------
    custom_flattener: FlattenerOptions
        Specify the Flattener class to use.
    from_case: CaseTranslatorOptions
        The original case of the key names in dictionary
    to_case: CaseTranslatorOptions
        The destination case of the key names in dictionary
    replace_string: str
        String used to separate the nested keys
    remove_special_chars: bool
        Remove or not special characters on dataframe or column names
    Returns
    -------
    BaseFlattener -- Flattener class "
    """
    flattener = "dataflat.{}.flattener".format(custom_flattener.name.lower())

    if (from_case is None) or (to_case is None):
        logger.warning(
            "One or both parameters (from_case,to_case) are None, no translation will be applied."
        )
        case_translator = None
    elif from_case.name == to_case.name:
        logger.warning(
            "from_case and to_case are the same, no translation will be applied."
        )
        case_translator = None
    elif from_case.name == "LOWER":
        logger.warning(
            f"Is impossible to translate from LOWER to {to_case.name}, no translation will be applied."
        )
        case_translator = None
    else:
        case_translator = CustomCaseTranslator(from_case, to_case, remove_special_chars)

    return getattr(import_module(flattener), "CustomFlattener")(
        case_translator=case_translator, replace_string=replace_string
    )
