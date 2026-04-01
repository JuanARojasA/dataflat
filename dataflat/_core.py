"""
dataflat/_core.py - Entry point: FlattenerOptions enum, handler function, and FlatteningException.

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
from typing import Optional

from pydantic import validate_call

from dataflat.base_flattener import BaseFlattener
from dataflat.utils.case_translator import CaseTranslatorOptions, CustomCaseTranslator
from dataflat.utils.logger import init_logger

logger = init_logger(__name__)


class FlatteningException(Exception):
    """Generic exception raised for errors during the dataflat flattening process.

    Parameters
    ----------
    message : str
        Explanation of the error.
    """

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class FlattenerOptions(enum.Enum):
    DICTIONARY = 1
    PYSPARK_DF = 2
    POLARS_DF = 3
    PYARROW_TABLE = 4
    PANDAS_DF = 5


def _get_flattener_class(option: FlattenerOptions) -> type[BaseFlattener]:
    """Return the concrete CustomFlattener class for the given option.

    Imports are deferred so optional dependencies (pyspark, polars, pyarrow)
    are only loaded when the corresponding flattener is actually requested.
    """
    if option == FlattenerOptions.DICTIONARY:
        from dataflat.dictionary.flattener import CustomFlattener

        return CustomFlattener
    if option == FlattenerOptions.POLARS_DF:
        from dataflat.polars.flattener import CustomFlattener

        return CustomFlattener
    if option == FlattenerOptions.PYARROW_TABLE:
        from dataflat.pyarrow.flattener import CustomFlattener

        return CustomFlattener
    if option == FlattenerOptions.PANDAS_DF:
        from dataflat.pandas.flattener import CustomFlattener

        return CustomFlattener
    # FlattenerOptions.PYSPARK_DF
    from dataflat.pyspark.flattener import CustomFlattener

    return CustomFlattener


@validate_call
def handler(
    custom_flattener: FlattenerOptions,
    from_case: Optional[CaseTranslatorOptions] = None,
    to_case: Optional[CaseTranslatorOptions] = None,
    remove_special_chars: bool = False,
) -> BaseFlattener:
    """Return the selected flattener class from FlattenerOptions

    Parameters
    ----------
    custom_flattener: FlattenerOptions
        Specify the Flattener class to use.
    from_case: CaseTranslatorOptions
        The original case of the key names in dictionary
    to_case: CaseTranslatorOptions
        The destination case of the key names in dictionary
    remove_special_chars: bool
        Remove or not special characters on dataframe or column names
    Returns
    -------
    BaseFlattener -- Flattener class
    """
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
        case_translator = CustomCaseTranslator(
            from_case=from_case,
            to_case=to_case,
            remove_special_chars=remove_special_chars,
        )

    flattener_class = _get_flattener_class(custom_flattener)
    logger.info(f"CustomFlattener for {custom_flattener.name} has been initiated")
    return flattener_class(case_translator=case_translator)
