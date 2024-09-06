"""
dataflat/utils/case_translator.py - The case translator script for strings

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
import re
from typing import List

from typeguard import typechecked

from dataflat.commons import init_logger

logger = init_logger(__name__)


class CaseTranslatorOptions(enum.Enum):     # Possible values
    SNAKE = {"id": 1, "split_string": "_"}
    KEBAB = {"id": 2, "split_string": "-"}
    CAMEL = {"id": 3, "split_string": " "}
    PASCAL = {"id": 4, "split_string": " "}
    HUMAN = {"id": 5, "split_string": " "}
    LOWER = {"id": 6, "split_string": " "}


@typechecked
class CustomCaseTranslator:
    def __init__(self, from_case: CaseTranslatorOptions, to_case: CaseTranslatorOptions, remove_special_chars: bool):
        logger.info(f"CustomCaseTranslator for {from_case.name} to {to_case.name} has been initiated")
        self.from_case = from_case
        self.to_case = to_case
        self.remove_special_chars = remove_special_chars

    def _pre_process_string(self, string: str) -> str:
        """Receive an input string in camel or Pascal case and process it
        to return a split string.

        Parameters
        ----------
        string: str

        Returns
        -------
        conv_string: str
        """
        # Step 1: Handle camel case, Pascal case, and mixed alphanumeric patterns
        conv_string = re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', string)  # camelCase to camel Case
        conv_string = re.sub(r'(?<=[A-Z])(?=[A-Z][a-z])', ' ', conv_string)  # PascalCase to Pascal Case
        conv_string = re.sub(r'(?<=[a-z])(?=\d)', ' ', conv_string)  # text123 to text 123
        conv_string = re.sub(r'(?<=\d)(?=[a-zA-Z])', ' ', conv_string)  # 123text to 123 text
        conv_string = re.sub(r'(?<=\d)(?=[A-Z])', ' ', conv_string)  # 123ABC to 123 ABC

        # Handle special characters if `remove_special_chars` is False
        if not self.remove_special_chars:
            # Ensure spaces around special characters
            conv_string = re.sub(r'([^\w\s])', r' \1 ', conv_string)

        # Remove extra spaces around special characters and condense multiple spaces
        conv_string = re.sub(r'\s+', ' ', conv_string).strip()
        return conv_string

    def _normalize(self, string: str) -> List[str]:
        """Receive a splittable string, removes all non-alphanumerical or underscore
        characters, and return a list of the processed words in the string

        Parameters
        ----------
        string: str
        
        Returns
        -------
        string: str
        """
        replaced_strings = string.split(self.from_case.value['split_string'])
        return [replaced_string.lower() for replaced_string in replaced_strings if replaced_string != ""]

    def _kebab(self, string: str) -> str:
        """Receive a string and convert it to kebab-case

        Parameters
        ----------
        string: str
        
        Returns
        -------
        string: str
        """
        return "-".join(self._normalize(string))

    def _snake(self, string: str) -> str:
        """Receive a string and convert it to snake_case

        Parameters
        ----------
        string: str
        
        Returns
        -------
        string: str
        """
        return "_".join(self._normalize(string))

    def _camel(self, string: str) -> str:
        """Receive a string and convert it to camelCase

        Parameters
        ----------
        string: str
        
        Returns
        -------
        string: str
        """
        return "".join([word.capitalize() if index > 0 else word for index, word in enumerate(self._normalize(string))])

    def _pascal(self, string: str) -> str:
        """Receive a string and convert it to PascalCase

        Parameters
        ----------
        string: str
        
        Returns
        -------
        string: str
        """
        return "".join([word.capitalize() for word in self._normalize(string)])
    
    def _lower(self, string: str) -> str:
        """Receive a string and convert it to lowercase

        Parameters
        ----------
        string: str
        
        Returns
        -------
        string: str
        """
        return "".join(self._normalize(string)).lower()

    def _human(self, string: str) -> str:
        """Receive a string and convert it to Human readable

        Parameters
        ----------
        string: str
        
        Returns
        -------
        string: str
        """
        return " ".join(
            [word if index > 0 else word.capitalize() for index, word in enumerate(self._normalize(string))]
        )
    
    def translate(self, string: str) -> str:
        """Receive a string and convert it to the
        desirable case.

        Parameters
        ----------
        string: str
        
        Returns
        -------
        string: str
        """
        if self.remove_special_chars:
            string = re.sub(r'\W+', "", string)
        if self.from_case.name in ('CAMEL', 'PASCAL'):
            string = self._pre_process_string(string)
        return getattr(self, f"_{self.to_case.name.lower()}")(string)
