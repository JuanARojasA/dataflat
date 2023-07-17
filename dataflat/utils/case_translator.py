'''
dataflat/utils/case_translator.py - The case translator script for strings

Copyright (C) 2023 Juan ROJAS
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Authors:
    Juan ROJAS <jarojasa97@gmail.com>
'''

import enum
import re
from dataflat.commons import init_logger
from typeguard import typechecked
from typing import List

logger = init_logger(__name__)

class CaseTranslatorOptions(enum.Enum): # Possible values
    SNAKE_CASE = 1
    KEBAB_CASE = 2
    CAMEL_CASE = 3
    PASCAL_CASE = 4
    LOWER_CASE = 5
    HUMAN_READABLE = 6

@typechecked
class CustomCaseTranslator():
    def __init__(self, from_case:str, to_case:str, split_string:str=""):
        logger.info(f"CustomCaseTranslator for {from_case} to {to_case} has been initiated")
        self.from_case = from_case
        self.to_case = to_case
        self.split_string = split_string

    def _pre_process_string(self, string:str) -> str:
        """Receive an input string in camel or pascal case and process it
        to return a character splitted string.

        Parameters
        ----------
        string: str

        Returns
        -------
        conv_string: str
        """
        conv_string = re.sub('(.)([A-Z][a-z]+)', r'\1 \2', re.sub(r'\W+', "", string))
        conv_string = re.sub('(.)([0-9]+)', r'\1 \2', conv_string)
        conv_string = re.sub('([a-z0-9])([A-Z])', r'\1 \2', conv_string)
        return conv_string

    def _normalize(self, string:str) -> List[str]:
        """Receive a splittable string, removes all non alphanumerical or underscore
        characters, and return a list of the proccesed words in the string

        Parameters
        ----------
        string: str
        
        Returns
        -------
        string: str
        """
        replaced_strings = [re.sub(r'\W+', "", sub_string) for sub_string in string.split(self.split_string)]
        return [replaced_string.lower() for replaced_string in replaced_strings if replaced_string!=""]

    def _kebab_case(self, string:str) -> str:
        """Receive a string and convert it to kebab-case

        Parameters
        ----------
        string: str
        
        Returns
        -------
        string: str
        """
        return "-".join(self._normalize(string))

    def _snake_case(self, string:str) -> str:
        """Receive a string and convert it to snake_case

        Parameters
        ----------
        string: str
        
        Returns
        -------
        string: str
        """
        return "_".join(self._normalize(string))

    def _camel_case(self, string:str) -> str:
        """Receive a string and convert it to camelCase

        Parameters
        ----------
        string: str
        
        Returns
        -------
        string: str
        """
        return "".join([word.capitalize() if index>0 else word for index, word in enumerate(self._normalize(string))])

    def _pascal_case(self, string:str) -> str:
        """Receive a string and convert it to PascalCase

        Parameters
        ----------
        string: str
        
        Returns
        -------
        string: str
        """
        return "".join([word.capitalize() for word in self._normalize(string)])
    
    def _lower_case (self, string:str) -> str:
        """Receive a string and convert it to lowercase

        Parameters
        ----------
        string: str
        
        Returns
        -------
        string: str
        """
        return "".join(self._normalize(string)).lower()

    def _human_readable (self, string:str) -> str:
        """Receive a string and convert it to Human readable

        Parameters
        ----------
        string: str
        
        Returns
        -------
        string: str
        """
        return " ".join([word if index>0 else word.capitalize() for index, word in enumerate(self._normalize(string))])
    
    def translate(self, string:str) -> str:
        """Receive a string and convert it to the
        desirable case.

        Parameters
        ----------
        string: str
        
        Returns
        -------
        string: str
        """
        if self.from_case in ('camel_case','pascal_case'):
            string = self._pre_process_string(string)
        return getattr(self, f"_{self.to_case}")(string)