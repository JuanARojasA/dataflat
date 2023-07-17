'''
dataflat/flattener_handler.py - a module handler for dataflat lib

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
from importlib import import_module
from dataflat.commons import init_logger
from typing import Literal
from typeguard import typechecked
from utils.case_translator import CustomCaseTranslator

logger = init_logger(__name__)

class FlattenerOptions(enum.Enum): # Possible values
    DICTIONARY = 1
    PANDAS_DF = 2
    SPARK_DF = 3

class CaseTranslatorOptions(enum.Enum): # Possible values
    SNAKE_CASE = 1
    KEBAB_CASE = 2
    CAMEL_CASE = 3
    PASCAL_CASE = 4
    LOWER_CASE = 5
    HUMAN_READABLE = 6

@typechecked
class Flattener():
    def __init__(self):
        logger.info("Flattener Handler initiated")

    def handler(self, custom_flattener: FlattenerOptions,
                from_case: CaseTranslatorOptions=CaseTranslatorOptions.CAMEL_CASE,
                to_case:CaseTranslatorOptions=CaseTranslatorOptions.CAMEL_CASE,
                replace_dots:bool=False):
        """Returns relevant flattener

        Parameters
        ----------
        custom_flattener: FlattenerOptions
            Specify the Flattener class to use.
        from_case: CaseTranslatorOptions
            The original case of the key names in dictionary
        from_case: CaseTranslatorOptions
            The destination case of the key names in dictionary
        replace_dots: bool
            Sepcify if all the dots (used as nested dictionary separator) on processed dictionary
            will be replaced with underscores
        Returns
        -------
        class -- CustomFlattener object 
        """
        split_string = " " if from_case.name in ("CAMEL_CASE","PASCAL_CASE","HUMAN_CASE") else "_" if from_case.name == "SNAKE_CASE" else "-"
        flattener = "dataflat.{}.flattener".format(custom_flattener.name.lower())

        return getattr(import_module(flattener), 'CustomFlattener')(CustomCaseTranslator(from_case.name.lower(), to_case.name.lower(), split_string), replace_dots)