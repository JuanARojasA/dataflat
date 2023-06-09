'''
flattener_handler.py - a module handler for pyflat lib

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

logger = init_logger(__name__)

class Options(enum.Enum): # Possible values
    DICTIONARY = 1
    PANDAS_DF = 2
    SPARK_DF = 3

@typechecked
class Flattener():
    def __init__(self):
        logger.info("Flattener Handler initiated")

    def handler(self, custom_flattener: Literal[Options.DICTIONARY, Options.PANDAS_DF, Options.SPARK_DF]):
        """Returns relevant flattener

        Parameters
        ----------
        custom_flattener: Type[Options]
            Specify the Flattener class to use.

        Returns
        -------
        class -- CustomFlattener object 
        """
        flattener = "dataflat.{}.flattener".format(custom_flattener.name.lower())

        return getattr(import_module(flattener), 'CustomFlattener')()