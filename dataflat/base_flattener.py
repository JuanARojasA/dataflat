"""
dataflat/base_flattener.py - The Base class for the dataflat library

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

from abc import ABC
from dataclasses import dataclass, field
from typing import Any, Optional

from dataflat.utils.case_translator import CustomCaseTranslator


@dataclass
class BaseFlattener(ABC):
    case_translator: Optional[CustomCaseTranslator] = None
    replace_string: str = "."
    entity_name: str = "data"
    primary_key: str = "id"
    partition_keys: list[str] = field(default_factory=list)
    black_list: list[str] = field(default_factory=list)

    def _process_strings(self, string: str) -> str:
        """Translate case and replace separator dots in a dot-joined key string."""
        if self.case_translator is not None:
            return self.replace_string.join(
                [
                    self.case_translator.translate(sub_string)
                    for sub_string in string.split(".")
                    if string != ""
                ]
            )
        return self.replace_string.join(string.split("."))

    def flatten(
        self,
        data: Any,
        primary_key: Optional[str] = None,
        entity_name: Optional[str] = None,
        partition_keys: Optional[list[str]] = None,
        black_list: Optional[list[str]] = None,
    ) -> Any:
        return None
