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

from dataflat.utils.case_translator import CaseTranslatorOptions, CustomCaseTranslator
from dataflat.utils.string import dot_join_args


@dataclass
class BaseFlattener(ABC):
    case_translator: Optional[CustomCaseTranslator] = None
    entity_name: str = "data"
    primary_key: Optional[str] = None
    partition_keys: list[str] = field(default_factory=list)
    black_list: list[str] = field(default_factory=list)
    white_list: list[str] = field(default_factory=list)

    @property
    def _dataflat_id_col_name(self) -> str:
        if self.case_translator is None:
            return "dataflat_id_column"
        to_case = self.case_translator.to_case
        if to_case == CaseTranslatorOptions.KEBAB:
            return "dataflat-id-column"
        if to_case == CaseTranslatorOptions.CAMEL:
            return "dataflatIdColumn"
        if to_case == CaseTranslatorOptions.PASCAL:
            return "DataflatIdColumn"
        if to_case == CaseTranslatorOptions.HUMAN:
            return "Dataflat id column"
        if to_case == CaseTranslatorOptions.LOWER:
            return "dataflatidcolumn"
        return "dataflat_id_column"  # SNAKE or unknown

    def _setup(
        self,
        primary_key: Optional[str] = None,
        entity_name: Optional[str] = None,
        partition_keys: Optional[list[str]] = None,
        black_list: Optional[list[str]] = None,
        white_list: Optional[list[str]] = None,
    ) -> None:
        self.primary_key = primary_key
        self.entity_name = entity_name if entity_name else self.entity_name
        self.partition_keys = partition_keys if partition_keys is not None else []
        self.black_list = black_list if black_list is not None else []
        self.white_list = white_list if white_list is not None else []

    def _is_blacklisted(self, entity_name: str, col: str) -> bool:
        return any(
            dot_join_args(entity_name, col).endswith(item) for item in self.black_list
        )

    def _process_strings(self, string: str) -> str:
        if self.case_translator is not None:
            return ".".join(
                self.case_translator.translate(sub_string)
                for sub_string in string.split(".")
                if sub_string != ""
            )
        return string

    def _add_entity_level_entry(
        self,
        full_wl: str,
        result_keys: list[str],
        plan: dict[str, Optional[set[str]]],
    ) -> None:
        for key in result_keys:
            if key == full_wl or key.startswith(full_wl + "."):
                plan[key] = None  # entity-level overrides any column-level entry

    def _add_column_level_entry(
        self,
        full_wl: str,
        result_keys: list[str],
        entity_inherited_columns: dict[str, list[str]],
        plan: dict[str, Optional[set[str]]],
    ) -> None:
        parent_entity = max(
            (key for key in result_keys if full_wl.startswith(key + ".")),
            key=len,
            default=None,
        )
        if parent_entity is None:
            return
        col_name = full_wl[len(parent_entity) + 1 :]
        inherited = entity_inherited_columns.get(parent_entity, [])
        if parent_entity not in plan:
            plan[parent_entity] = set(inherited) | {col_name}
        elif (existing := plan[parent_entity]) is not None:
            existing.add(col_name)
        # If plan[parent_entity] is None (entity-level), leave it as None.

    def _compute_white_list_plan(
        self,
        result_keys: list[str],
        entity_inherited_columns: dict[str, list[str]],
    ) -> dict[str, Optional[set[str]]]:
        if not self.white_list:
            return dict.fromkeys(result_keys)

        plan: dict[str, Optional[set[str]]] = {}
        for wl in self.white_list:
            full_wl = f"{self.entity_name}.{wl}"
            if full_wl in result_keys:
                self._add_entity_level_entry(full_wl, result_keys, plan)
            else:
                self._add_column_level_entry(full_wl, result_keys, entity_inherited_columns, plan)
        return plan

    def _apply_white_list(self) -> None: ...

    def _apply_column_translate(self) -> None: ...

    def flatten(
        self,
        data: Any,
        primary_key: Optional[str] = None,
        entity_name: Optional[str] = None,
        partition_keys: Optional[list[str]] = None,
        black_list: Optional[list[str]] = None,
        white_list: Optional[list[str]] = None,
    ) -> Any:
        return None
