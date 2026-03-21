"""
dataflat/dictionary/flattener.py - The processor script for dictionaries flattening process

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

import re
import uuid
from collections import defaultdict
from typing import Any, Optional

from typeguard import typechecked

from dataflat.base_flattener import BaseFlattener
from dataflat.utils.logger import init_logger
from dataflat.utils.string import dot_join_args

logger = init_logger(__name__)


@typechecked
class CustomFlattener(BaseFlattener):
    logger.info("CustomFlattener for Python Dictionaries has been initiated")

    # ------------------------------------------------------------------
    # Internal state initialisation
    # ------------------------------------------------------------------

    def _setup(
        self,
        primary_key: Optional[str] = None,
        entity_name: Optional[str] = None,
        partition_keys: Optional[list[str]] = None,
        black_list: Optional[list[str]] = None,
    ) -> None:
        self.primary_key = primary_key
        self.entity_name = entity_name if entity_name else self.entity_name
        self.partition_keys = partition_keys if partition_keys else []
        self.black_list = black_list if black_list is not None else []
        self.__temp_dict: defaultdict[str, dict[str, Any]] = defaultdict(dict)
        self.__flatten_dict: dict[str, list[dict[str, Any]]] = defaultdict(list)

    # ------------------------------------------------------------------
    # Field helpers
    # ------------------------------------------------------------------

    @staticmethod
    def __split_and_dict(string: str) -> dict[str, str]:
        pattern = re.compile(r"(\._\d+_)")
        parts = pattern.split(string)
        result_dict = {
            parts[i - 1].lstrip("."): parts[i][2:-1] for i in range(1, len(parts), 2)
        }
        return result_dict

    def __set_heritable_fields(self, dictionary: dict[str, Any]) -> None:
        # primary_key is always set to a non-None string before this is called.
        assert self.primary_key is not None
        pk = self.primary_key
        self._heritable_fields = {dot_join_args(self.entity_name, pk): dictionary[pk]}
        for partition_key in self.partition_keys:
            self._heritable_fields[dot_join_args(self.entity_name, partition_key)] = (
                dictionary[partition_key]
            )

    # ------------------------------------------------------------------
    # Column-name translation helpers
    # ------------------------------------------------------------------

    def __apply_column_translate(self) -> None:
        if self.case_translator is not None:
            translated: dict[str, list[dict[str, Any]]] = {}
            for entity_name, records in self.__flatten_dict.items():
                new_entity = self._process_strings(entity_name)
                new_records = [
                    {self._process_strings(k): v for k, v in record.items()}
                    for record in records
                ]
                translated[new_entity] = new_records
            self.__flatten_dict = translated

    # ------------------------------------------------------------------
    # Dict traversal
    # ------------------------------------------------------------------

    def __fix_nested_list(self) -> None:
        dict_names = list(self.__temp_dict.keys())
        for dict_name in dict_names:
            index_keys = self.__split_and_dict(dict_name)
            aux = self.__temp_dict.pop(dict_name)
            fixed_dict_name = dict_name
            if index_keys:
                aux.update(self._heritable_fields)
                last_key, last_value = index_keys.popitem()
                trailing_index_key = ""
                for index_key, index_value in index_keys.items():
                    trailing_index_key = dot_join_args(trailing_index_key, index_key)
                    aux[dot_join_args(trailing_index_key, "index")] = int(index_value)
                aux["index"] = int(last_value)
                fixed_dict_name = dot_join_args(trailing_index_key, last_key)
            if fixed_dict_name in self.__flatten_dict:
                self.__flatten_dict[fixed_dict_name].append(aux)
            else:
                self.__flatten_dict[fixed_dict_name] = [aux]

    def __process_list(
        self, key: str, value: list, dict_name: str, schema_ref: str
    ) -> None:
        if isinstance(value[0], dict):
            for index, item in enumerate(value):
                self.__processor(
                    item,
                    dot_join_args(dict_name, schema_ref, key, f"_{str(index)}_"),
                    "",
                )
        else:
            for index, item in enumerate(value):
                dict_item = {key: item}
                self.__temp_dict[
                    dot_join_args(dict_name, schema_ref, key, f"_{str(index)}_")
                ] = dict_item

    def __processor(
        self, dictionary: dict[str, Any], dict_name: str, schema_ref: str
    ) -> None:
        for key, value in dictionary.items():
            if (
                not any(
                    dot_join_args(dict_name, schema_ref, key).endswith(item)
                    for item in self.black_list
                )
                and value is not None
                and value != []
                and value != ""
            ):
                if isinstance(value, dict):
                    self.__processor(value, dict_name, dot_join_args(schema_ref, key))
                elif isinstance(value, list):
                    self.__process_list(key, value, dict_name, schema_ref)
                else:
                    self.__temp_dict[dict_name][dot_join_args(schema_ref, key)] = value

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def flatten(
        self,
        data: dict[str, Any],
        primary_key: Optional[str] = None,
        entity_name: Optional[str] = None,
        partition_keys: Optional[list[str]] = None,
        black_list: Optional[list[str]] = None,
    ) -> dict[str, list[dict[str, Any]]]:
        self._setup(primary_key, entity_name, partition_keys, black_list)
        tmp = data.copy()
        if self.primary_key is None:
            pk_col = self._dataflat_id_col_name
            tmp[pk_col] = str(uuid.uuid4())
            self.primary_key = pk_col
        self.__set_heritable_fields(tmp)
        tmp[dot_join_args(self.entity_name, self.primary_key)] = tmp[self.primary_key]
        for partition_key in self.partition_keys:
            tmp[dot_join_args(self.entity_name, partition_key)] = data[partition_key]
        self.__processor(tmp, self.entity_name, "")
        self.__fix_nested_list()
        self.__flatten_dict[self.entity_name][0].pop(
            dot_join_args(self.entity_name, self.primary_key)
        )
        for partition_key in self.partition_keys:
            self.__flatten_dict[self.entity_name][0].pop(
                dot_join_args(self.entity_name, partition_key)
            )
        self.__apply_column_translate()
        del tmp
        return self.__flatten_dict
