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

from pydantic import validate_call

from dataflat.base_flattener import BaseFlattener
from dataflat.utils.logger import init_logger
from dataflat.utils.string import dot_join_args

logger = init_logger(__name__)


class CustomFlattener(BaseFlattener):
    # ------------------------------------------------------------------
    # Internal state initialisation
    # ------------------------------------------------------------------

    def _setup(
        self,
        primary_key: Optional[str] = None,
        entity_name: Optional[str] = None,
        partition_keys: Optional[list[str]] = None,
        black_list: Optional[list[str]] = None,
        white_list: Optional[list[str]] = None,
    ) -> None:
        super()._setup(primary_key, entity_name, partition_keys, black_list, white_list)
        self._entity_inherited_columns: dict[str, list[str]] = {}
        self._temp_dict: defaultdict[str, dict[str, Any]] = defaultdict(dict)
        self._flatten_dict: dict[str, list[dict[str, Any]]] = defaultdict(list)

    # ------------------------------------------------------------------
    # Field helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _split_and_dict(string: str) -> dict[str, str]:
        pattern = re.compile(r"(\._\d+_)")
        parts = pattern.split(string)
        result_dict = {
            parts[i - 1].lstrip("."): parts[i][2:-1] for i in range(1, len(parts), 2)
        }
        return result_dict

    def _set_heritable_fields(self, dictionary: dict[str, Any]) -> None:
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

    def _apply_column_translate(self) -> None:
        if self.case_translator is not None:
            translated: dict[str, list[dict[str, Any]]] = {}
            for entity_name, records in self._flatten_dict.items():
                new_entity = self._process_strings(entity_name)
                new_records = [
                    {self._process_strings(k): v for k, v in record.items()}
                    for record in records
                ]
                translated[new_entity] = new_records
            self._flatten_dict = translated

    def _apply_white_list(self) -> None:
        if not self.white_list:
            return
        plan = self._compute_white_list_plan(
            list(self._flatten_dict.keys()), self._entity_inherited_columns
        )
        new_dict: dict[str, list[dict[str, Any]]] = {}
        for entity_key, cols in plan.items():
            records = self._flatten_dict[entity_key]
            if cols is not None:
                records = [{k: v for k, v in r.items() if k in cols} for r in records]
            new_dict[entity_key] = records
        self._flatten_dict = new_dict

    # ------------------------------------------------------------------
    # Dict traversal
    # ------------------------------------------------------------------

    def _fix_nested_list(self) -> None:
        dict_names = list(self._temp_dict.keys())
        for dict_name in dict_names:
            index_keys = self._split_and_dict(dict_name)
            aux = self._temp_dict.pop(dict_name)
            fixed_dict_name = dict_name
            if index_keys:
                aux.update(self._heritable_fields)
                last_key, last_value = index_keys.popitem()
                trailing_index_key = ""
                intermediate_index_cols: list[str] = []
                for index_key, index_value in index_keys.items():
                    trailing_index_key = dot_join_args(trailing_index_key, index_key)
                    idx_col = dot_join_args(trailing_index_key, "index")
                    intermediate_index_cols.append(idx_col)
                    aux[idx_col] = int(index_value)
                aux["index"] = int(last_value)
                fixed_dict_name = dot_join_args(trailing_index_key, last_key)
                # Track inherited columns once per entity (same structure every record).
                if fixed_dict_name not in self._entity_inherited_columns:
                    self._entity_inherited_columns[fixed_dict_name] = (
                        list(self._heritable_fields.keys())
                        + intermediate_index_cols
                        + ["index"]
                    )
            if fixed_dict_name in self._flatten_dict:
                self._flatten_dict[fixed_dict_name].append(aux)
            else:
                self._flatten_dict[fixed_dict_name] = [aux]

    def _process_list(
        self, key: str, value: list, dict_name: str, schema_ref: str
    ) -> None:
        if isinstance(value[0], dict):
            for index, item in enumerate(value):
                self._processor(
                    item,
                    dot_join_args(dict_name, schema_ref, key, f"_{str(index)}_"),
                    "",
                )
        else:
            self._temp_dict[dict_name][dot_join_args(schema_ref, key)] = "|".join(
                str(item) for item in value
            )

    def _processor(
        self, dictionary: dict[str, Any], dict_name: str, schema_ref: str
    ) -> None:
        for key, value in dictionary.items():
            if (
                not self._is_blacklisted(dict_name, dot_join_args(schema_ref, key))
                and value is not None
                and value != []
                and value != ""
            ):
                if isinstance(value, dict):
                    self._processor(value, dict_name, dot_join_args(schema_ref, key))
                elif isinstance(value, list):
                    self._process_list(key, value, dict_name, schema_ref)
                else:
                    self._temp_dict[dict_name][dot_join_args(schema_ref, key)] = value

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @validate_call
    def flatten(
        self,
        data: dict[str, Any],
        primary_key: Optional[str] = None,
        entity_name: Optional[str] = None,
        partition_keys: Optional[list[str]] = None,
        black_list: Optional[list[str]] = None,
        white_list: Optional[list[str]] = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Flatten a Python dictionary that may contain nested dicts and lists.

        Parameters
        ----------
        data:
            Root dictionary to flatten.
        primary_key:
            Key to use as the root primary key.  When absent a UUID string is
            generated automatically.
        entity_name:
            Name prefix for the root entity; defaults to ``"data"``.
        partition_keys:
            Additional root keys (e.g. ``["date"]``) inherited by all child
            entities, prefixed with the entity name.
        black_list:
            Dot-separated field paths whose values should be excluded from all
            output, e.g. ``["totalOrders", "summary.totalClients"]``.
        white_list:
            Dot-separated paths that select which entities and/or columns to
            retain after flattening, e.g. ``["orders.items", "summary.total_revenue"]``.
            Entity-level entries keep the full entity and all descendants;
            column-level entries narrow the parent entity to inherited join
            columns plus the specified column.  An empty list (default) keeps
            everything.

        Returns
        -------
        dict[str, list[dict[str, Any]]]
            Mapping from entity name (dot-joined path) to a list of records.
            Every record carries the full chain of pk / index columns so
            parent–child relationships can be reconstructed.
        """
        self._setup(primary_key, entity_name, partition_keys, black_list, white_list)
        tmp = data.copy()
        if self.primary_key is None:
            pk_col = self._dataflat_id_col_name
            tmp[pk_col] = str(uuid.uuid4())
            self.primary_key = pk_col
        assert self.primary_key is not None
        self._entity_inherited_columns[self.entity_name] = [
            self.primary_key
        ] + self.partition_keys
        self._set_heritable_fields(tmp)
        tmp[dot_join_args(self.entity_name, self.primary_key)] = tmp[self.primary_key]
        for partition_key in self.partition_keys:
            tmp[dot_join_args(self.entity_name, partition_key)] = data[partition_key]
        self._processor(tmp, self.entity_name, "")
        self._fix_nested_list()
        self._flatten_dict[self.entity_name][0].pop(
            dot_join_args(self.entity_name, self.primary_key)
        )
        for partition_key in self.partition_keys:
            self._flatten_dict[self.entity_name][0].pop(
                dot_join_args(self.entity_name, partition_key)
            )
        self._apply_white_list()
        self._apply_column_translate()
        del tmp
        return self._flatten_dict
