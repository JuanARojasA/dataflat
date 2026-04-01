"""
dataflat/pyspark/flattener.py - The processor script for spark dataframes flattening process

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
from collections import defaultdict
from typing import Any, Optional

from pydantic import ConfigDict, validate_call
from pyspark.sql import DataFrame, SparkSession

from dataflat.base_flattener import BaseFlattener
from dataflat._core import FlatteningException
from dataflat.utils.logger import init_logger
from dataflat.utils.string import dot_join_args


def _add_backticks_if_special_char(string: str) -> str:
    pattern = re.compile(r"[.!@]")
    string = f"`{string}`" if pattern.search(string) else string
    return string


def _split_field_if_special_char(field: str) -> str:
    split = field.split(".")
    field = _add_backticks_if_special_char(split[-1])
    return ".".join([*split[:-1], field])


logger = init_logger(__name__)

_FLATTEN_CONFIG = ConfigDict(arbitrary_types_allowed=True)


class CustomFlattener(BaseFlattener):
    spark: SparkSession

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
        self._flattened_schemas: dict[str, list[str]] = {}
        self._relations: dict[str, str] = {}
        self._heritable_fields: defaultdict[str, list] = defaultdict(list)
        self._scalar_array_fields: defaultdict[str, list] = defaultdict(list)
        self._flattened_dataframes: dict[str, DataFrame] = {}

    # ------------------------------------------------------------------
    # Column-name translation helpers
    # ------------------------------------------------------------------

    def _apply_white_list(self) -> None:
        if not self.white_list:
            return
        plan = self._compute_white_list_plan(
            list(self._flattened_dataframes.keys()), self._entity_inherited_columns
        )
        new_dfs: dict[str, DataFrame] = {}
        for entity_key, cols in plan.items():
            df = self._flattened_dataframes[entity_key]
            if cols is not None:
                keep = [c for c in df.columns if c in cols]
                df = df.select(*[f"`{c}`" if "." in c else c for c in keep])
            new_dfs[entity_key] = df
        self._flattened_dataframes = new_dfs

    def _apply_column_translate(self) -> None:
        if self.case_translator is not None:
            translated_dfs = {}
            for df_name, df in self._flattened_dataframes.items():
                select_expr = [
                    f"`{col}` `{self._process_strings(col)}`" for col in df.columns
                ]
                df = df.selectExpr(*select_expr)
                fixed_df_name = self._process_strings(df_name)
                translated_dfs[fixed_df_name] = df
            self._flattened_dataframes = translated_dfs

    # ------------------------------------------------------------------
    # Query builders
    # ------------------------------------------------------------------

    def _generate_select_query(
        self, table_name: str, source_table: str, heritable_fields: list[str]
    ) -> str:
        def get_columns() -> str:
            columns = []
            scalar_arr_fields = self._scalar_array_fields[table_name]
            for field in self._flattened_schemas[table_name]:
                if field in scalar_arr_fields:
                    col_ref = (
                        _split_field_if_special_char(field)
                        if "." in field
                        else _add_backticks_if_special_char(field)
                    )
                    columns.append(f"ARRAY_JOIN({col_ref}, '|') AS `{field}`")
                elif "." in field:
                    columns.append(
                        f"{_split_field_if_special_char(field)} AS `{field}`"
                    )
                else:
                    columns.append(_add_backticks_if_special_char(field))
            return ", ".join(columns)

        return (
            f"SELECT {get_columns()} "
            f"{',' if heritable_fields and self._flattened_schemas[table_name] else ''}"
            f"{', '.join(heritable_fields)} "
            f"FROM {source_table}"
        )

    def _get_heritable_fields(self, source_table: str) -> list[str]:
        if source_table not in self._heritable_fields:
            self._heritable_fields[source_table].extend(
                [
                    field
                    for field in self._flattened_dataframes[source_table].columns
                    if field.endswith("index")
                ]
            )
        return self._heritable_fields[source_table]

    # ------------------------------------------------------------------
    # Schema traversal
    # ------------------------------------------------------------------

    def _get_nested_struct(
        self, schema: dict[str, Any], df_name: str, schema_ref: str
    ) -> None:
        selected_fields = []
        fields = [
            field
            for field in schema["fields"]
            if not self._is_blacklisted(
                df_name, dot_join_args(schema_ref, field["name"])
            )
        ]
        for field in fields:
            try:
                nested_field = field["type"]
                if nested_field["type"] == "struct":
                    self._get_nested_struct(
                        nested_field, df_name, dot_join_args(schema_ref, field["name"])
                    )
                elif nested_field["type"] == "array":
                    fixed_field_name = dot_join_args("", schema_ref, field["name"])
                    selected_fields.append(fixed_field_name)
                    if isinstance(nested_field["elementType"], dict):
                        self._relations[
                            dot_join_args(df_name, schema_ref, field["name"])
                        ] = df_name
                        self._get_nested_struct(
                            nested_field["elementType"],
                            dot_join_args(df_name, schema_ref, field["name"]),
                            "",
                        )
                    else:
                        # Scalar array: join with "|" in parent entity instead of
                        # creating a separate child table.
                        self._scalar_array_fields[df_name].append(fixed_field_name)
                else:
                    raise FlatteningException(
                        f"{nested_field['type']} is not supported, field {field['name']} will not be processed."
                    )
            except (TypeError, FlatteningException):
                fixed_field_name = dot_join_args("", schema_ref, field["name"])
                selected_fields.append(fixed_field_name)
        try:
            self._flattened_schemas[df_name].extend(selected_fields)
        except KeyError:
            self._flattened_schemas.update({df_name: selected_fields})

    # ------------------------------------------------------------------
    # Explode processor
    # ------------------------------------------------------------------

    def _processor(
        self,
        source_table: str,
        partition_keys: list[str],
        heritable_fields: list[str],
        target_table: str,
        explode_field: str,
    ) -> list[str]:
        columns = [
            f"{partition_key} AS `{self.entity_name}.{partition_key}`"
            for partition_key in partition_keys
        ]
        rename_heritable_fields_query = (
            f"{self.primary_key} AS `{self.entity_name}.{self.primary_key}`, "
            f"{', '.join(columns)}"
        )

        columns = [
            f"`{self.entity_name}.{partition_key}`" for partition_key in partition_keys
        ]
        select_heritable_fields_query = (
            f"`{self.entity_name}.{self.primary_key}`, {', '.join(columns)}"
        )

        fields = (
            rename_heritable_fields_query
            if source_table == self.entity_name
            else select_heritable_fields_query
        )
        if heritable_fields:
            columns = [
                (
                    f"`{heritable_field}`"
                    if heritable_field != "index"
                    else f"{heritable_field} AS `{source_table}.{heritable_field}`"
                )
                for heritable_field in heritable_fields
            ]
            fields += f", {', '.join(columns)}"
        explode = bool(self._flattened_schemas[target_table])
        exploded_field = explode_field.split(".")[-1]
        query = (
            f"SELECT * FROM (SELECT {fields}, POSEXPLODE(`{explode_field}`) "
            f"AS (index, {exploded_field}) FROM `{source_table}`)"
        )
        temp = self.spark.sql(query)
        heritable_fields = temp.columns
        if explode:
            temp = temp.select("*", f"{exploded_field}.*")
            heritable_fields.remove(exploded_field)
        temp = temp.drop(exploded_field) if explode else temp
        temp.createOrReplaceTempView("temp")
        return heritable_fields

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @validate_call(config=_FLATTEN_CONFIG)
    def flatten(
        self,
        data: DataFrame,
        primary_key: Optional[str] = None,
        entity_name: Optional[str] = None,
        partition_keys: Optional[list[str]] = None,
        black_list: Optional[list[str]] = None,
        white_list: Optional[list[str]] = None,
    ) -> dict[str, DataFrame]:
        """Flatten a PySpark DataFrame that may contain Struct and Array columns.

        Parameters
        ----------
        data:
            Root DataFrame to flatten.
        primary_key:
            Column name to use as the root primary key.  When absent a UUID
            column is added automatically via ``uuid()``.
        entity_name:
            Name prefix for the root entity; defaults to ``"data"``.
        partition_keys:
            Additional root columns (e.g. ``["date"]``) inherited by all child
            DataFrames, renamed with the entity prefix.
        black_list:
            Dot-separated field paths whose values should be excluded from all
            output DataFrames, e.g. ``["totalOrders", "summary.totalClients"]``.
        white_list:
            Dot-separated paths that select which entities and/or columns to
            retain after flattening, e.g. ``["orders.items", "summary.total_revenue"]``.
            Entity-level entries keep the full entity and all descendants;
            column-level entries narrow the parent entity to inherited join
            columns plus the specified column.  An empty list (default) keeps
            everything.

        Returns
        -------
        dict[str, DataFrame]
            Mapping from entity name (dot-joined path) to its flattened
            DataFrame.  Every DataFrame carries the full chain of pk / index
            columns so parent–child relationships can be reconstructed.
        """
        self._setup(primary_key, entity_name, partition_keys, black_list, white_list)

        if self.primary_key is None:
            from pyspark.sql import functions as F

            pk_col = self._dataflat_id_col_name
            data = data.withColumn(pk_col, F.expr("uuid()"))
            self.primary_key = pk_col

        assert self.primary_key is not None
        self._entity_inherited_columns[self.entity_name] = [
            self.primary_key
        ] + self.partition_keys

        session = SparkSession.getActiveSession()
        assert session is not None, "No active Spark session"
        self.spark = session
        data.createOrReplaceTempView(self.entity_name)
        self._get_nested_struct(data.schema.jsonValue(), self.entity_name, "")
        sorted_dataframes = sorted(
            self._flattened_schemas.keys(), key=lambda k: k.split(".")
        )

        for table_name in sorted_dataframes:
            parent_table = (
                self._relations[table_name]
                if table_name in self._relations
                else self.entity_name
            )
            explode_col: str = table_name.removeprefix(f"{parent_table}.")
            source_table = self.entity_name
            heritable_fields_raw: list[str] = []

            if table_name != self.entity_name:
                source_table = "temp"
                heritable_fields_raw = self._processor(
                    parent_table,
                    self.partition_keys,
                    self._get_heritable_fields(parent_table),
                    table_name,
                    explode_col,
                )
                self._entity_inherited_columns[table_name] = list(heritable_fields_raw)
                heritable_fields = [
                    f"`{field}`" if "." in field else field
                    for field in heritable_fields_raw
                ]
            else:
                heritable_fields = []

            select_query = self._generate_select_query(
                table_name, source_table, heritable_fields
            )
            temp = self.spark.sql(select_query)
            self._flattened_dataframes[table_name] = temp
            temp.createOrReplaceTempView(f"`{table_name}`")
            if explode_col and table_name != self.entity_name:
                self._flattened_dataframes[parent_table] = self._flattened_dataframes[
                    parent_table
                ].drop(explode_col)

        self._apply_white_list()
        self._apply_column_translate()
        return self._flattened_dataframes
