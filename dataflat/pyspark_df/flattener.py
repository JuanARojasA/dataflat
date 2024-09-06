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

from pyspark.sql import DataFrame, SparkSession
from typeguard import typechecked

from dataflat.base.flattener import BaseFlattener
from dataflat.commons import init_logger
from dataflat.exceptions import FlatteningException
from dataflat.utils.case_translator import CustomCaseTranslator
from dataflat.utils.string import dot_join_args


def _add_backticks_if_special_char(string: str):
    pattern = re.compile(r"[.!@]")
    string = f"`{string}`" if pattern.search(string) else string
    return string


def _split_field_if_special_char(field: str) -> str:
    split = field.split(".")
    field = _add_backticks_if_special_char(split[-1])
    return ".".join([*split[:-1], field])


logger = init_logger(__name__)


@typechecked
class CustomFlattener(BaseFlattener):
    logger.info("CustomFlattener for PySpark Dataframes has been initiated")
    spark: SparkSession

    def __set__(
        self,
        primary_key: Optional[str],
        entity_name: Optional[str] = None,
        partition_keys: Optional[list[str]] = None,
        black_list: Optional[list[str]] = None,
        case_translator: Optional[CustomCaseTranslator] = None,
        replace_string: Optional[str] = None,
    ):

        self.primary_key = primary_key if primary_key else self.primary_key
        self.entity_name = entity_name if entity_name else self.entity_name
        self.partition_keys = partition_keys if partition_keys else []
        self.black_list = black_list if black_list is not None else []
        self.case_translator = (
            case_translator if case_translator else self.case_translator
        )
        self.replace_string = replace_string if replace_string else "."
        self._flattened_schemas: dict[str, list[str]] = {}
        self._relations: dict[str, str] = {}
        self._heritable_fields: defaultdict[str, list] = defaultdict(list)
        self._flattened_dataframes: dict[str, DataFrame] = {}

    def __process_strings(self, string: str):
        if self.case_translator is not None:
            return self.replace_string.join(
                [
                    self.case_translator.translate(sub_string)
                    for sub_string in string.split(".")
                    if string != ""
                ]
            )
        return self.replace_string.join(string.split("."))

    def __apply_column_translate(self):
        if (self.replace_string != ".") or (self.case_translator is not None):
            translated_dfs = {}
            for df_name, df in self._flattened_dataframes.items():
                select_expr = [
                    f"`{col}` `{self.__process_strings(col)}`" for col in df.columns
                ]
                df = df.selectExpr(*select_expr)
                fixed_df_name = self.__process_strings(df_name)
                translated_dfs[fixed_df_name] = df
            self._flattened_dataframes = translated_dfs

    def __generate_select_query(
        self, table_name: str, source_table: str, heritable_fields: list[str]
    ) -> str:
        def get_columns() -> str:
            columns = []
            for field in self._flattened_schemas[table_name]:
                if "." in field:
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

    def __get_heritable_fields(self, source_table: str) -> list[str]:
        if source_table not in self._heritable_fields:
            self._heritable_fields[source_table].extend(
                [
                    field
                    for field in list(self._flattened_dataframes[source_table].columns)
                    if field.endswith("index")
                ]
            )
        return self._heritable_fields[source_table]

    def __get_nested_struct(
        self, schema: dict[str, Any], df_name: str, schema_ref: str
    ) -> None:
        selected_fields = []
        fields = [
            field
            for field in schema["fields"]
            if not any(
                dot_join_args(df_name, schema_ref, field["name"]).endswith(item)
                for item in self.black_list
            )
        ]
        for field in fields:
            try:
                nested_field = field["type"]
                if nested_field["type"] == "struct":
                    self.__get_nested_struct(
                        nested_field, df_name, dot_join_args(schema_ref, field["name"])
                    )
                elif nested_field["type"] == "array":
                    self._relations[
                        dot_join_args(df_name, schema_ref, field["name"])
                    ] = df_name
                    fixed_field_name = dot_join_args("", schema_ref, field["name"])
                    selected_fields.append(fixed_field_name)
                    if isinstance(nested_field["elementType"], dict):
                        self.__get_nested_struct(
                            nested_field["elementType"],
                            dot_join_args(df_name, schema_ref, field["name"]),
                            "",
                        )
                    else:
                        self._flattened_schemas[
                            dot_join_args(df_name, schema_ref, field["name"])
                        ] = []
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

    def __processor(
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
        explode = True if self._flattened_schemas[target_table] else False
        exploded_field = explode_field.split(".")[-1]
        query = (
            f"SELECT * FROM (SELECT {fields}, POSEXPLODE(`{explode_field}`) "
            f"AS (index, {exploded_field}) FROM `{source_table}`)"
        )
        temp = self.spark.sql(query)
        heritable_fields = list(temp.columns)
        if explode:
            temp = temp.select("*", f"{exploded_field}.*")
            heritable_fields.remove(exploded_field)
        temp = temp.drop(exploded_field) if explode else temp
        temp.createOrReplaceTempView("temp")
        return heritable_fields

    def flatten(
        self,
        data: DataFrame,
        primary_key: Optional[str] = None,
        entity_name: Optional[str] = None,
        partition_keys: Optional[list[str]] = None,
        black_list: Optional[list[str]] = None,
    ) -> dict[str, DataFrame]:
        self.__set__(primary_key, entity_name, partition_keys, black_list)
        self.spark = SparkSession.getActiveSession()
        data.createOrReplaceTempView(self.entity_name)
        self.__get_nested_struct(data.schema.jsonValue(), self.entity_name, "")
        sorted_dataframes = sorted(
            list(self._flattened_schemas.keys()), key=lambda k: k.split(".")
        )

        for table_name in sorted_dataframes:
            parent_table = (
                self._relations[table_name]
                if table_name in self._relations
                else self.entity_name
            )
            explode_col: str = table_name.removeprefix(f"{parent_table}.")
            source_table = table_name if table_name == self.entity_name else "temp"
            heritable_fields = []

            if table_name != self.entity_name:
                source_table = "temp"
                heritable_fields = self.__processor(
                    parent_table,
                    self.partition_keys,
                    self.__get_heritable_fields(parent_table),
                    table_name,
                    explode_col,
                )
                heritable_fields = [
                    f"`{field}`" if "." in field else field
                    for field in heritable_fields
                ]
            select_query = self.__generate_select_query(
                table_name, source_table, heritable_fields
            )
            temp = self.spark.sql(select_query)
            self._flattened_dataframes[table_name] = self.spark.createDataFrame(
                data=temp.rdd, schema=temp.schema
            )
            self._flattened_dataframes[table_name].createOrReplaceTempView(
                f"`{table_name}`"
            )
            temp.unpersist()
            if explode_col and table_name != self.entity_name:
                self._flattened_dataframes[parent_table] = self._flattened_dataframes[
                    parent_table
                ].drop(explode_col)

        self.__apply_column_translate()
        return self._flattened_dataframes
