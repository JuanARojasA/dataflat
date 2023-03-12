'''
pyspark_dataframe/processor.py - The processor script for pyspark dataframes flattening process

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

import json
import re
from typeguard import typechecked
from typing import List
from pyflat.commons import init_logger
from pyflat.exceptions import FlatteningException
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import posexplode

logger = init_logger(__name__)

@typechecked
class CustomFlattener():
    def __init__(self):
        logger.info("CustomFlattener for PySpark Dataframes has been initiated")


    def _to_snake_case(self, dataframe:DataFrame, replace_dots:bool):
        """Receive a PySpark Dataframe and return a snake_case columns like, dataframe.
        If replace_dots is True, then all the subcolumns like "Column.SubColumn" will be
        renamed as "column_sub_column"; if False then "column.sub_column"

        Parameters
        ----------
        dataframe: pyspark.Dataframe
        replace_dots: bool

        Returns
        -------
        dataframe: pyspark.Dataframe
        """
        pattern = re.compile(r'(?<!^)(?=[A-Z])')
        for col in dataframe.columns:
            sc_col = pattern.sub('_', col).lower().replace("__", "_").replace("._", ".")
            if replace_dots:
                sc_col = sc_col.replace(".", "_")
            dataframe = dataframe.withColumnRenamed(col, sc_col)
        return dataframe

    def _get_schema_struct(self, dataframe_schema:dict, id_key:str, black_list:List[str], dataframe_name:str, ref:str = "", named_struct:dict = {}):
        """Receive a dictionary with a PySpark Dataframe Schema, and return 
        a dictionary[str, str] with the named_struct expression required
        to flatten de Dataframe.
        The number of expressions on returned dictionary will depends on the number
        of nested arrays present on the input dataframe schema, if none, only
        one expression will be returned.

        Parameters
        ----------
        dataframe_schema : dict
        id_key : str
        black_list : List[str]
        dataframe_name : str
        ref : str, default is ""
            This value is not required to assing, will be used internaly to assing
            the column names for subcolumns.
        named_struct : dict, default is {}

        Returns
        -------
        named_struct: dict[str, str]
        """
        for field in dataframe_schema['fields']:
            if field['name'] not in black_list:
                field_name = ref + field['name']
                if type(field['type']) is dict:
                    if field['type']['type'] == "array":
                        try:
                            named_struct[f"{dataframe_name}.{field_name}"] += f"'{id_key}', {id_key},'{field_name}', {field_name},"
                        except:
                            named_struct[f"{dataframe_name}.{field_name}"] = f"'{id_key}', {id_key},'{field_name}', {field_name},"
                    else:
                        named_struct = self._get_schema_struct(field['type'], id_key, black_list, dataframe_name, f"{field_name}.", named_struct)
                elif field['type'] == "map":
                    print(f"MapType is not supported with pyflat processing, skipping column: {field_name}")
                else:
                    try:
                        named_struct[dataframe_name] += f"'{field_name}', {field_name},"
                    except:
                        named_struct[dataframe_name] = f"'{field_name}', {field_name},"
        return named_struct

    def _processor(self, dataframe:DataFrame, id_key:str, black_list:List[str], dataframe_name:str):
        """Receive a pyspark.Dataframe and returns a Dictionary[str, pyspark.DataFrame]
        with all the processed dataframes.
        The ammount of dataframes on returned dictionary will depends on the number
        of nested arrays present on the input dataframe, if none, only one Dataframe
        will be returned.

        Parameters
        ----------
        dataframe_schema : dict
        id_key : bool
        black_list : List[str]
        dataframe_name : str

        Returns
        -------
        processed_data : dict[str, pyspark.DataFrame]
        """
        processed_data = {}
        schema = json.loads(dataframe.schema.json())
        ns = self._get_schema_struct(schema, id_key, black_list, dataframe_name, ns={})
        for df_name, struct in ns.items():
            selectExpr = f"named_struct( {struct[:-1]} ) as Item"
            aux_df = dataframe.selectExpr( selectExpr ).select("Item.*")
            if df_name != dataframe_name:
                expld_col = aux_df.columns[1]
                expld_expr = expld_col if "." not in expld_col else f"`{expld_col}`"
                aux_df = aux_df.select(id_key, posexplode(expld_expr).alias('pos', expld_col))
                processed_data[df_name] = aux_df
                for types in aux_df.dtypes:
                    if "struct<" in types[1]:
                        nested_dfs = self._processor(aux_df, id_key, black_list, df_name)
                        processed_data.update(nested_dfs)
            else:
                processed_data[df_name] = aux_df
        return processed_data

    def transform(self, dataframe:DataFrame, id_key:str, black_list:List[str] = [], dataframe_name:str = "df", snake_case_cols:bool = False, replace_dots:bool = False):
        """Receive a pyspark.Dataframe and returns a Dictionary[str, pyspark.DataFrame]
        with all the processed dataframes.
        The ammount of dataframes on returned dictionary will depends on the number
        of nested arrays present on the input dataframe, if none, only one Dataframe
        will be returned.
        If snake_case_cols is set to True, the column names on every processed Dataframe
        will be casted to snake_case.
        If replace_docts is set to True, all the dots present on a column name will
        be replaces such as follow example, "Column.SubColumn" -> "column_sub_column".

        Parameters
        ----------
        dataframe_schema : dict
        id_key : bool
        black_list : List[str], default is []
        dataframe_name : str, default is "df
        snake_case_cols : bool, default is False
        replace_dots : bool, default is False

        Returns
        -------
        processed_data : dict[str, pyspark.DataFrame]
        """
        if len(dataframe.head(1)) > 0:
            raise FlatteningException("The provided dataframe is empty.")
        processed_data = {}
        processed_data = self._processor(dataframe, id_key, black_list, dataframe_name)
        if snake_case_cols:
            for processed_df_name, processed_df in processed_data.items():
                processed_data[processed_df_name] = self._to_snake_case(processed_df, replace_dots)
        return processed_data