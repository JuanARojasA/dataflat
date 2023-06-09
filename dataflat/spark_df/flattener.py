'''
spark_df/flattener.py- The processor script for pyspark dataframes flattening process

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
from dataflat.commons import init_logger
from dataflat.exceptions import FlatteningException
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import posexplode

logger = init_logger(__name__)

@typechecked
class CustomFlattener():
    def __init__(self):
        logger.info("CustomFlattener for PySpark Dataframes has been initiated")

    def _to_snake_case(self, process_col:bool, col_name:str):
        if process_col:
            pattern = re.compile(r'(?<!^)(?=[A-Z])')
            return pattern.sub('_', col_name).lower().replace("__", "_").replace("._", ".")
        return col_name
    
    def _replace_dots(self, process_col:bool, col_name:str):
        if process_col:
            return col_name.replace(".", "_")
        return col_name

    def _process_column_name(self, dataframe:DataFrame, to_snake_case:bool, replace_dots:bool):
        """Receive a Spark Dataframe and return a snake_case columns like dataframe.
        If replace_dots is True, then all the subcolumns like "Column.SubColumn" will be
        renamed as "Column_SubColumn".

        Parameters
        ----------
        dataframe: pyspark.Dataframe
            The Spark Dataframe to rename each column to snake_case and replace dots with underscores.
        to_snake_case: bool
            If True rename the column to Snake Case column name like.
        replace_dots: bool
            If True replace the column name dots with underscores.

        Returns
        -------
        dataframe: pyspark.Dataframe
        """
        for col in dataframe.columns:
            sc_col = self._to_snake_case(to_snake_case, col)
            sc_col = self._replace_dots(replace_dots, sc_col)
            if to_snake_case | replace_dots:
                dataframe = dataframe.withColumnRenamed(col, sc_col)
        return dataframe

    def _get_schema_struct(self, dataframe_schema:dict, id_key:str, black_list:List[str], dataframe_name:str, _ref:str = "", named_struct:dict = {}):
        """Receive Spark Dataframe Schema in dictionary format, and return 
        a dictionary[str, str] with the all required named_struct expressions
        to flatten de Dataframe.

        Parameters
        ----------
        dataframe_schema: dict
            A dictionary with the Dataframe schema.
        id_key: str
            The id key to be used as reference to the parent dataframe.
        black_list: List[str]
            A list of columns to ignore and not to add to the resulting flattened dictionary.
        dataframe_name: str
            A reference name for the dataframe, used to difference each
            resulting dataframe in the named_struct return.
        named_struct : dict, default is {}

        Returns
        -------
        named_struct: dict[str, str]
            A dictionary with all the named_struct expressions to flatten the
            provided Dataframe schema.
        """
        for field in dataframe_schema['fields']:
            if field['name'] not in black_list:
                field_name = _ref + field['name']
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

        Parameters
        ----------
        dataframe: pyspark.DataFrame
            A Dataframe to be flattened
        id_key: str
            The id key to be used as reference to the parent dataframe.
        black_list: List[str]
            A list of columns to ignore and not to add to the resulting flattened dictionary.
        dataframe_name: str
            A reference name for the dataframe, used to difference each
            resulting dataframe in the named_struct return.

        Returns
        -------
        processed_data : dict[str, pyspark.DataFrame]
            A dictionary with all the resulting Dataframes from the flattening process.
            Each ArrayType column will be flattened and added as a Dataframe inside the
            processed_data return.
        """
        processed_data = {}
        schema = json.loads(dataframe.schema.json())
        ns = self._get_schema_struct(schema, id_key, black_list, dataframe_name, _ref="", named_struct={})
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

    def transform(self, dataframe:DataFrame, id_key:str, black_list:List[str] = [], dataframe_name:str = "df", to_snake_case:bool = False, replace_dots:bool = False):
        """Receive a pyspark.Dataframe and returns a Dictionary[str, pyspark.DataFrame]
        with all the processed dataframes.
        The ammount of dataframes on returned dictionary will depends on the number
        of ArrayType columns present on the input dataframe.
        If to_snake_case is set True, the column names on every processed Dataframe
        will be casted to snake_case.
        If replace_docts is set to True, all the dots present on a column name will
        be replaces such as follow example, "Column.SubColumn" -> "Column_SubColumn".

        Parameters
        ----------
        dataframe: pyspark.DataFrame
            A Dataframe to be flattened
        id_key: str
            The id key to be used as reference to the parent dataframe.
        black_list: List[str]
            A list of columns to ignore and not to add to the resulting flattened dictionary.
        dataframe_name: str
            A reference name for the dataframe, used to difference each
            resulting dataframe in the named_struct return.
        to_snake_case: bool
            If True rename the column to Snake Case column name like.
        replace_dots: bool
            If True replace the column name dots with underscores.

        Returns
        -------
        processed_data : dict[str, pyspark.DataFrame]
            A dictionary with all the resulting Dataframes from the flattening process.
            Each ArrayType column will be flattened and added as a Dataframe inside the
            processed_data return.
        """
        if len(dataframe.head(1)) == 0:
            raise FlatteningException("The provided dataframe is empty.")
        processed_data = {}
        processed_data = self._processor(dataframe, id_key, black_list, dataframe_name)
        for processed_df_name, processed_df in processed_data.items():
            processed_data[processed_df_name] = self._process_column_name(processed_df, to_snake_case, replace_dots)
        return processed_data