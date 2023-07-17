'''
dataflat/spark_df/flattener.py - The processor script for spark dataframes flattening process

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
from dataflat.utils.case_translator import CustomCaseTranslator
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import posexplode

logger = init_logger(__name__)

@typechecked
class CustomFlattener():
    def __init__(self, case_translator:CustomCaseTranslator, replace_dots:bool):
        logger.info("CustomFlattener for PySpark Dataframes has been initiated")
        self._case_translator = case_translator
        self._reference_separator = "_" if replace_dots else "."


    def _process_strings(self, strings:List[str], join_key:str):
        return join_key.join([self._case_translator.translate(string) for string in strings if string!=""])


    def _process_dataframe_columns(self, dataframe_name:str, dataframe:DataFrame):
        for col in dataframe.columns:
            renamed_col = re.sub(f"{self._reference_separator}+", self._reference_separator, self._process_strings(col.split("."), self._reference_separator))
            dataframe = dataframe.withColumnRenamed(col, renamed_col)
        dataframe_name = re.sub(f"\\{self._reference_separator}\\{self._reference_separator}+", self._reference_separator, self._process_strings(dataframe_name.split("."), self._reference_separator))
        return (dataframe_name, dataframe)


    def _get_schema_struct(self, dataframe_schema:dict, id_key:str, dataframe_name:str, _ref:str = "", named_struct:dict = {}):
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
            if field['name'] not in self._black_list:
                field_ref = _ref + f"`{field['name']}`"
                field_name = field_ref.replace("`","").replace(f"{dataframe_name.split('.')[-1]}.","")
                if type(field['type']) is dict:
                    if field['type']['type'] == "array":
                        field_name_aux = 'value' if isinstance(field['type']['elementType'], str) else field_name
                        
                        try:
                            named_struct[f"{dataframe_name}.{field_name}"] += f"'{self._parent_df_name}_{self._parent_id_key}',{id_key},'{field_name_aux}',{field_ref},"
                        except:
                            named_struct[f"{dataframe_name}.{field_name}"] = f"'{self._parent_df_name}_{self._parent_id_key}',{id_key},'{field_name_aux}',{field_ref},"
                    else:
                        named_struct = self._get_schema_struct(field['type'], id_key, dataframe_name, f"{field_ref}.", named_struct)
                elif field['type'] == "map":
                    logger.warning(f"MapType is not supported, skipping column: {field_name}")
                else:
                    try:
                        named_struct[dataframe_name] += f"'{field_name}',{field_ref},"
                    except:
                        named_struct[dataframe_name] = f"'{field_name}',{field_ref},"
        return named_struct


    def _processor(self, dataframe:DataFrame, id_key:str, dataframe_name:str):
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
        ns = self._get_schema_struct(schema, id_key, dataframe_name, _ref="", named_struct={})
        parent_pos = f",'{dataframe_name}_index',`index`" if "'index',`index`" in ns[dataframe_name] else ""
        for df_name, struct in ns.items():
            if df_name != dataframe_name:
                selectExpr = f"named_struct( {struct[:-1]}{parent_pos} ) as Item"
                aux_df = dataframe.selectExpr( selectExpr ).select("Item.*")
                child_id_key = aux_df.columns[0] if "." not in aux_df.columns[0] else f"`{aux_df.columns[0]}`"
                expld_col = aux_df.columns[1]
                expld_expr = expld_col if "." not in expld_col else f"`{expld_col}`"
                selected_cols = [child_id_key, posexplode(expld_expr).alias('index', expld_col)]
                if parent_pos:
                    selected_cols.append(f'`{dataframe_name}_index`')
                aux_df = aux_df.select(selected_cols) \
                    .withColumnRenamed(expld_col, expld_col.split(".")[-1])
                processed_data[df_name] = aux_df
                for types in aux_df.dtypes:
                    if "struct<" in types[1]:
                        nested_dfs = self._processor(aux_df, child_id_key, df_name)
                        processed_data.update(nested_dfs)
            else:
                selectExpr = f"named_struct( {struct[:-1]} ) as Item"
                aux_df = dataframe.selectExpr( selectExpr ).select("Item.*")
                processed_data[df_name] = aux_df
        return processed_data


    def transform(self, dataframe:DataFrame, id_key:str, black_list:List[str] = [], dataframe_name:str = "df"):
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
        Returns
        -------
        processed_data : dict[str, pyspark.DataFrame]
            A dictionary with all the resulting Dataframes from the flattening process.
            Each ArrayType column will be flattened and added as a Dataframe inside the
            processed_data return.
        """
        if len(dataframe.head(1)) == 0:
            raise FlatteningException("The provided dataframe is empty.")
        elif id_key not in dataframe.columns:
            raise FlatteningException(f"The provided id_key={id_key} does not exist in dataframe")
        
        self._parent_df_name = dataframe_name
        self._parent_id_key = id_key
        self._black_list = black_list
        aux, self._processed_data = {}, {}
        aux = self._processor(dataframe, id_key, dataframe_name)
        for processed_df_name, processed_df in aux.items():
            processed_aux = self._process_dataframe_columns(processed_df_name, processed_df)
            self._processed_data[processed_aux[0]] = processed_aux[1]
        return self._processed_data