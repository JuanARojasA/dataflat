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
            renamed_col = self._to_snake_case(to_snake_case, col)
            renamed_col = self._replace_dots(replace_dots, renamed_col)
            if to_snake_case | replace_dots:
                dataframe = dataframe.withColumnRenamed(col, renamed_col)
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
                field_ref = _ref + f"`{field['name']}`"
                field_name = field_ref.replace("`","").replace(f"{dataframe_name.split('.')[-1]}.","")
                if type(field['type']) is dict:
                    if field['type']['type'] == "array":
                        try:
                            named_struct[f"{dataframe_name}.{field_name}"] += f"'{dataframe_name}_id',{id_key},'{field_name}',{field_ref},"
                        except:
                            named_struct[f"{dataframe_name}.{field_name}"] = f"'{dataframe_name}_id',{id_key},'{field_name}',{field_ref},"
                    else:
                        named_struct = self._get_schema_struct(field['type'], id_key, black_list, dataframe_name, f"{field_ref}.", named_struct)
                elif field['type'] == "map":
                    print(f"MapType is not supported, skipping column: {field_name}")
                else:
                    try:
                        named_struct[dataframe_name] += f"'{field_name}',{field_ref},"
                    except:
                        named_struct[dataframe_name] = f"'{field_name}',{field_ref},"
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
        parent_pos = f",'{dataframe_name}_pos',`pos`" if "'pos',`pos`" in ns[dataframe_name] else ""
        for df_name, struct in ns.items():
            if df_name != dataframe_name:
                selectExpr = f"named_struct( {struct[:-1]}{parent_pos} ) as Item"
                aux_df = dataframe.selectExpr( selectExpr ).select("Item.*")
                child_id_key = aux_df.columns[0] if "." not in aux_df.columns[0] else f"`{aux_df.columns[0]}`"
                expld_col = aux_df.columns[1]
                expld_expr = expld_col if "." not in expld_col else f"`{expld_col}`"
                selected_cols = [child_id_key, posexplode(expld_expr).alias('pos', expld_col)]
                if parent_pos:
                    selected_cols.append(f'`{dataframe_name}_pos`')
                aux_df = aux_df.select(selected_cols) \
                    .withColumnRenamed(expld_col, expld_col.split(".")[-1])
                processed_data[df_name] = aux_df
                for types in aux_df.dtypes:
                    if "struct<" in types[1]:
                        nested_dfs = self._processor(aux_df, child_id_key, black_list, df_name)
                        processed_data.update(nested_dfs)
            else:
                selectExpr = f"named_struct( {struct[:-1]} ) as Item"
                aux_df = dataframe.selectExpr( selectExpr ).select("Item.*")
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