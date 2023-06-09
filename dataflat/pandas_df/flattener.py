'''
pandas_df/flattener.py - The processor script for pandas dataframes flattening process

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

import pandas as pd
from typeguard import typechecked
from typing import List
from dataflat.commons import init_logger
from dataflat.exceptions import FlatteningException
from dataflat.dictionary.flattener import CustomFlattener as DictionaryCustomFlattener

logger = init_logger(__name__)

@typechecked
class CustomFlattener():
    def __init__(self):
        logger.info("CustomFlattener for Pandas Dataframes has been initiated")


    def transform(self, dataframe:pd.DataFrame, id_key:str, black_list:List[str] = [], dataframe_name:str = "df", to_snake_case:bool = False, replace_dots:bool = False, chunk_size:int = 500) -> dict:
        """Receive a pandas Dataframe, and return a dictionary with the
        flattenend pandas Dataframes.
        If a black_list is provided then all the column names inside the black_list will
        be skipped. Notice that if ['name'] is provided as black list, then 
        all the 'name' columns will be skipped, even if they are under a nested
        column.
        The dataframe_name will be used to reference each resulting flattened dataframe
        under the processed_dataframes dictionary returned.

        BEAWARE: This function will convert the dataframe to multiple dicitonaries (one per row) 
        and process them using the DictionaryProcessor.
        If you already have dictionaries before converting them to Dataframe, use DictionaryProcessor first, and later
        convert the resulting dictionaries to Dataframes.

        Parameters
        ----------
        df: pandas.Dataframe
            The dataframe to be flattened.
        id_key: str
            The id key to be used as reference to the parent dataframe.
        black_list: List[str], (default [])
            A list of keys to ignore and not to add to the resulting flattened dictionary.
        dataframe_name: str, (default 'df')
            A reference name for the dataframe, used to difference each
            resulting dataframe in the processed_dataframes return.
        to_snake_case: bool
            If True process the column name to Snake Case.
        replace_dots: bool
            If True replace the column name dots with underscores.
        chunk_size: int, (default 500)
            The chunk size used to process the dataframe in batches. 
            i.e. If dataframe size is 2000, and chunk_size is 500,
            then the dataframe will be processed in 4 batches.

        Returns
        -------
        processed_dicts : dict
        """
        dataframe_len = len(dataframe)
        if dataframe_len == 0:
            raise FlatteningException("The provided dataframe is empty.")
    
        flattener = DictionaryCustomFlattener()
        processed_dataframes = {}

        for i in range(0, dataframe_len, chunk_size):
            records = dataframe[i:i+chunk_size].to_dict('records')
            for dictionary in records:
                processed_dictionary = flattener.transform(dictionary, id_key, black_list, dataframe_name, 
                                                           to_snake_case=to_snake_case,
                                                           replace_dots=replace_dots)
                for key, value in processed_dictionary.items():
                    print(f"{key}: {value}")
                    try:
                        processed_dataframes[key].extend( processed_dictionary[key] )
                    except:
                        processed_dataframes[key] = processed_dictionary[key]
        return processed_dataframes