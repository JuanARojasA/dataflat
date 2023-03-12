'''
pandas_dataframe/processor.py - The processor script for pandas dataframes flattening process

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
from pyflat.commons import init_logger
from pyflat.exceptions import FlatteningException
from pyflat.dictionary.processor import CustomFlattener as DictionaryCustomFlattener


logger = init_logger(__name__)

@typechecked
class CustomFlattener():
    def __init__(self):
        logger.info("CustomFlattener for Pandas Dataframes has been initiated")


    def transform(self, dataframe:pd.DataFrame, id_key:str, black_list:List[str] = [], dataframe_name:str = "df", chunk_size:int = 500, snake_case_cols:bool = False, replace_dots:bool = False) -> dict:
        """Receive a pandas Dataframe, and return a dictionary with the
        flattenend pandas Dataframes. If a black_list is specified,
        the columns inside the black_list will be skipped from the flattening
        process.
        The chunk_size variables helps to proccess the provided dataframes in
        batches.
        If a dataframe_name is provided, will be used to define the key_names on
        the returned dataframes.

        Parameters
        ----------
        df : pandas.Dataframe
        id_key : str
        black_list : List[str], default is []
        dataframe_name : str, default is 'df'
        chunk_size : int, default is 500
        snake_case_cols : bool, default is False
        replace_dots : bool, default is False

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
                                                           snake_case_cols=snake_case_cols,
                                                           replace_dots=replace_dots)
                for key, value in processed_dictionary.items():
                    print(f"{key}: {value}")
                    try:
                        processed_dataframes[key].extend( processed_dictionary[key] )
                    except:
                        processed_dataframes[key] = processed_dictionary[key]
        return processed_dataframes