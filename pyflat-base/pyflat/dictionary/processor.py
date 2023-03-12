'''
dictionary/processor.py - The processor script for dictionaries flattening process

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

import re
from typeguard import typechecked
from typing import Tuple, List
from pyflat.commons import init_logger
from pyflat.exceptions import FlatteningException


logger = init_logger(__name__)

@typechecked
class CustomFlattener():
    def __init__(self):
        logger.info("CustomFlattener for Python Dictionaries has been initiated")


    def _to_snake_case(self, string:str, replace_dots:bool) -> str:
        """Convert the input string to snake_case string

        Parameters
        ----------
        string : str
        replace_dots : bool

        Returns
        -------
        str 
        """
        pattern = re.compile(r'(?<!^)(?=[A-Z])')
        string = pattern.sub('_', string).lower().replace("__", "_").replace("._", ".")
        if replace_dots:
            string = string.replace(".", "_")
        return 


    def _nested_processor(self, nested_list:dict, id_key:str, black_list:List[str], dict_name:str, 
                           dict_ref:str, super_dict_ref_id:List[str], 
                           processed_data:dict, snake_case_cols:bool, replace_dots:bool) -> dict:
        """Receive a nested list or tuple from a dictionary, execute
        the flattening process for each element, if element is list or tuple,
        then assign an id using the position, if element is dictionary, then
        execute the flattening process.
        Finally return a dictionary with the processed lists as values. 

        Parameters
        ----------
        nested_list : dict
        id_key : str
        black_list : List[str]
        dict_name : str
        dict_ref : str
        super_dict_ref_id : List[str]
        processed_data : dict

        Returns
        -------
        processed_data: dict
        """
        dict_list = []
        if snake_case_cols:
            dict_ref = self._to_snake_case(dict_ref, replace_dots)
        if isinstance(nested_list[0], type(dict)):
            for nested_dict in nested_list:
                aux, processed_data = self._processor(nested_dict, id_key, black_list, 
                                                         dict_name = dict_name, 
                                                         dict_ref = "", 
                                                         super_dict_ref_id = super_dict_ref_id, 
                                                         is_nested_list = True, 
                                                         processed_data = processed_data)
                dict_list.append( aux )
        else:
            for i in range(len(nested_list)):
                dict_list.append( {f"{super_dict_ref_id[0]}{id_key}": super_dict_ref_id[1], "id": i+1, f"{dict_ref}": nested_list[i]} )
        
        try:
            processed_data[f"{super_dict_ref_id[0]}{dict_ref}"].extend(dict_list)
        except:
            processed_data[f"{super_dict_ref_id[0]}{dict_ref}"] = dict_list

        return processed_data


    def _processor(self, dictionary:dict, id_key:str, black_list:List[str], snake_case_cols:bool, replace_dots:bool, 
                   dict_name:str = "", dict_ref:str = "", super_dict_ref_id:List[str] = [], is_nested_list:bool = False, 
                   processed_data:dict = {}) -> Tuple[dict, dict]:
        """Receive a dictionary and iterate over all the keys,
        if the key.value is a dict, then execute a new flattening
        process over the value, if list, then execute nested_processor,
        else add the value to the flattened dictionary, using the
        key reference location on the original dictionary.
        THis function is able to receive a black_list, where a list of the
        unwanted keys, and the flattening process will avoid them.

        Parameters
        ----------
        dictionary : dict
        id_key : str
        black_list : List[str], default is []
        dict_name : str, default is ''
        dict_ref : str, default is ''
        super_dict_ref_id : List[str], default is []
        processed_data : dict, default is {}

        Returns
        -------
        flat_dict, processed_data : Tuple[dict, dict]
        """
        flat_dict = {}
        if is_nested_list:
            flat_dict.update({f"{super_dict_ref_id[0]}{id_key}": super_dict_ref_id[1]})
        dict_ref += "." if dict_ref != '' else dict_ref

        for key, value in dictionary.items():
            if key not in black_list:
                if snake_case_cols:
                    key = self._to_snake_case(key, replace_dots)
                if (dict_ref == ''):
                    if dict_name:
                        dict_ref_id = [f"{dict_name.replace('-', '_')}.{dict_ref}", dictionary[id_key]]
                    else:
                        dict_ref_id = [dict_ref, dictionary[id_key]]
                else:
                    dict_ref_id = super_dict_ref_id

                if type(value) is dict:
                    aux, processed_data = self._processor(value, id_key, black_list,
                                                            dict_name = dict_name,
                                                            dict_ref = f"{dict_ref}{key}",
                                                            super_dict_ref_id = dict_ref_id,
                                                            is_nested_list = False,
                                                            processed_data = processed_data)
                    flat_dict.update(aux)
                elif type(value) in (list, tuple):
                    if value:
                        processed_data = self._nested_processor(value, id_key, black_list,
                                                                  dict_name = dict_name,
                                                                  dict_ref = f"{dict_ref}{key}",
                                                                  super_dict_ref_id = dict_ref_id,
                                                                  processed_data = processed_data)
                else:
                    flat_dict.update({f"{dict_ref}{key}": value})
        return flat_dict, processed_data


    def transform(self, dictionary:dict, id_key:str, black_list:List[str] = [], dict_name:str = "dct", snake_case_cols:bool = False, replace_dots:bool = False) -> dict:
        """Receive a dictionary and an id_key to start processing.
        The key names provided in the black_list will be
        avoided during the processing.
        In case a dict_name is provided, it will be appended
        at the start of the nested list,tuples if any.
        And return a dictionary of dictionaries with all the
        proccesed data.

        Parameters
        ----------
        dictionary : dict
        id_key : str
        black_list : List[str], default is []
        dict_name : str, default is 'dct'
        snake_case_cols : bool, default is False
        replace_dots : bool, default is False

        Returns
        -------
        processed_data : dict
        """
        if not id_key:
            raise FlatteningException("The id_key has not been provided, ensure the value is not an empty string or None.")
        elif not (id_key in dictionary):
            raise FlatteningException("The provided id_key does not exist on the dictionary.")
        elif not ( type(dictionary[id_key]) in (int, float, str) ):
            raise FlatteningException("The provided id_key must be a str, integer or float.")
        processed_data = {}
        flatten_dict, processed_data = self._processor(dictionary, id_key, black_list, dict_name, processed_data=processed_data, 
                                                       snake_case_cols=snake_case_cols, replace_dots=replace_dots)
        processed_data[dict_name] = [flatten_dict]
        return processed_data