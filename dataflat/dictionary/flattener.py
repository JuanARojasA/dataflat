'''
dictionary/flattener.py - The processor script for dictionaries flattening process

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
from typing import Tuple, List, Union, Any
from dataflat.commons import init_logger
from dataflat.exceptions import FlatteningException


logger = init_logger(__name__)

@typechecked
class CustomFlattener():
    def __init__(self):
        logger.info("CustomFlattener for Python Dictionaries has been initiated")


    def _process_key_name(self, string:str, to_snake_case:bool, replace_dots:bool) -> str:
        """Receive an input string and process it to Snake Case, and
        replace the dots with underscores.

        Parameters
        ----------
        string: str
            The String to be processed.
        to_snake_case: bool
            If True process the string to Snake Case.
        replace_dots: bool
            If True process the string and replace dots with underscores.

        Returns
        -------
        string: str
            The processed string.
        """
        if to_snake_case:
            pattern = re.compile(r'(?<!^)(?=[A-Z])')
            string = pattern.sub('_', string).lower().replace("__", "_").replace("._", ".")
        if replace_dots:
            string = string.replace(".", "_")
        return 


    def _nested_processor(self, nested_list:Union[List,Tuple], id_key:str, black_list:List[str], dict_name:str, 
                           dict_ref:str, super_dict_ref_id:List[str], 
                           processed_data:dict, to_snake_case:bool, replace_dots:bool) -> dict:
        """Receive a nested list or tuple from a dictionary, execute
        the flattening process for each element, if the first element is a list or a tuple,
        then assign an 'id' using the position of each element, if element type is dictionary, then
        execute the flattening process.
        Finally return a dictionary with the processed lists as values.
        All elements in the nested_list must be of the same data type.

        Parameters
        ----------
        nested_list: Union[List,Tuple]
            The nested list of a dictionary to be processed.
        id_key: str
            The id key to be used as reference to the parent dictionary.
        black_list: List[str]
            A list of keys to ignore and not to add to the resulting flattened dictionary.
        dict_name: str
            A reference name for the dictionary, used to difference each
            resulting dictionary in the processed_data return.
        dict_ref: str
            The schema reference location for each key. 
            i.e. data.user, all the keys under the above reference will be named as data.user.XXXX (i.e. data.user.name)
        super_dict_ref_id: List[str]
            A list with the reference to the parent id_key.
            i.e. [user_id, dda00276-c90f-11ed-afa1-0242ac120002] is the reference for the user.addresses list.
        processed_data: dict
            A dictionary with all the flattened dictionaries as key-value pairs [str, dict].
        to_snake_case: bool
            If True process the key name to Snake Case.
        replace_dots: bool
            If True replace the key name dots with underscores.

        Returns
        -------
        processed_data: dict
            A dictionary with all the flattened lists as key-value pairs [str, dict].
        """
        dict_list = []
        dict_ref = self._process_key_name(dict_ref, to_snake_case, replace_dots)
        if isinstance(nested_list[0], type(dict)):
            for nested_dict in nested_list:
                aux, processed_data = self._processor(nested_dict, id_key, black_list, 
                                                         dict_name = dict_name, 
                                                         dict_ref = "", 
                                                         super_dict_ref_id = super_dict_ref_id, 
                                                         _is_nested_list = True, 
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


    def _processor(self, dictionary:dict, id_key:str, black_list:List[str], to_snake_case:bool, replace_dots:bool, 
                   dict_name:str = "", dict_ref:str = "", super_dict_ref_id:List[str] = [], _is_nested_list:bool = False, 
                   processed_data:dict = {}) -> Tuple[dict, dict]:
        """Receive a dictionary and iterate over all the keys,
        if the key.value is a dictionary, then execute a new flattening
        process over the value, if list, then execute nested_processor,
        else add the value to the flattened dictionary, using the
        key reference location on the original dictionary.
        If a black_list is provided then all the keys inside the black_list will
        be skipped. Notice that if ['name'] is provided as black list, then 
        all the 'name' keys will be skipped, even if they are under a nested
        dictionary or list.

        Parameters
        ----------
        dictionary: dict
            The dictionary to be flattened.
        id_key: str
            The id key to be used as reference to the parent dictionary.
        black_list: List[str], (default [])
            A list of keys to ignore and not to add to the resulting flattened dictionary.
        to_snake_case: bool
            If True process the key name to Snake Case.
        replace_dots: bool
            If True replace the key name dots with underscores.
        dict_name: str, (default '')
            A reference name for the dictionary, used to difference each
            resulting dictionary in the processed_data return.
        dict_ref: str, (default '')
            The schema reference location for each key. 
            i.e. user_data, all the keys under the above reference will be named as user_data.XXXX (i.e. user_data.name)
        super_dict_ref_id: List[str], default is []
            A list with the reference to the parent id_key.
            i.e. [user_id, dda00276-c90f-11ed-afa1-0242ac120002] is the reference for the user.addresses list.
        processed_data: dict, default is {}
            A dictionary with all the flattened dictionaries as key-value pairs [str, dict].

        Returns
        -------
        flat_dict, processed_data: Tuple[dict, dict]
            A dictionary with all the flattened dictionaries as key-value pairs [str, dict].
        """
        flat_dict = {}
        if _is_nested_list:
            flat_dict.update({f"{super_dict_ref_id[0]}{id_key}": super_dict_ref_id[1]})
        dict_ref += "." if dict_ref != '' else dict_ref

        for key, value in dictionary.items():
            if key not in black_list:
                key = self._process_key_name(key, to_snake_case, replace_dots)
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
                                                            _is_nested_list = False,
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


    def transform(self, dictionary:dict, id_key:Any, black_list:List[str] = [], dict_name:str = "dct", to_snake_case:bool = False, replace_dots:bool = False) -> dict:
        """Receive a dictionary to flatten and a id_key used as 
        reference key under all the resulting flattened dictionaries.
        Receive a dictionary and an id_key to start processing.
        If a black_list is provided then all the keys inside the black_list will
        be skipped. Notice that if ['name'] is provided as black list, then 
        all the 'name' keys will be skipped, even if they are under a nested
        dictionary or list.
        The dict_name will be used to reference each resulting flattened dictionary
        under the processed_data dictionary returned.

        Parameters
        ----------
        dictionary: dict
            The dictionary to be flattened.
        id_key: str
            The id key to be used as reference to the parent dictionary.
        black_list: List[str], (default [])
            A list of keys to ignore and not to add to the resulting flattened dictionary.
        dict_name : str, (default 'dct')
            A reference name for the dictionary, used to difference each
            resulting dictionary in the processed_data return.
        to_snake_case: bool
            If True process the key name to Snake Case.
        replace_dots: bool
            If True replace the key name dots with underscores.

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
                                                       to_snake_case=to_snake_case, replace_dots=replace_dots)
        processed_data[dict_name] = [flatten_dict]
        return processed_data