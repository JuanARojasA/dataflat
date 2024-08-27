'''
dataflat/dictionary/flattener.py - The processor script for dictionaries flattening process

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
from typing import Tuple, List, Any, Union

from dataflat.base.flattener import BaseFlattener
from dataflat.commons import init_logger
from dataflat.exceptions import FlatteningException
from dataflat.utils.case_translator import CustomCaseTranslator


logger = init_logger(__name__)

@typechecked
class CustomFlattener(BaseFlattener):
    def __init__(self, case_translator:CustomCaseTranslator, replace_string :str):
        logger.info("CustomFlattener for Python Dictionaries has been initiated")
        self._case_translator = case_translator
        self._replace_string = replace_string
        self._processed_data = {}


    def _replace_dots(self, string:str):
        if self._replace_string != ".":
            string = string.replace(".", self._replace_string)
            string = re.sub(f"\\{self._replace_string}\\{self._replace_string}+", self._replace_string, string)
        return string


    def _insert_child_record(self, processed_data:dict, json_name:str, key_ref:str, value:Any,
                             nested_index:Union[None,int]=None,
                             parent_index:Union[Tuple[None,None], Tuple[str,None], Tuple[str,int]]=(None,None)):
        if nested_index is not None:
            if parent_index[1] is not None:
                parent_index_name = f"{parent_index[0]}{self._replace_string}index"
                if processed_data[json_name]:
                    try:
                        if processed_data[json_name][parent_index[1]]:
                            try:
                                processed_data[json_name][parent_index[1]][nested_index][key_ref] = value
                            except:

                                processed_data[json_name][parent_index[1]].append({parent_index_name:parent_index[1],key_ref:value})
                        else:
                            processed_data[json_name][parent_index[1]] = [{parent_index_name:parent_index[1],key_ref:value}]
                    except Exception as e:
                        processed_data[json_name].update({parent_index[1]:[{parent_index_name:parent_index[1],key_ref:value}]})
                else:
                    processed_data[json_name] = {}
                    processed_data[json_name].update({parent_index[1]:[{parent_index_name:parent_index[1],key_ref:value}]})
            else:
                try:
                    processed_data[json_name][nested_index][key_ref] = value
                except:
                    if not processed_data[json_name]:
                        processed_data[json_name] = {}
                    processed_data[json_name].update({nested_index:{key_ref:value}})
        else:
            try:
                processed_data[json_name][key_ref] = value
            except:
                processed_data[json_name] = [{key_ref:value}]
        return processed_data


    def _processor(self, data:Any, json_name: str, ref: str = "", processed_data = None,
                   nested_index:Union[None,int]=None,
                   parent_index:Union[Tuple[None,None], Tuple[str,None], Tuple[str,int]]=(None,None)):
        processed_data = {} if processed_data is None else processed_data

        processed_data[json_name] = {} if json_name not in processed_data else processed_data[json_name]
        if isinstance(data, dict):
            for key, value in data.items():
                if key not in self.black_list:
                    key_name = self._case_translator.translate(key)
                    key_ref = f"{ref}.{key_name}" if ref!="" else key_name
                    key_ref = self._replace_dots(key_ref)
                    if isinstance(value, dict):
                        self._processor(value, json_name, key_ref, processed_data, nested_index, parent_index)
                    elif type(value) in (list, tuple):
                        nested_json_name = self._replace_dots(f"{json_name}.{key_ref}")
                        self._processor(value, nested_json_name, "", processed_data, nested_index, (json_name, None))
                    else:
                        self._insert_child_record(processed_data, json_name, key_ref, value, nested_index, parent_index)
        elif type(data) in (list, tuple):
            if nested_index is not None:
                parent_index = (parent_index[0], nested_index)
            for index, item in enumerate(data):
                if isinstance(item, dict):
                    self._processor(item, json_name, "", processed_data, index, parent_index)
                else:
                    processed_data = self._insert_child_record(processed_data, json_name, "value", item, index, parent_index)
        else:
            logger.warning(f"Non-supported data type: {type(data)} with data {data}")
        return processed_data

    def transform(
            self, json_data: dict[str, Any], primary_key:str,
            black_list:List[str] = None, json_name: str = "json"
    ) -> dict[str, Any]:
        if not json_data:
            raise FlatteningException("The provided dictionary is empty.")
        elif primary_key not in json_data:
            raise FlatteningException(f"The provided id_key={primary_key} does not exist in json_data")
        
        self.black_list = [] if black_list is None else black_list
        json_name = self._case_translator.translate(json_name)
        self._processed_data = self._processor(json_data, json_name, processed_data=self._processed_data)
        for dict_name, flattened_dicts in self._processed_data.items():
            if dict_name != json_name:
                dict_list = []
                parent_json_id = self._replace_dots(f"{json_name}{self._replace_string}{primary_key}" )
                for index in flattened_dicts.keys():
                    if isinstance(flattened_dicts[index], list):
                        for sub_index, sub_dict in enumerate(flattened_dicts[index]):
                            sub_dict['index'] = sub_index
                            sub_dict[parent_json_id] = self._processed_data[json_name][primary_key]
                            dict_list.append(sub_dict)
                    else:
                        flattened_dicts[index]['index'] = index
                        flattened_dicts[index][parent_json_id] = self._processed_data[json_name][primary_key]
                        dict_list.append(flattened_dicts[index])
                self._processed_data[dict_name] = dict_list
        return self._processed_data
