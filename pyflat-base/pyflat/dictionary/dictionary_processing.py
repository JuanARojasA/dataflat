import re
from typing import Tuple, List
from pyflat.commons import init_logger
from pyflat.exceptions import FlatteningException


logger = init_logger(__name__)

class CustomFlattener():
    def __init__(self):
        logger.info("CustomFlattener for Python Dictionaries has been initiated")

    def _to_snake_case(self, string:str) -> str:
        """Convert the input string to snake_case string

        Parameters
        ----------
        string : str

        Returns
        -------
        str 
        """
        pattern = re.compile(r'(?<!^)(?=[A-Z])')
        return pattern.sub('_', string).lower().replace("__", "_")


    def _nested_processing(self, nested_list:dict, id_key:str, black_list:List[str], dict_name:str, 
                           dict_ref:str, super_dict_ref_id:List[str], processed_dicts:dict) -> dict:
        """Receive a nested list or tuple from a dictionary, execute
        the flattening process for each element, if element is list or tuple,
        then assign an id using the position, if element is dictionary, then
        execute the flattening process.
        Finally return a dictionary with the processed lists as values. 

        Parameters
        ----------
        nested_list : dict
        id_key: str
        black_list: List[str]
        dict_name: str
        dict_ref: str
        super_dict_ref_id: List[str]
        processed_dicts: dict

        Returns
        -------
        processed_dicts: dict
        """
        dict_list = []
        if isinstance(nested_list[0], type(dict)):
            for nested_dict in nested_list:
                aux, processed_dicts = self._processing(nested_dict, id_key, black_list, 
                                                         dict_name = dict_name, 
                                                         dict_ref = "", 
                                                         super_dict_ref_id = super_dict_ref_id, 
                                                         is_nested_list = True, 
                                                         processed_dicts = processed_dicts)
                dict_list.append( aux )
        else:
            for i in range(len(nested_list)):
                dict_list.append( {f"{super_dict_ref_id[0]}{id_key}": super_dict_ref_id[1], "id": i+1, f"{self._to_snake_case(dict_ref.split('.')[-1])}": nested_list[i]} )
        
        try:
            processed_dicts[f"{super_dict_ref_id[0]}{self._to_snake_case(dict_ref.split('.')[-1])}"].extend(dict_list)
        except:
            processed_dicts[f"{super_dict_ref_id[0]}{self._to_snake_case(dict_ref.split('.')[-1])}"] = dict_list

        return processed_dicts


    def _processing(self, dictionary:dict, id_key:str, black_list:List[str] = [], dict_name:str = "",
                    dict_ref:str = "", super_dict_ref_id:List[str] = [], is_nested_list:bool = False, processed_dicts:dict = {}) -> Tuple[dict, dict]:
        """Receive a dictionary and iterate over all the keys,
        if the key.value is a dict, then execute a new flattening
        process over the value, if list, then execute a nested_processing,
        else add the value to the flattened dictionary, using the
        key reference location on the original dictionary.
        THis function is able to receive a black_list, where a list of the
        unwanted keys, and the flattening process will avoid them.

        Parameters
        ----------
        dictionary : dict
        id_key: str
        black_list: List[str], default is []
        dict_name: str, default is ''
        dict_ref: str, default is ''
        super_dict_ref_id: List[str], default is []
        processed_dicts: dict, default is {}

        Returns
        -------
        flat_dict, processed_dicts: Tuple[dict, dict]
        """
        flat_dict = {}
        if is_nested_list:
            flat_dict.update({f"{super_dict_ref_id[0]}{id_key}": super_dict_ref_id[1]})
        dict_ref += "." if dict_ref != '' else dict_ref

        for key, value in dictionary.items():
            if key not in black_list:
                if (dict_ref == ''):
                    if dict_name:
                        dict_ref_id = [f"{dict_name.replace('-', '_')}.{dict_ref}", dictionary[id_key]]
                    else:
                        dict_ref_id = [dict_ref, dictionary[id_key]]
                else:
                    dict_ref_id = super_dict_ref_id

                if type(value) is dict:
                    aux, processed_dicts = self._processing(value, id_key, black_list,
                                                            dict_name = dict_name,
                                                            dict_ref = f"{dict_ref}{self._to_snake_case(key)}",
                                                            super_dict_ref_id = dict_ref_id,
                                                            is_nested_list = False,
                                                            processed_dicts = processed_dicts)
                    flat_dict.update(aux)
                elif type(value) in (list, tuple):
                    if value:
                        processed_dicts = self._nested_processing(value, id_key, black_list,
                                                                  dict_name = dict_name,
                                                                  dict_ref = f"{dict_ref}{key}",
                                                                  super_dict_ref_id = dict_ref_id,
                                                                  processed_dicts = processed_dicts)
                else:
                    flat_dict.update({f"{dict_ref}{self._to_snake_case(key)}": value})
        return flat_dict, processed_dicts


    def transform(self, dictionary:dict, id_key:str, black_list:List[str] = [], dict_name:str = "") -> Tuple[dict, dict]:
        """Receive a dictionary and an id_key to start processing.
        The key names provided in the black_list will be
        avoided during the processing.
        In case a dict_name is provided, it will be appended
        at the start of the nested list,tuples if any.

        Parameters
        ----------
        dictionary : dict
        id_key: str
        black_list: List[str], default is []
        dict_name: str, default is ''

        Returns
        -------
        flat_dict, processed_dicts: Tuple[dict, dict]
        """
        if not id_key:
            raise FlatteningException("The id_key has not been provided, ensure the value is not an empty string or None.")
        elif not (id_key in dictionary):
            raise FlatteningException("The provided id_key does not exist on the dictionary.")
        elif not ( type(dictionary[id_key]) in (int, float, str) ):
            raise FlatteningException("The provided id_key must be a str, integer or float.")

        processed_dicts = {}
        return self._processing(dictionary, id_key, black_list, dict_name, processed_dicts=processed_dicts)