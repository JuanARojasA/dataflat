from dataclasses import dataclass
from dataflat.utils.case_translator import CustomCaseTranslator


@dataclass
class BaseFlattener:
    """
    Base class for Flattener Common interface for every CustomFlattener type.

    Parameters
    ----------
    entity_name : str (default = "data")
        Entity name of the data

    primary_key : str (default = "id")
        Primary key of the data

    replace_string : str (default = ".")
        String used to separate nested data

    case_translator : CustomCaseTranslator (default = None)
        Case translator used for rename columns

    heritable_keys : list[str] (default = None)
        Keys or columns to be inherited

    black_list : list[str] (default = None)
        Keys or columns to be ignored
    """
    entity_name: str = "data"
    primary_key: str = "id"
    replace_string: str = "."
    case_translator: CustomCaseTranslator = None
    heritable_keys: list[str] = None
    black_list: list[str] = None
