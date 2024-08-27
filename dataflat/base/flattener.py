from dataclasses import dataclass
from dataflat.utils.case_translator import CustomCaseTranslator
from typing import Optional

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

    heritable_keys : list[str] (default = None)
        Keys or columns to be inherited
    """
    entity_name: str = "data"
    primary_key: str = "id"
    replace_string: str = "."
    heritable_keys: Optional[list[str]] = None
