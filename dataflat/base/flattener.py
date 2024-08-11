from dataclasses import dataclass


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

    heritable_keys : list[str] (default = None)
        Keys or columns to be inherited

    black_list : list[str] (default = None)
        Keys or columns to be ignored
    """
    entity_name: str = "data"
    primary_key: str = "id"
    heritable_keys: list[str] = None
    black_list: list[str] = None
