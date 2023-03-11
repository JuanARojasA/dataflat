import enum
from typing import Type
from importlib import import_module
from pyflat.commons import init_logger


logger = init_logger(__name__)

class Flattener(enum.Enum): # Possible values
    DICTIONARY = 1
    PANDAS_DF = 2
    SPARK_DF = 3

class FlattenerHandler():
    def __init__(self):
        logger.info("Flattener Handler initiated")

    def pre_processor(self, flattener:Type[Flattener] = Flattener.DICTIONARY):
        """Returns relevant flattener

        Parameters
        ----------
        flattener_type : Type[Flattener], default is DICTIONARY
            Specify the Flattener class to use.

        Returns
        -------
        class -- CustomFlattener object 
        """
        pre_processor = "pyflat.{}".format(flattener.name.lower())

        return getattr(import_module(pre_processor), 'CustomFlattener')