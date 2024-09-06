# dataflat
A library to flatten all this annoiyng nested keys and columns on Dictionaries, Pandas Dataframes
and Spark (pyspark) Dataframes.

### Installation
```bash
pip install dataflat
```

### Get started
How to instantiate a Flattener:

First import the FlattenerOptions, CaseTranslator and Flattener classes
```Python
from dataflat.flattener_handler import CaseTranslatorOptions, FlattenerOptions, handler
```
The following step is to pass the required variables to the 


The following step is assing the required variables for the flattening process.
* reference_name: Used to assign a key name to each resulting dictionary or DataFrame from the transform function.
* id_key: Used as identifier value in case there is any nested list, that will be exploded to a new entire dictionary or dataframe, the exploded Dataframes will have a reference to it parent Dataframe or dictionary.
* black_list: A list of keys or columns that will be skipped during the flattening process.
* replace_dots: Specify if the dots (used as hierarchical separator) will be replaced with underscores.
* from_case, to_case: Specify the keys or columns current case (snake, camel...) and the desired output case for each key.

```Python
# Default values:
#   from_case = None
#   to_case = None
#   replace_string = "."
#   remove_special_chars = False
from_case = CaseTranslatorOptions.CAMEL
to_case = CaseTranslatorOptions.SNAKE
replace_string = "."
remove_special_chars = False
```

Later, select the desired flattener from the options, according to the current type of your data (dict, pandas.DataFrame, spark.DataFrame)
```Python
custom_flattener = FlattenerOptions.DICTIONARY
custom_flattener = FlattenerOptions.PANDAS_DF
custom_flattener = FlattenerOptions.SPARK_DF
```

Finally instantiate the flattener class, and apply the transform function.
```Python
flattener = handler(custom_flattener, from_case, to_case)


# Default values:
#   from_case = None
#   to_case = None
#   replace_string = "."
#   remove_special_chars = False
entity_name = "data"
primary_key = "id"
black_list = ['keys','or','columns','to','skip']
flattener_dict.transform(data, id_key, black_list, reference_name)
```

### Recommendations
1. For SPARK_DF flattener it;s recommended to set the 'caseSensitive' configuration to True on Spark.
    On some occasions two keys can be the same, and can only be differentiated due to a capital letter.
    ```Python
        spark.conf.set('spark.sql.caseSensitive', True)
    ```