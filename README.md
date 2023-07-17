# dataflat
A library to flatten all this annoiyng nested keys and columns on Dictionaries, Pandas Dataframes
and Spark (pyspark) Dataframes.

### Installation
```
pip install dataflat
```

### Get started
How to instantiate a Flattener:

First import the FlattenerOptions, CaseTranslator and Flattener classes
```Python
from dataflat.flattener_handler import FlattenerOptions, CaseTranslatorOptions, Flattener
```

The following step is assing the required variables for the flattening process.
* reference_name: Used to assign a key name to each resulting dictionary or DataFrame from the transform function.
* id_key: Used as identifier value in case there is any nested list, that will be exploded to a new entire dictionary or dataframe, the exploded Dataframes will have a reference to it parent Dataframe or dictionary.
* black_list: A list of keys or columns that will be skipped during the flattening process.
* replace_dots: Specify if the dots (used as hierarchical separator) will be replaced with underscores.
* from_case, to_case: Specify the keys or columns current case (snake, camel...) and the desired output case for each key.

```Python
# Default values:
#   from_case = CaseTranslatorOptions.CAMEL_CASE
#   to_case = CaseTranslatorOptions.SNAKE_CASE
#   replace_dots = False
#   chunk size = 500
reference_name = 'data'
id_key = 'id'
black_list = ['keys','or','columns','to','skip']
replace_dots = False
from_case = CaseTranslatorOptions.CAMEL_CASE
to_case = CaseTranslatorOptions.SNAKE_CASE
```

Later, select the desired flattener from the options, according to the current type of your data (dict, pandas.DataFrame, spark.DataFrame)
```Python
custom_flattener = FlattenerOptions.DICTIONARY
custom_flattener = FlattenerOptions.PANDAS_DF
custom_flattener = FlattenerOptions.SPARK_DF
```

Finally instantiate the flattener class, and apply the transform function.
```Python
flattener = Flattener().handler(custom_flattener, from_case, to_case, replace_dots)
flattener_dict.transform(data, id_key, black_list, reference_name)
```