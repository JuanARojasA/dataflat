# dataflat
A library to flatten all this annoiyng nested keys and columns on Dictionaries, Pandas Dataframes
and Spark (pyspark) Dataframes.

### Installation
```
pip install dataflat
```

### Get started
How to instantiate a Flattener:

```Python
from dataflat.flattener_handler import Options, Flattener

id_key = 'id'
black_list = ['keys','or','columns','to','skip']

#The following variables have the default values for each transform function
to_snake_case = False
replace_dots = False
dict_name = 'dct'
chunk_size = 500
dataframe_name = 'df'

# Instantiate a Dictionary Flattener
flattener_dict = Flattener().handler(Options.DICTIONARY)
flattener.transform(dictionary, id_key, black_list, dict_name, to_snake_case, replace_dots)


# Instantiate a Pandas Dataframe Flattener
flattener_pd = Flattener().handler(Options.PANDAS_DF)
transform(dataframe, id_key, black_list, dataframe_name, chunk_size, to_snake_case, replace_dots)

# Instantiate a Spark Dataframe Flattener
flattener_sp = Flattener().handler(Options.SPARK_DF)
transform(dataframe, id_key, black_list, dataframe_name, to_snake_case, replace_dots)
```