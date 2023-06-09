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

# Instantiate a Dictionary Flattener
dict_name = 'dct'
flattener_dict = Flattener().handler(Options.DICTIONARY)
flattener_dict.transform(dictionary, id_key, black_list, dict_name, to_snake_case, replace_dots)


# Instantiate a Pandas Dataframe Flattener
##Default chunk size = 500
dataframe_name = 'df'
chunk_size = 500
flattener_pd = Flattener().handler(Options.PANDAS_DF)
flattener_pd.transform(dataframe, id_key, black_list, dataframe_name, to_snake_case, replace_dots, chunk_size)

# Instantiate a Spark Dataframe Flattener
dataframe_name = 'df'
flattener_sp = Flattener().handler(Options.SPARK_DF)
flattener_sp.transform(dataframe, id_key, black_list, dataframe_name, to_snake_case, replace_dots)
```