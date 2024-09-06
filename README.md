# dataflat
A library to flatten all this annoiyng nested keys and columns on Dictionaries, Pandas Dataframes
and Spark (pyspark) Dataframes.

### Installation
```bash
pip install dataflat
```

### Get started
How to instantiate a Flattener:

1. Import CaseTranslatorOptions, and FlattenerOptions

In this step we define the case translator used to convert keys, or column names after the flattening process.

It's necessary to select the required Flattener, for example DICTIONARY or PYSPARK_DF (more coming...)
Secondly it's optional to select a from_case and a to_case option, for example SNAKE and CAMEL respectively.
Finally we need to set a replace string, this is the string used to indicate the nested dependency, for example
client.id or item.price, also we can specify if it's needed to remove special characters like '@' or '|'
keys or column names.

```Python
from dataflat.flattener_handler import CaseTranslatorOptions, FlattenerOptions

# Default values:
#   from_case = None
#   to_case = None
#   replace_string = "."
#   remove_special_chars = False
custom_flattener = FlattenerOptions.DICTIONARY
from_case = CaseTranslatorOptions.SNAKE
to_case = CaseTranslatorOptions.CAMEL
replace_string = "."
remove_special_chars = False
```

After that we can proceed to instantiate a flattener using the handler, and passing it the variables defined before, and 
flatten some data.
**All CustomFlattener receive the same parameters on the flatten function.**
```Python
from dataflat.flattener_handler import handler

flattener = handler(
    custom_flattener=custom_flattener,
    from_case=from_case,
    to_case=to_case,
    replace_string=replace_string,
    remove_special_chars=remove_special_chars
)


# Default values:
#   entity_name = "data"
#   primary_key = "id
#   partition_keys = []
#   black_list = []
data={}
entity_name = "data"
primary_key = "id"
partition_keys = ["date"]
black_list = ['keys.or', 'columns', 'to.be.ignored']
flatten_data = flattener.flatten(
    data=data,
    primary_key=primary_key, 
    entity_name=entity_name,
    partition_keys=partition_keys,
    black_list=black_list
)
```
* ```primary_key```: Used to connect dictionaries or dataframes with the "parent" dataframe, for example
```id```, its required that the dictionary or dataframe contains a 'primary_key' that can be propagated to 'child'.
* ```partition_keys```: List of keys that you must want to propagate to nested data, for example "date" key/column.
* ```black_list```: List of keys that must me ignored/skipped during the flattening process, this keys will not be
present on the flatten_data.
* ```flatten_data```: A dictionary with one or multiples keys, one for the "parent" and much for the "child".
Each list or array inside the "original" data will result on a key on this dictionary, an example of flatten_data could be:
```json
{
  "data": [{"id": 1, "date": "2024-01-01", "total": 1900}],
  "data.orders": [
    {"id": "abc123", "total": 700, "data.id": 1, "data.date": "2024-01-01", "index": 0},
    {"id": "dfg456", "total": 1200, "data.id": 1, "data.date": "2024-01-01", "index": 1}
  ],
  "data.orders.products": [
    {"id": "ab", "price": 200, "data.id": 1, "data.date": "2024-01-01", "data.orders.index": 0, "index": 0},
    {"id": "cd", "price": 500, "data.id": 1, "data.date": "2024-01-01", "data.orders.index": 0, "index": 1},
    {"id": "fg", "price": 1200, "data.id": 1, "data.date": "2024-01-01", "data.orders.index": 1, "index": 0}
  ]
}
```

The original json that result in this data was:
```json
{
  "id":  1,
  "date": "2024-01-01",
  "orders": [
    {
      "id":  "abc123",
      "products": [
        {"id":  "ab", "price":  200},
        {"id":  "cd", "price":  500}
      ],
      "total":  700
    },
    {
      "id": "dfg456",
      "products": [
        {"id":  "fg", "price":  1200}
      ],
      "total":  1200}
  ],
  "total":  1900
}
```

### Recommendations
1. For PYSPARK_DF flattener it's recommended to set the 'caseSensitive' configuration to True on Spark.
    ```Python
        spark.conf.set('spark.sql.caseSensitive', True)
    ```