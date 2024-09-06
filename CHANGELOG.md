## v2.0.0 (2024-09-06)

### BREAKING CHANGE

- deprecate pandas flattener,change variable names and delete unused variables from function calls

### Fix

- **dataflat**: fix error with spark and dictionary flattener, deprecate pandas flattener

## v1.1.2 (2024-09-03)

### Fix

- **base/flattener.py**: fix problem with flatten method, it should be abstract
- **commons.py**: a fix was made for a problem with "| None"

## v1.1.1 (2024-09-02)

### Refactor

- **dictionary/flattener.py**: fix sonarcloud issue with function complexity