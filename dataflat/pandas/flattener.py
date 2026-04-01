"""
dataflat/pandas/flattener.py - The processor script for Pandas DataFrames flattening process

Copyright (C) 2024 Juan ROJAS
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Authors:
    Juan ROJAS <jarojasa97@gmail.com>
"""

import uuid
import warnings
from typing import Optional

import pandas as pd
import pyarrow as pa
from pydantic import ConfigDict, validate_call

from dataflat.pyarrow._base import _PyArrowBaseFlattener
from dataflat.utils.logger import init_logger


logger = init_logger(__name__)

_FLATTEN_CONFIG = ConfigDict(arbitrary_types_allowed=True)


class CustomFlattener(_PyArrowBaseFlattener):
    @validate_call(config=_FLATTEN_CONFIG)
    def flatten(
        self,
        data: pd.DataFrame,
        primary_key: Optional[str] = None,
        entity_name: Optional[str] = None,
        partition_keys: Optional[list[str]] = None,
        black_list: Optional[list[str]] = None,
        white_list: Optional[list[str]] = None,
    ) -> dict[str, pd.DataFrame]:
        """Flatten a Pandas DataFrame that may contain nested dicts and lists.

        The input DataFrame is first converted to a PyArrow Table to reliably
        distinguish column types.  With the numpy_nullable dtype_backend, string,
        dict, and list columns all appear as ``object`` dtype; the PyArrow
        conversion infers the correct struct and list types automatically.

        Parameters
        ----------
        data:
            Root DataFrame to flatten.  May use any dtype_backend; the
            conversion to PyArrow handles the normalisation internally.
        primary_key:
            Column name to use as the root primary key.  When absent (or when
            the column does not exist in ``data``), a sequential index is added
            automatically as ``{primary_key}``.
        entity_name:
            Name prefix for the root entity; defaults to ``"data"``.
        partition_keys:
            Additional root columns (e.g. ``["date"]``) that are inherited by
            all child DataFrames, renamed with the entity prefix.
        black_list:
            Dot-separated field paths whose values should be excluded from all
            output DataFrames, e.g. ``["totalOrders", "summary.totalClients"]``.
        white_list:
            Dot-separated paths that select which entities and/or columns to
            retain after flattening, e.g. ``["orders.items", "summary.total_revenue"]``.
            Entity-level entries keep the full entity and all descendants;
            column-level entries narrow the parent entity to inherited join
            columns plus the specified column.  An empty list (default) keeps
            everything.

        Returns
        -------
        dict[str, pd.DataFrame]
            Mapping from entity name (dot-joined path) to its flattened
            DataFrame.  All columns use the PyArrow dtype_backend
            (``pd.ArrowDtype``) for consistent type representation.
            Every DataFrame carries the full chain of pk / index columns so
            parent–child relationships can be reconstructed.
        """
        self._setup(primary_key, entity_name, partition_keys, black_list, white_list)

        # Convert to PyArrow Table: this is the "change dtype_backend to pyarrow"
        # step.  pa.Table.from_pandas correctly infers struct and list types from
        # object columns, which the numpy_nullable backend cannot distinguish.
        table = pa.Table.from_pandas(data, preserve_index=False)
        # Strip pandas metadata (dtype strings like 'struct<...>[pyarrow]' stored
        # for ArrowDtype input columns).  Leaving the metadata would cause
        # to_pandas(types_mapper=pd.ArrowDtype) to attempt restoring the original
        # complex type from its string representation on every derived table,
        # which fails for deeply nested types.
        table = table.replace_schema_metadata({})

        # Ensure the root Table has a primary-key column.
        if self.primary_key is None:
            pk_col = self._dataflat_id_col_name
            table = table.append_column(
                pa.field(pk_col, pa.string()),
                pa.array(
                    [str(uuid.uuid4()) for _ in range(len(table))], type=pa.string()
                ),
            )
            self.primary_key = pk_col
        elif self.primary_key not in table.schema.names:
            table = table.append_column(
                pa.field(self.primary_key, pa.int64()),
                pa.array(range(len(table)), type=pa.int64()),
            )

        root_inherited = [self.primary_key] + self.partition_keys
        self._process_table(table, self.entity_name, root_inherited, is_root=True)
        self._apply_white_list()
        self._apply_column_translate()

        # Convert each pa.Table result back to a pandas DataFrame backed by
        # the PyArrow dtype_backend so callers get proper nullable typed columns.
        # Suppress Pandas4Warning: raised by some pandas/PyArrow version combinations
        # when using types_mapper=pd.ArrowDtype; the behaviour is intentional here.
        with warnings.catch_warnings():
            if hasattr(pd.errors, "Pandas4Warning"):
                warnings.filterwarnings("ignore", category=pd.errors.Pandas4Warning)
            return {
                name: t.to_pandas(types_mapper=pd.ArrowDtype)
                for name, t in self._result.items()
            }
