"""
dataflat/pyarrow/flattener.py - The processor script for PyArrow Tables flattening process

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
from typing import Optional

import pyarrow as pa

from dataflat.pyarrow._base import _PyArrowBaseFlattener
from dataflat.utils.logger import init_logger


logger = init_logger(__name__)


class CustomFlattener(_PyArrowBaseFlattener):
    logger.info("CustomFlattener for PyArrow Tables has been initiated")

    def flatten(
        self,
        data: pa.Table,
        primary_key: Optional[str] = None,
        entity_name: Optional[str] = None,
        partition_keys: Optional[list[str]] = None,
        black_list: Optional[list[str]] = None,
    ) -> dict[str, pa.Table]:
        """Flatten a PyArrow Table that may contain Struct and List columns.

        Parameters
        ----------
        data:
            Root Table to flatten.
        primary_key:
            Column name to use as the root primary key.  When absent a
            sequential integer index is added automatically.
        entity_name:
            Name prefix for the root entity; defaults to ``"data"``.
        partition_keys:
            Additional root columns (e.g. ``["date"]``) that are inherited by
            all child Tables, renamed with the entity prefix.
        black_list:
            Dot-separated field paths whose values should be excluded from all
            output Tables, e.g. ``["totalOrders", "summary.totalClients"]``.

        Returns
        -------
        dict[str, pa.Table]
            Mapping from entity name (dot-joined path) to its flattened Table.
            Every Table carries the full chain of pk / index columns so
            parent–child relationships can be reconstructed.
        """
        self._setup(primary_key, entity_name, partition_keys, black_list)

        # Ensure the root Table has a primary-key column.
        if self.primary_key is None:
            pk_col = self._dataflat_id_col_name
            data = data.append_column(
                pa.field(pk_col, pa.string()),
                pa.array(
                    [str(uuid.uuid4()) for _ in range(len(data))], type=pa.string()
                ),
            )
            self.primary_key = pk_col
        elif self.primary_key not in data.schema.names:
            data = data.append_column(
                pa.field(self.primary_key, pa.int64()),
                pa.array(range(len(data)), type=pa.int64()),
            )

        root_inherited = [self.primary_key] + self.partition_keys
        self._process_table(data, self.entity_name, root_inherited, is_root=True)
        self._apply_column_translate()
        return self._result
