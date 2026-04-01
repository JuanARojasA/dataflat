"""
dataflat/polars/flattener.py - The processor script for Polars DataFrames flattening process

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
from typing import Optional, cast

import polars as pl
from pydantic import ConfigDict, validate_call

from dataflat.base_flattener import BaseFlattener
from dataflat.utils.logger import init_logger
from dataflat.utils.string import dot_join_args


logger = init_logger(__name__)

_FLATTEN_CONFIG = ConfigDict(arbitrary_types_allowed=True)


class CustomFlattener(BaseFlattener):
    # ------------------------------------------------------------------
    # Internal state initialisation
    # ------------------------------------------------------------------

    def _setup(
        self,
        primary_key: Optional[str] = None,
        entity_name: Optional[str] = None,
        partition_keys: Optional[list[str]] = None,
        black_list: Optional[list[str]] = None,
        white_list: Optional[list[str]] = None,
    ) -> None:
        super()._setup(primary_key, entity_name, partition_keys, black_list, white_list)
        self._result: dict[str, pl.DataFrame] = {}
        self._entity_inherited_columns: dict[str, list[str]] = {}

    # ------------------------------------------------------------------
    # Column-name translation helpers
    # ------------------------------------------------------------------

    def _apply_column_translate(self) -> None:
        if self.case_translator is not None:
            translated: dict[str, pl.DataFrame] = {}
            for df_name, df in self._result.items():
                rename_map = {col: self._process_strings(col) for col in df.columns}
                translated[self._process_strings(df_name)] = df.rename(rename_map)
            self._result = translated

    def _apply_white_list(self) -> None:
        if not self.white_list:
            return
        plan = self._compute_white_list_plan(
            list(self._result.keys()), self._entity_inherited_columns
        )
        new_result: dict[str, pl.DataFrame] = {}
        for entity_key, cols in plan.items():
            df = self._result[entity_key]
            if cols is not None:
                keep = [c for c in df.columns if c in cols]
                df = df.select(keep)
            new_result[entity_key] = df
        self._result = new_result

    # ------------------------------------------------------------------
    # Struct expansion
    # ------------------------------------------------------------------

    def _unnest_struct_col(self, df: pl.DataFrame, col: str) -> pl.DataFrame:
        new_names = [
            dot_join_args(col, f.name) for f in cast(pl.Struct, df.schema[col]).fields
        ]
        return df.with_columns(pl.col(col).struct.rename_fields(new_names)).unnest(col)

    def _expand_structs(self, df: pl.DataFrame, entity_name: str) -> pl.DataFrame:
        while True:
            struct_cols = [c for c, t in df.schema.items() if isinstance(t, pl.Struct)]
            if not struct_cols:
                break
            for col in struct_cols:
                if self._is_blacklisted(entity_name, col):
                    df = df.drop(col)
                else:
                    df = self._unnest_struct_col(df, col)
        return df

    # ------------------------------------------------------------------
    # Core recursive processor
    # ------------------------------------------------------------------

    def _process_df(
        self,
        df: pl.DataFrame,
        entity_name: str,
        inherited_cols: list[str],
        is_root: bool = False,
    ) -> None:
        # primary_key is always set to a non-None string before _process_df is called.
        assert self.primary_key is not None
        pk = self.primary_key

        # Track inherited columns for white_list support.
        self._entity_inherited_columns[entity_name] = (
            list(inherited_cols) if is_root else list(inherited_cols) + ["index"]
        )

        # 1. Expand every Struct column in-place (with dot-prefixed field names).
        df = self._expand_structs(df, entity_name)

        # 2. Collect list columns that are not blacklisted.
        list_cols = [
            col
            for col, dtype in df.schema.items()
            if isinstance(dtype, pl.List) and not self._is_blacklisted(entity_name, col)
        ]

        # 3. For each list column, build a child DataFrame and recurse.
        for list_col in list_cols:
            inner_dtype = cast(pl.List, df.schema[list_col]).inner

            if not isinstance(inner_dtype, pl.Struct):
                # Scalar list: join values with "|" into a string column in place.
                df = df.with_columns(
                    pl.col(list_col)
                    .cast(pl.List(pl.String))
                    .list.join("|", ignore_nulls=True)
                )
                continue

            child_name = dot_join_args(entity_name, list_col)

            if is_root:
                # Rename pk and partition_keys with the entity prefix for children.
                rename_map: dict[str, str] = {pk: dot_join_args(entity_name, pk)}
                for part_key in self.partition_keys:
                    rename_map[part_key] = dot_join_args(entity_name, part_key)

                child_df = df.select(inherited_cols + [list_col]).rename(rename_map)
                # The prefixed columns that will be inherited by grandchildren.
                child_pk_cols = [dot_join_args(entity_name, c) for c in inherited_cols]
            else:
                # Rename own "index" to "entity_name.index" before propagating.
                own_index_prefixed = dot_join_args(entity_name, "index")
                child_df = df.select(inherited_cols + ["index", list_col]).rename(
                    {"index": own_index_prefixed}
                )
                child_pk_cols = inherited_cols + [own_index_prefixed]

            # Explode the list column and drop null rows.
            child_df = child_df.explode(list_col).filter(pl.col(list_col).is_not_null())

            # Per-parent positional index (0-based, resets for each parent group).
            child_df = (
                child_df.with_row_index("_global_idx")
                .with_columns(
                    (
                        pl.col("_global_idx")
                        - pl.col("_global_idx")
                        .min()
                        .over([pl.col(c) for c in child_pk_cols])
                    ).alias("index")
                )
                .drop("_global_idx")
            )

            # Expand struct elements; scalar elements keep their original column name.
            # Use plain unnest (no field-name prefixing) so that the struct's own
            # field names are preserved.  Top-level struct columns inside those
            # fields will be prefixed correctly by _expand_structs when _process_df
            # recurses into the child entity.
            if isinstance(inner_dtype, pl.Struct):
                child_df = child_df.unnest(list_col)

            # Recurse into the child entity.
            self._process_df(child_df, child_name, child_pk_cols, is_root=False)

            # Remove the list column from the parent; data lives in the child.
            df = df.drop(list_col)

        # 4. Drop any remaining blacklisted scalar columns (inherited cols are
        #    always kept regardless of the black-list).
        blacklisted_scalars = [
            col
            for col in df.columns
            if col not in inherited_cols and self._is_blacklisted(entity_name, col)
        ]
        if blacklisted_scalars:
            df = df.drop(blacklisted_scalars)

        # 5. Store the fully-processed DataFrame for this entity.
        self._result[entity_name] = df

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @validate_call(config=_FLATTEN_CONFIG)
    def flatten(
        self,
        data: pl.DataFrame,
        primary_key: Optional[str] = None,
        entity_name: Optional[str] = None,
        partition_keys: Optional[list[str]] = None,
        black_list: Optional[list[str]] = None,
        white_list: Optional[list[str]] = None,
    ) -> dict[str, pl.DataFrame]:
        """Flatten a Polars DataFrame that may contain Struct and List columns.

        The input must be loadable by Polars (use ``pl.read_ndjson`` for data
        that contains nullable structs — see the notebooks/ example).

        Parameters
        ----------
        data:
            Root DataFrame to flatten.
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
        dict[str, pl.DataFrame]
            Mapping from entity name (dot-joined path) to its flattened
            DataFrame.  Every DataFrame carries the full chain of pk / index
            columns so parent–child relationships can be reconstructed.
        """
        self._setup(primary_key, entity_name, partition_keys, black_list, white_list)

        # Ensure the root DataFrame has a primary-key column.
        if self.primary_key is None:
            pk_col = self._dataflat_id_col_name
            data = data.with_columns(
                pl.Series(pk_col, [str(uuid.uuid4()) for _ in range(len(data))])
            )
            self.primary_key = pk_col
        elif self.primary_key not in data.columns:
            data = data.with_row_index(self.primary_key)

        root_inherited = [self.primary_key] + self.partition_keys
        self._process_df(data, self.entity_name, root_inherited, is_root=True)
        self._apply_white_list()
        self._apply_column_translate()
        return self._result
