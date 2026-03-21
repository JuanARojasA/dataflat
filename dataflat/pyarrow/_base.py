"""
dataflat/pyarrow/_base.py - Shared PyArrow Table processing used by the PyArrow and
Pandas flatteners.  Both convert their input to a pa.Table and run the same
struct-expansion / list-explosion algorithm defined here.

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

from typing import Optional

import pyarrow as pa
import pyarrow.compute as pc

from dataflat.base_flattener import BaseFlattener
from dataflat.utils.string import dot_join_args


def _rename_columns(table: pa.Table, rename_map: dict[str, str]) -> pa.Table:
    """Return a new table with columns renamed according to rename_map."""
    new_names = [rename_map.get(name, name) for name in table.schema.names]
    return table.rename_columns(new_names)


class _PyArrowBaseFlattener(BaseFlattener):
    """Shared PyArrow Table flattening logic.

    Subclasses implement ``flatten()`` to handle input/output conversion;
    all internal struct-expansion and list-explosion processing lives here.
    """

    # ------------------------------------------------------------------
    # Internal state initialisation
    # ------------------------------------------------------------------

    def _setup(
        self,
        primary_key: Optional[str] = None,
        entity_name: Optional[str] = None,
        partition_keys: Optional[list[str]] = None,
        black_list: Optional[list[str]] = None,
    ) -> None:
        self.primary_key = primary_key
        self.entity_name = entity_name if entity_name else self.entity_name
        self.partition_keys = partition_keys if partition_keys else []
        self.black_list = black_list if black_list is not None else []
        self._result: dict[str, pa.Table] = {}

    # ------------------------------------------------------------------
    # Column-name translation helpers
    # ------------------------------------------------------------------

    def _apply_column_translate(self) -> None:
        if self.case_translator is not None:
            translated: dict[str, pa.Table] = {}
            for entity_name, table in self._result.items():
                new_names = [self._process_strings(col) for col in table.schema.names]
                translated[self._process_strings(entity_name)] = table.rename_columns(
                    new_names
                )
            self._result = translated

    # ------------------------------------------------------------------
    # Black-list helper
    # ------------------------------------------------------------------

    def _is_blacklisted(self, entity_name: str, col: str) -> bool:
        return any(
            dot_join_args(entity_name, col).endswith(item) for item in self.black_list
        )

    # ------------------------------------------------------------------
    # Struct expansion
    # ------------------------------------------------------------------

    def _unnest_struct_col(self, table: pa.Table, col: str) -> pa.Table:
        """Prefix every field of a Struct column with `col.` and unnest it."""
        struct_type = table.schema.field(col).type
        struct_arr = table.column(col)
        col_idx = table.schema.get_field_index(col)
        table = table.remove_column(col_idx)
        for i in range(struct_type.num_fields):
            field = struct_type.field(i)
            new_name = dot_join_args(col, field.name)
            arr = pc.struct_field(struct_arr, field.name)  # pyrefly: ignore[missing-attribute]
            table = table.append_column(pa.field(new_name, field.type), arr)
        return table

    def _expand_structs(self, table: pa.Table, entity_name: str) -> pa.Table:
        """Repeatedly expand all top-level Struct columns until none remain.

        Blacklisted struct columns are dropped instead of expanded so their
        child fields never appear in the output.
        """
        while True:
            struct_cols = [
                name
                for name in table.schema.names
                if pa.types.is_struct(table.schema.field(name).type)
            ]
            if not struct_cols:
                break
            for col in struct_cols:
                if self._is_blacklisted(entity_name, col):
                    col_idx = table.schema.get_field_index(col)
                    table = table.remove_column(col_idx)
                else:
                    table = self._unnest_struct_col(table, col)
        return table

    # ------------------------------------------------------------------
    # List explosion
    # ------------------------------------------------------------------

    def _explode(
        self,
        table: pa.Table,
        col: str,
        inner_type: pa.DataType,
    ) -> tuple[pa.Table, pa.Array]:
        """Explode a list column into one row per element.

        Returns the exploded table (with the list column replaced by its
        elements) and a per-parent positional index Array (0-based, resets
        for each parent row).  Null list rows and null elements are dropped.
        """
        list_arr = table.column(col)
        if isinstance(list_arr, pa.ChunkedArray):
            list_arr = list_arr.combine_chunks()

        # Drop rows where the list is null or empty.
        # lengths is annotated as pa.Array to collapse the Int32Array | Int64Array union
        # that list_value_length returns; without it pyrefly picks the Scalar overload of
        # pc.greater (stubs limitation) and infers BooleanScalar instead of BooleanArray.
        lengths: pa.Array = pc.list_value_length(list_arr)  # pyrefly: ignore[missing-attribute]
        lengths_filled: pa.Array = lengths.fill_null(0)
        has_items: pa.lib.BooleanArray = pc.greater(lengths_filled, pa.scalar(0))  # pyrefly: ignore[bad-argument-type,bad-assignment,missing-attribute]
        table = table.filter(has_items)
        list_arr = table.column(col)
        if isinstance(list_arr, pa.ChunkedArray):
            list_arr = list_arr.combine_chunks()

        # Flatten all list values into one contiguous array (may include nulls).
        flat_values = pc.list_flatten(list_arr)  # pyrefly: ignore[missing-attribute]
        valid_mask = pc.is_valid(flat_values)  # pyrefly: ignore[missing-attribute]
        valid_list = valid_mask.to_pylist()

        # Build repeat_indices (which parent row each element comes from) and
        # group_indices (0-based position within that parent), skipping nulls.
        row_lengths = [ln or 0 for ln in pc.list_value_length(list_arr).to_pylist()]  # pyrefly: ignore[missing-attribute]
        repeat_indices: list[int] = []
        group_indices: list[int] = []
        flat_idx = 0
        for i, length in enumerate(row_lengths):
            group_count = 0
            for _ in range(length):
                if valid_list[flat_idx]:
                    repeat_indices.append(i)
                    group_indices.append(group_count)
                    group_count += 1
                flat_idx += 1

        repeat_arr = pa.array(repeat_indices, type=pa.int64())
        index_arr = pa.array(group_indices, type=pa.int64())
        valid_flat = flat_values.filter(valid_mask)

        # Repeat parent rows to match the (non-null) element count.
        exploded = table.take(repeat_arr)
        col_idx = exploded.schema.get_field_index(col)

        if pa.types.is_struct(inner_type):
            # Plain unnest: use the struct's own field names (no prefix).
            # Top-level struct columns inside those fields will be prefixed
            # correctly by _expand_structs when _process_table recurses.
            exploded = exploded.remove_column(col_idx)
            for i in range(inner_type.num_fields):
                field = inner_type.field(i)
                arr = pc.struct_field(valid_flat, field.name)  # pyrefly: ignore[missing-attribute]
                exploded = exploded.append_column(pa.field(field.name, field.type), arr)
        else:
            exploded = exploded.set_column(col_idx, col, valid_flat)

        return exploded, index_arr

    # ------------------------------------------------------------------
    # Core recursive processor
    # ------------------------------------------------------------------

    def _process_table(
        self,
        table: pa.Table,
        entity_name: str,
        inherited_cols: list[str],
        is_root: bool = False,
    ) -> None:
        """Expand structs, extract every List column as a child Table, recurse.

        Parameters
        ----------
        table:
            The Table to process at this level of the hierarchy.
        entity_name:
            Dot-joined path that identifies this entity, e.g. ``data.orders.items``.
        inherited_cols:
            For the root entity: the raw pk/partition-key column names.
            For non-root entities: the already-prefixed ancestor key columns.
            These are selected and propagated into every child Table.
        is_root:
            True only for the root call.  The root's inherited columns have
            raw names (``id``, ``date``) that must be renamed with the entity
            prefix (``data.id``, ``data.date``) when building child Tables.
        """
        # primary_key is always set to a non-None string before _process_table is called.
        assert self.primary_key is not None
        pk = self.primary_key

        # 1. Expand every Struct column in-place (with dot-prefixed field names).
        table = self._expand_structs(table, entity_name)

        # 2. Collect list columns that are not blacklisted.
        list_cols = [
            col
            for col in table.schema.names
            if pa.types.is_list(table.schema.field(col).type)
            and not self._is_blacklisted(entity_name, col)
        ]

        # 3. For each list column, build a child Table and recurse.
        for list_col in list_cols:
            child_name = dot_join_args(entity_name, list_col)
            inner_type = table.schema.field(list_col).type.value_type

            if is_root:
                # Rename pk and partition_keys with the entity prefix for children.
                rename_map: dict[str, str] = {
                    pk: dot_join_args(entity_name, pk)
                }
                for part_key in self.partition_keys:
                    rename_map[part_key] = dot_join_args(entity_name, part_key)

                child_table = table.select(inherited_cols + [list_col])
                child_table = _rename_columns(child_table, rename_map)
                child_pk_cols = [dot_join_args(entity_name, c) for c in inherited_cols]
            else:
                # Rename own "index" to "entity_name.index" before propagating.
                own_index_prefixed = dot_join_args(entity_name, "index")
                child_table = table.select(inherited_cols + ["index", list_col])
                child_table = _rename_columns(
                    child_table, {"index": own_index_prefixed}
                )
                child_pk_cols = inherited_cols + [own_index_prefixed]

            # Explode and compute per-parent positional index.
            child_table, index_arr = self._explode(child_table, list_col, inner_type)
            child_table = child_table.append_column(
                pa.field("index", pa.int64()), index_arr
            )

            # Recurse into the child entity.
            self._process_table(child_table, child_name, child_pk_cols, is_root=False)

            # Remove the list column from the parent; data lives in the child.
            col_idx = table.schema.get_field_index(list_col)
            table = table.remove_column(col_idx)

        # 4. Drop any remaining blacklisted scalar columns (inherited cols kept).
        for col in [
            c
            for c in table.schema.names
            if c not in inherited_cols and self._is_blacklisted(entity_name, c)
        ]:
            col_idx = table.schema.get_field_index(col)
            table = table.remove_column(col_idx)

        # 5. Store the fully-processed Table for this entity.
        self._result[entity_name] = table
