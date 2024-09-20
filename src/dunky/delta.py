from enum import Enum

import deltalake as dl
import pyarrow as pa
from deltalake._internal import TableNotFoundError


class WriteModes(str, Enum):
    """Enum class for the write modes supported by the plugin."""

    OVERWRITE_PARTITION = "overwrite_partition"
    MERGE = "merge"
    OVERWRITE = "overwrite"


class PartitionKeyMissingError(Exception):
    """Exception raised when the partition key is missing from the target configuration."""

    pass


class UniqueKeyMissingError(Exception):
    """Exception raised when the unique key is missing from the target configuration."""

    pass


class DeltaTablePathMissingError(Exception):
    """Exception raised when the delta table path is missing from the source configuration."""

    pass


def delta_table_exists(table_path: str, storage_options: dict) -> bool:
    """Check if a delta table exists at the given path."""
    try:
        dl.DeltaTable(table_path, storage_options=storage_options)
    except TableNotFoundError:
        return False
    return True


def create_insert_partition(
    table_path: str, data: pa.lib.Table, partitions: list, storage_options: dict
) -> None:
    """Create a new delta table with partitions or overwrite an existing one."""

    if delta_table_exists(table_path, storage_options):
        partition_expr = [
            (partition_name, "=", partition_value)
            for (partition_name, partition_value) in partitions
        ]
        print(
            f"Overwriting delta table under: {table_path} \nwith partition expr: {partition_expr}"
        )
        dl.write_deltalake(
            table_path, data, partition_filters=partition_expr, mode="overwrite"
        )
    else:
        partitions = [
            partition_name for (partition_name, partition_value) in partitions
        ]
        print(
            f"Creating delta table under: {table_path} \nwith partitions: {partitions}"
        )
        dl.write_deltalake(table_path, data, partition_by=partitions)


def delta_write(
    mode: WriteModes,
    table_path: str,
    df: pa.lib.Table,
    storage_options: dict,
    partition_key: str | list[str],
    unique_key: str | list[str],
):
    if storage_options is None:
        storage_options = {}

    if mode == WriteModes.OVERWRITE_PARTITION:
        if not partition_key:
            raise PartitionKeyMissingError(
                "'partition_key' has to be defined for mode 'overwrite_partition'!"
            )

        if isinstance(partition_key, str):
            partition_key = [partition_key]

        partition_dict = []
        for each_key in partition_key:
            unique_key_array = pa.compute.unique(df[each_key])

            if len(unique_key_array) == 1:
                partition_dict.append((each_key, str(unique_key_array[0])))
            else:
                raise Exception(
                    f"'{each_key}' column has not one unique value, values are: {str(unique_key_array)}"
                )
        create_insert_partition(table_path, df, partition_dict, storage_options)
    elif mode == WriteModes.MERGE:
        if not unique_key:
            raise UniqueKeyMissingError(
                "'unique_key' has to be defined when mode 'merge'!"
            )
        if isinstance(unique_key, str):
            unique_key = [unique_key]

        predicate_stm = " and ".join(
            [
                f'source."{each_unique_key}" = target."{each_unique_key}"'
                for each_unique_key in unique_key
            ]
        )

        if not delta_table_exists(table_path, storage_options):
            dl.write_deltalake(
                table_or_uri=table_path, data=df, storage_options=storage_options
            )

        target_dt = dl.DeltaTable(table_path, storage_options=storage_options)

        target_dt.merge(
            source=df,
            predicate=predicate_stm,
            source_alias="source",
            target_alias="target",
        ).when_not_matched_insert_all().execute()
    elif mode == WriteModes.OVERWRITE:
        dl.write_deltalake(
            table_or_uri=table_path,
            data=df,
            mode="overwrite",
            storage_options=storage_options,
        )
    else:
        raise NotImplementedError(f"Mode {mode} not supported!")
