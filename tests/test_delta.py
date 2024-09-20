import pytest
import pyarrow as pa
from deltalake._internal import TableNotFoundError
from dunky.delta import (
    delta_table_exists,
    create_insert_partition,
    delta_write,
    WriteModes,
    PartitionKeyMissingError,
    UniqueKeyMissingError,
)

def test_delta_table_exists_returns_true(mocker):
    mocker.patch("deltalake.DeltaTable", return_value=True)
    assert delta_table_exists("path", {}) is True

def test_delta_table_exists_returns_false(mocker):
    mocker.patch("deltalake.DeltaTable", side_effect=TableNotFoundError)
    assert delta_table_exists("path", {}) is False

def test_create_insert_partition_creates_table(mocker):
    mocker.patch("dunky.delta.delta_table_exists", return_value=False)
    mock_write = mocker.patch("deltalake.write_deltalake")
    create_insert_partition("path", pa.table({"a": [1]}), [("a", "1")], {})
    mock_write.assert_called_once()

def test_create_insert_partition_overwrites_table(mocker):
    mocker.patch("dunky.delta.delta_table_exists", return_value=True)
    mock_write = mocker.patch("deltalake.write_deltalake")
    create_insert_partition("path", pa.table({"a": [1]}), [("a", "1")], {})
    mock_write.assert_called_once()

def test_delta_write_raises_partition_key_missing_error():
    with pytest.raises(PartitionKeyMissingError):
        delta_write(WriteModes.OVERWRITE_PARTITION, "path", pa.table({"a": [1]}), {}, None, "unique_key")

def test_delta_write_raises_unique_key_missing_error():
    with pytest.raises(UniqueKeyMissingError):
        delta_write(WriteModes.MERGE, "path", pa.table({"a": [1]}), {}, "partition_key", None)

def test_delta_write_overwrites_table(mocker):
    mock_write = mocker.patch("deltalake.write_deltalake")
    delta_write(WriteModes.OVERWRITE, "path", pa.table({"a": [1]}), {}, "partition_key", "unique_key")
    mock_write.assert_called_once()

def test_delta_write_merges_table(mocker):
    mocker.patch("dunky.delta.delta_table_exists", return_value=True)
    mock_merge = mocker.patch("deltalake.DeltaTable")
    delta_write(WriteModes.MERGE, "path", pa.table({"a": [1]}), {}, "partition_key", "unique_key")
    mock_merge.assert_called_once()