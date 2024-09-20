import pytest
import pyarrow as pa
from dunky.store import store
from dunky.config import DunkyTargetConfig, TableConfig
from dunky.delta import PartitionKeyMissingError, UniqueKeyMissingError


def test_store_happy_path(mocker):
    mocker.patch("dunky.store.Unitycatalog")
    mock_delta_write = mocker.patch("dunky.store.delta_write")
    mock_create_table = mocker.patch("dunky.store.create_table_if_not_exists")
    mocker.patch(
        "dunky.store.uc_get_storage_credentials", return_value={}
    )

    target_config = DunkyTargetConfig(
        location="s3://bucket/path",
        table_config=TableConfig("catalog", "schema", "table"),
        storage_options={"mode": "overwrite"},
    )
    df = pa.table({"a": [1, 2, 3]})

    store(target_config, df)

    mock_create_table.assert_called_once()
    mock_delta_write.assert_called_once()


def test_store_fails(mocker):
    mock_uc_client = mocker.patch("dunky.store.Unitycatalog")
    mock_delta_write = mocker.patch("dunky.store.delta_write", side_effect=Exception)
    mock_create_table = mocker.patch("dunky.store.create_table_if_not_exists")
    mocker.patch(
        "dunky.store.uc_get_storage_credentials", return_value={}
    )

    target_config = DunkyTargetConfig(
        location="s3://bucket/path",
        table_config=TableConfig("catalog", "schema", "table"),
        storage_options={"mode": "overwrite"},
    )
    df = pa.table({"a": [1, 2, 3]})

    with pytest.raises(Exception):
        store(target_config, df)

        mock_create_table.assert_called_once()

        mock_delta_write.assert_called_once()
        mock_uc_client.delete_table.assert_called_once()


def test_store_missing_table_location():
    target_config = DunkyTargetConfig(
        location=None,
        table_config=TableConfig("catalog", "schema", "table"),
        storage_options={"mode": "overwrite"},
    )
    df = pa.table({"a": [1, 2, 3]})

    with pytest.raises(AssertionError, match="Location is required for storing data!"):
        store(target_config, df)


def test_store_partition_key_missing_error(mocker):
    mocker.patch("dunky.store.Unitycatalog")
    mocker.patch(
        "dunky.store.delta_write", side_effect=PartitionKeyMissingError
    )
    mocker.patch("dunky.store.create_table_if_not_exists")
    mocker.patch(
        "dunky.store.uc_get_storage_credentials", return_value={}
    )

    target_config = DunkyTargetConfig(
        location="s3://bucket/path",
        table_config=TableConfig("catalog", "schema", "table"),
        storage_options={"mode": "overwrite"},
    )
    df = pa.table({"a": [1, 2, 3]})

    with pytest.raises(PartitionKeyMissingError):
        store(target_config, df)


def test_store_unique_key_missing_error(mocker):
    mocker.patch("dunky.store.Unitycatalog")
    mocker.patch(
        "dunky.store.delta_write", side_effect=UniqueKeyMissingError
    )
    mocker.patch("dunky.store.create_table_if_not_exists")
    mocker.patch(
        "dunky.store.uc_get_storage_credentials", return_value={}
    )

    target_config = DunkyTargetConfig(
        location="s3://bucket/path",
        table_config=TableConfig("catalog", "schema", "table"),
        storage_options={"mode": "overwrite"},
    )
    df = pa.table({"a": [1, 2, 3]})

    with pytest.raises(UniqueKeyMissingError):
        store(target_config, df)
