import os
import requests_mock
from unitycatalog import Unitycatalog
from unitycatalog.types import (
    TableListResponse,
    SchemaListResponse,
    TableInfo,
    SchemaInfo,
)

from dunky.unity import (
    uc_table_exists,
    uc_schema_exists,
    uc_get_storage_credentials,
    create_table_if_not_exists,
fetch_s3_create_table_credentials
)


def test_uc_table_exists_returns_true(mocker):
    mock_client = mocker.Mock(spec=Unitycatalog)
    mock_client.tables = mocker.Mock()
    mock_client.tables.list.return_value = TableListResponse(
        tables=[TableInfo(name="table_name")]
    )
    assert uc_table_exists(mock_client, "table_name", "schema_name") is True


def test_uc_table_exists_returns_false(mocker):
    mock_client = mocker.Mock(spec=Unitycatalog)
    mock_client.tables = mocker.Mock()
    mock_client.tables.list.return_value = TableListResponse(tables=[])
    assert uc_table_exists(mock_client, "table_name", "schema_name") is False


def test_uc_schema_exists_returns_true(mocker):
    mock_client = mocker.Mock(spec=Unitycatalog)
    mock_client.schemas = mocker.Mock()
    mock_client.schemas.list.return_value = SchemaListResponse(
        schemas=[SchemaInfo(name="schema_name")]
    )
    assert uc_schema_exists(mock_client, "schema_name") is True


def test_uc_schema_exists_returns_false(mocker):
    mock_client = mocker.Mock(spec=Unitycatalog)
    mock_client.schemas = mocker.Mock()
    mock_client.schemas.list.return_value = SchemaListResponse(schemas=[])
    assert uc_schema_exists(mock_client, "schema_name") is False


def test_uc_get_storage_credentials_returns_credentials(mocker):
    mock_client = mocker.Mock(spec=Unitycatalog)
    mock_client.tables = mocker.Mock()
    mock_client.tables.list.return_value = TableListResponse(
        tables=[TableInfo(table_id="table_id")]
    )
    mocker.patch("dunky.unity.uc_table_exists", return_value=True)
    mock_creds = mocker.Mock(
        aws_temp_credentials=mocker.Mock(
            access_key_id="access_key_id",
            secret_access_key="secret_access_key",
            session_token="session_token",
        )
    )
    mock_client.temporary_table_credentials = mocker.Mock()
    mock_client.temporary_table_credentials.create.return_value = mock_creds
    creds = uc_get_storage_credentials(
        mock_client, "catalog_name", "schema_name", "table_name"
    )
    mock_client.temporary_table_credentials.create.assert_called_once()
    assert creds == {
        "AWS_ACCESS_KEY_ID": "access_key_id",
        "AWS_SECRET_ACCESS_KEY": "secret_access_key",
        "AWS_SESSION_TOKEN": "session_token",
    }


def test_uc_get_storage_credentials_returns_empty(mocker):
    mock_client = mocker.Mock(spec=Unitycatalog)
    mock_client.tables = mocker.Mock()
    mock_client.tables.list.return_value = TableListResponse(
        tables=[TableInfo(table_id=None)]
    )
    creds = uc_get_storage_credentials(
        mock_client, "catalog_name", "schema_name", "table_name"
    )
    assert creds == {}


def test_create_table_if_not_exists_creates_schema_and_table(mocker):
    mock_client = mocker.Mock(spec=Unitycatalog)
    mock_client.schemas = mocker.Mock()
    mock_client.tables = mocker.Mock()
    mocker.patch("dunky.unity.uc_schema_exists", return_value=False)
    mocker.patch("dunky.unity.uc_table_exists", return_value=False)
    create_table_if_not_exists(
        mock_client,
        "table_name",
        "schema_name",
        "catalog_name",
        "s3://bucket/path",
        [],
        "PARQUET",
    )
    mock_client.schemas.create.assert_called_once()
    mock_client.tables.create.assert_called_once()


def test_create_table_if_not_exists_creates_table_only(mocker):
    mock_client = mocker.Mock(spec=Unitycatalog)
    mock_client.schemas = mocker.Mock()
    mock_client.tables = mocker.Mock()
    mocker.patch("dunky.unity.uc_schema_exists", return_value=True)
    mocker.patch("dunky.unity.uc_table_exists", return_value=False)
    create_table_if_not_exists(
        mock_client,
        "table_name",
        "schema_name",
        "catalog_name",
        "s3://bucket/path",
        [],
        "PARQUET",
    )
    mock_client.schemas.create.assert_not_called()
    mock_client.tables.create.assert_called_once()


def test_create_table_if_not_exists_does_nothing(mocker):
    mock_client = mocker.Mock(spec=Unitycatalog)

    mock_client.schemas = mocker.Mock()
    mock_client.tables = mocker.Mock()
    mocker.patch("dunky.unity.uc_schema_exists", return_value=True)
    mocker.patch("dunky.unity.uc_table_exists", return_value=True)
    create_table_if_not_exists(
        mock_client,
        "table_name",
        "schema_name",
        "catalog_name",
        "s3://bucket/path",
        [],
        "PARQUET",
    )
    mock_client.schemas.create.assert_not_called()
    mock_client.tables.create.assert_not_called()


def test_fetch_s3_create_table_credentials():
    endpoint = "http://localhost:8080/api/2.1/unity-catalog"
    path = "s3://bucket/path"
    operation = "PATH_CREATE_TABLE"
    token = "test_token"
    os.environ["UC_TOKEN"] = token

    credentials_response = {
        "aws_temp_credentials": {
            "access_key_id": "test_access_key_id",
            "secret_access_key": "test_secret_access_key",
            "session_token": "test_session_token"
        }
    }

    with requests_mock.Mocker() as m:
        m.post(f"{endpoint}/temporary-path-credentials", json=credentials_response)
        credentials = fetch_s3_create_table_credentials(endpoint, path, operation)

    assert credentials == {
        "AWS_REGION": "eu-west-1",
        "AWS_ACCESS_KEY_ID": "test_access_key_id",
        "AWS_SECRET_ACCESS_KEY": "test_secret_access_key",
        "AWS_SESSION_TOKEN": "test_session_token",
    }
