import pandas
import pytest
from unittest.mock import patch, MagicMock
from dunky.dunky_kernel import (
    DunkyKernel,
    is_select_query,
    is_show_query,
    is_attach_query,
    is_detach_query,
    is_create_external_table_as_select_query,
)


@pytest.fixture
@patch("dunky.dunky_kernel.duckdb.connect")
def kernel(mock_connect):
    kernel = DunkyKernel()
    mock_connect.return_value = MagicMock()
    return kernel


def test_bootstrap(kernel):
    kernel._bootstrap()
    kernel._conn.sql.assert_any_call("""
        INSTALL uc_catalog from core_nightly;
        INSTALL delta from core;
        LOAD delta;
        LOAD uc_catalog;
        CREATE SECRET (
            TYPE UC,
            TOKEN 'not-used',
            ENDPOINT 'http://localhost:8080',
            AWS_REGION 'eu-west-1'
        );
        """)


def test_select_query_executes_correctly(kernel):
    query = "SELECT * FROM test_table"
    kernel._conn.sql.return_value.df.return_value = MagicMock(
        columns=pandas.array(data=["a"]),
        values=pandas.array(data=pandas.array(data=pandas.array([[1]]))),
    )
    kernel._run_select_query(query, silent=False)
    kernel._conn.sql.assert_called_with(query)


def test_show_query_executes_correctly(kernel):
    query = "SHOW TABLES"
    kernel._conn.sql.return_value.df.return_value = MagicMock(
        columns=["table_name"], values=[["test_table"]]
    )
    kernel._run_show_query(query, silent=False)
    kernel._conn.sql.assert_called_with(query)


def test_attach_query_executes_correctly(kernel):
    query = "ATTACH 'test.db' AS test_db"
    kernel._conn.sql.return_value = None
    kernel._run_attach_query(query, silent=False)
    kernel._conn.sql.assert_called_with(query)


def test_detach_query_executes_correctly(kernel):
    query = "DETACH DATABASE test_db"
    kernel._conn.sql.return_value = None
    kernel._run_detach_query(query, silent=False)
    kernel._conn.sql.assert_called_with(query)


def test_create_external_table_as_select_query_executes_correctly(kernel):
    query = "CREATE EXTERNAL TABLE a.b.test_table LOCATION 'somewhere' OPTIONS (FORMAT=DELTA, MODE=OVERWRITE) AS SELECT * FROM source_table"
    kernel._conn.sql.return_value.arrow.return_value = MagicMock()
    with patch("dunky.dunky_kernel.DunkyPlugin.store") as mock_store:
        kernel._run_create_external_table_as_select_query(query, silent=False)
        kernel._conn.sql.assert_any_call("DETACH DATABASE a;")
        kernel._conn.sql.assert_any_call("ATTACH 'a' AS a (TYPE UC_CATALOG);")
        kernel._conn.sql.assert_any_call("SELECT * FROM a.b.test_table LIMIT 1;")
        mock_store.assert_called()


def test_shell_command_executes_correctly(kernel):
    command = "echo 'Hello, World!'"
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(stdout="Hello, World!", stderr="")
        kernel._run_shell_command(command, silent=False)
        mock_run.assert_called_with(command, shell=True, capture_output=True, text=True)


def test_select_query_identified_correctly():
    query = "SELECT * FROM test_table"
    assert is_select_query(query)


def test_show_query_identified_correctly():
    query = "SHOW TABLES"
    assert is_show_query(query)


def test_attach_query_identified_correctly():
    query = "ATTACH 'test.db' AS test_db"
    assert is_attach_query(query)


def test_detach_query_identified_correctly():
    query = "DETACH DATABASE test_db"
    assert is_detach_query(query)


def test_create_external_table_as_select_query_identified_correctly():
    query = "CREATE EXTERNAL TABLE test_table AS SELECT * FROM source_table"
    assert is_create_external_table_as_select_query(query)


def test_create_external_table_location_as_select_query_identified_correctly():
    query = "CREATE EXTERNAL TABLE test_table LOCATION 's3://example' AS SELECT * FROM source_table"
    assert is_create_external_table_as_select_query(query)


def test_create_external_table_location_option_as_select_query_identified_correctly():
    query = "CREATE EXTERNAL TABLE test_table LOCATION 's3://example' OPTIONS (format=delta) AS SELECT * FROM source_table"
    assert is_create_external_table_as_select_query(query)
