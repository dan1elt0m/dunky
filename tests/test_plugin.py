import pytest
from unittest.mock import patch

from dunky.plugin import DunkyPlugin, get_path


@pytest.fixture()
def plugin():
    return DunkyPlugin(name="dunky", plugin_config={})


def test_path_parses_correctly():
    table_name = "database.schema.identifier"
    path = get_path(table_name)
    assert path.database == "database"
    assert path.schema == "schema"
    assert path.identifier == "identifier"


def test_path_raises_value_error_for_invalid_format():
    with pytest.raises(ValueError):
        get_path("invalid_table_name")


@patch("dunky.plugin.Unitycatalog")
def test_plugin_initializes_correctly(mock_unitycatalog, plugin):
    with patch.dict(
        "os.environ",
        {"UC_ENDPOINT": "http://test-endpoint", "UC_AWS_REGION": "us-east-1"},
    ):
        plugin.initialize()
        mock_unitycatalog.assert_called_with(
            base_url="http://test-endpoint/api/2.1/unity-catalog"
        )
        assert plugin.aws_region == "us-east-1"


@patch("dunky.plugin.Unitycatalog")
def test_plugin_initializes_with_default_values(mock_unitycatalog, plugin):
    with patch.dict("os.environ", {}, clear=True):
        plugin.initialize()
        mock_unitycatalog.assert_called_with(
            base_url="http://localhost:8080/api/2.1/unity-catalog"
        )
