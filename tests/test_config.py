import pytest
from dunky.config import DunkyTargetConfig


def test_from_query_parses_correct_query():
    query = "CREATE EXTERNAL TABLE database.schema.table LOCATION 's3://bucket/path' OPTIONS (AWS_REGION='us-west-2') AS SELECT * FROM source_table"
    config = DunkyTargetConfig.from_query(query)
    assert config.table_config.catalog_name == "database"
    assert config.table_config.schema_name == "schema"
    assert config.table_config.table_name == "table"
    assert config.location == "s3://bucket/path"
    assert config.storage_options["AWS_REGION"] == "us-west-2"


def test_from_query_raises_value_error_for_invalid_query():
    with pytest.raises(ValueError):
        DunkyTargetConfig.from_query("INVALID QUERY")


def test_from_query_uses_default_aws_region():
    query = "CREATE EXTERNAL TABLE database.schema.table LOCATION 's3://bucket/path' AS SELECT * FROM source_table"
    config = DunkyTargetConfig.from_query(query)
    assert config.storage_options["AWS_REGION"] == "eu-west-1"


def test_from_query_parses_options_correctly():
    query = "CREATE EXTERNAL TABLE database.schema.table LOCATION 's3://bucket/path' OPTIONS (option1='value1', option2='value2') AS SELECT * FROM source_table"
    config = DunkyTargetConfig.from_query(query)
    assert config.storage_options["option1"] == "value1"
    assert config.storage_options["option2"] == "value2"
