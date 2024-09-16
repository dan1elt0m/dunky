import os
from dbt.adapters.duckdb.plugins.unity import Plugin
from dbt.adapters.base.relation import Path

from unitycatalog import Unitycatalog

from dbt.adapters.duckdb.plugins.unity import StorageFormat


def get_path(table_name: str) -> Path:
    parts = table_name.split(".")
    if len(parts) != 3:
        raise ValueError(
            "Table name must be in the format 'database.schema.identifier'"
        )
    return Path(database=parts[0], schema=parts[1], identifier=parts[2])


class DunkyPlugin(Plugin):
    catalog_name: str = "unity"

    # The default storage format
    default_format = StorageFormat.DELTA

    # The Unitycatalog client
    uc_client: Unitycatalog

    # The AWS region
    aws_region: str

    def initialize(self, credentials=None):
        uc_endpoint = os.environ.get("UC_ENDPOINT", "http://localhost:8080")
        base_url = f"{uc_endpoint}/api/2.1/unity-catalog"
        self.uc_client = Unitycatalog(base_url=base_url)
        self.aws_region = os.environ.get("UC_AWS_REGION", "eu-west-1")
