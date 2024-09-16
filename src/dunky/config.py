import os
import re

from dbt.adapters.base import BaseRelation
from dbt.adapters.duckdb.utils import TargetConfig, TargetLocation

from dunky.plugin import get_path


class DunkyTargetConfig(TargetConfig):
    @classmethod
    def from_query(cls, query: str) -> "DunkyTargetConfig":
        # Extract table name and location from the query
        match = re.search(
            r"CREATE\s+EXTERNAL\s+TABLE\s+(\S+)\s+LOCATION\s+\'(\S+)\'(?:\s+OPTIONS\s+\((.*?)\))?\s+AS\s+SELECT",
            query,
            re.IGNORECASE,
        )
        if not match:
            raise ValueError("Invalid CREATE EXTERNAL TABLE AS SELECT query")

        table_name = match.group(1)
        table_location = match.group(2)
        options_str = match.group(3)
        storage_options = {"AWS_REGION": os.environ.get("AWS_REGION", "eu-west-1")}
        if options_str:
            for option in options_str.split(","):
                key, value = option.split("=")
                storage_options[key.strip()] = value.strip().strip("'\"")

        path = get_path(table_name)

        cls.catalog_name = path.database

        relation = BaseRelation(get_path(table_name))

        # Create the necessary objects
        location = TargetLocation(path=table_location, format="delta")
        config = {
            "mode": "overwrite",  # Default mode, can be adjusted as needed
            "schema": path.schema,  # Default schema, can be adjusted as needed
            "storage_options": storage_options,
        }

        return cls(relation=relation, location=location, config=config, column_list=[])
