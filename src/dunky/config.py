import os
import re
from dataclasses import dataclass


@dataclass
class TableConfig:
    catalog_name: str
    schema_name: str
    table_name: str

def get_table_config(table_name: str) -> TableConfig:
    """"""
    parts = table_name.split(".")
    if len(parts) != 3:
        raise ValueError(
            "Table name must be in the format 'database.schema.identifier'"
        )
    return TableConfig(catalog_name=parts[0], schema_name=parts[1], table_name=parts[2])

@dataclass
class DunkyTargetConfig:
    table_config: TableConfig
    location: str
    storage_options: dict

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

        table_config = get_table_config(table_name)

        return cls(
            table_config=table_config,
            location=table_location,
            storage_options=storage_options,
        )
