import os
import logging
import pyarrow as pa
from unitycatalog import Unitycatalog
from pyarrow_unity.model import model_unity_schema

from dunky.config import DunkyTargetConfig
from dunky.delta import delta_write
from dunky.unity import uc_get_storage_credentials, create_table_if_not_exists

logger = logging.getLogger(__name__)


def store(target_config: DunkyTargetConfig, df: pa.lib.Table = None):
    """Store the given pyarrow table as delta table and create the table in the Unitycatalog."""
    base_url = f"{os.environ.get('UC_ENDPOINT', 'http://localhost:808')}/api/2.1/unity-catalog"
    uc_client = Unitycatalog(
        base_url=base_url,
        default_headers={"Authorization": f"Bearer {os.environ.get('UC_TOKEN')}"},

    )
    assert (
        target_config.location is not None
    ), "Location is required for storing data!"
    table_config = target_config.table_config

    # Get required variables from the target configuration
    table_path = target_config.location
    table_name = table_config.table_name

    # Get optional variables from the target configuration
    mode = target_config.storage_options.get("mode", "overwrite")

    storage_options = target_config.storage_options
    partition_key = storage_options.get("partition_key", None)
    unique_key = storage_options.get("unique_key", None)
    # extend the storage options with the aws region
    storage_options["AWS_REGION"] = os.environ.get("AWS_REGION", "eu-west-1")

    # Convert the pa schema to columns
    converted_schema = model_unity_schema(schema=df.schema)

    # Create the table in the Unitycatalog if it does not exist
    create_table_if_not_exists(
        uc_client=uc_client,
        table_name=table_name,
        schema_name=table_config.schema_name,
        catalog_name=table_config.catalog_name,
        storage_location=table_path,
        schema=converted_schema,
        storage_format="DELTA",
    )

    # extend the storage options with the temporary table credentials
    storage_options = storage_options | uc_get_storage_credentials(
        uc_client, table_config.catalog_name, table_config.schema_name, table_name
    )

    try:
        delta_write(
            mode=mode,
            table_path=table_path,
            df=df,
            storage_options=storage_options,
            partition_key=partition_key,
            unique_key=unique_key,
        )
    except Exception as e:
        # If the write fails, delete the table from the Unitycatalog
        logger.info("Write of delta table failed. Deleting table from Unitycatalog.")
        uc_client.tables.delete(
            full_name=f"{table_config.catalog_name}.{table_config.schema_name}.{table_name}"
        )
        raise e
