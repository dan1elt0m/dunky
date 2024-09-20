from pyarrow_unity.model import UCSupportedFormatLiteral
from unitycatalog import Unitycatalog
from unitycatalog.types import GenerateTemporaryTableCredentialResponse
from unitycatalog.types.table_create_params import Column


def uc_table_exists(
    client: Unitycatalog, table_name: str, schema_name: str, catalog_name: str = "unity"
) -> bool:
    """Check if a UC table exists in the catalog."""

    table_list_request = client.tables.list(
        catalog_name=catalog_name, schema_name=schema_name
    )

    if not table_list_request.tables:
        return False

    return table_name in [table.name for table in table_list_request.tables]


def uc_schema_exists(
    client: Unitycatalog, schema_name: str, catalog_name: str = "unity"
) -> bool:
    """Check if a UC schema exists in the catalog."""
    schema_list_request = client.schemas.list(catalog_name=catalog_name)

    if not schema_list_request.schemas:
        return False

    return schema_name in [schema.name for schema in schema_list_request.schemas]


def uc_get_storage_credentials(
    client: Unitycatalog, catalog_name: str, schema_name: str, table_name: str
) -> dict:
    """Get temporary table credentials for a UC table if they exist."""

    # Get the table ID

    if not uc_table_exists(client, table_name, schema_name, catalog_name):
        return {}

    table_response = client.tables.retrieve(
        full_name=f"{catalog_name}.{schema_name}.{table_name}"
    )

    if not table_response.table_id:
        return {}

    # Get the temporary table credentials
    creds: GenerateTemporaryTableCredentialResponse = (
        client.temporary_table_credentials.create(
            operation="READ_WRITE", table_id=table_response.table_id
        )
    )

    if creds.aws_temp_credentials:
        return {
            "AWS_ACCESS_KEY_ID": creds.aws_temp_credentials.access_key_id,
            "AWS_SECRET_ACCESS_KEY": creds.aws_temp_credentials.secret_access_key,
            "AWS_SESSION_TOKEN": creds.aws_temp_credentials.session_token,
        }

    return {}


def create_table_if_not_exists(
    uc_client: Unitycatalog,
    table_name: str,
    schema_name: str,
    catalog_name: str,
    storage_location: str,
    schema: list[Column],
    storage_format: UCSupportedFormatLiteral,
):
    """Create or update a Unitycatalog table."""

    if not uc_schema_exists(uc_client, schema_name, catalog_name):
        uc_client.schemas.create(catalog_name=catalog_name, name=schema_name)

    if not uc_table_exists(uc_client, table_name, schema_name, catalog_name):
        uc_client.tables.create(
            catalog_name=catalog_name,
            columns=schema,
            data_source_format=storage_format,
            name=table_name,
            schema_name=schema_name,
            table_type="EXTERNAL",
            storage_location=storage_location,
        )
