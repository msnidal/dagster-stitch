from dagster import AssetKey, AssetMaterialization
from typing import Iterator, Sequence

from dagster import AssetMaterialization, MetadataValue
from dagster._core.definitions.metadata.table import TableColumn, TableSchema

from dagster_stitch.types import StitchOutput

import requests


class BearerAuth(requests.auth.AuthBase):
    """Attaches Bearer Authentication to the given Request object."""

    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers["authorization"] = f"Bearer {self.token}"
        return r


def get_stitch_connector_url(
    account_id: str, data_source_id: str, stream_id: str = None, stream_name: str = None
) -> str:
    """Get the Stitch connector URL from the environment.

    Args:
        account_id (str): The Stitch account ID.
        data_source_id (str): The Stitch data source ID.
        stream_id (str): The Stitch stream ID.

    Returns:
        str: The Stitch connector URL.
    """
    base_url = f"https://app.stitchdata.com/client/{account_id}/pipeline/connections/{data_source_id}/data/"

    if stream_id:
        return f"{base_url}properties/{stream_id}/{stream_name}/"
    return base_url


def generate_materializations(
    stitch_output: StitchOutput, asset_key_prefix: Sequence[str]
) -> Iterator[AssetMaterialization]:
    """Generate Dagster materializations for all of the tables (streams) in the Stitch output.

    Args:
        stitch_output (StitchOutput): The Stitch output to generate materializations for.
        asset_key_prefix (Sequence[str]): The prefix to use for the asset keys of the materializations

    Yields:
        Iterator[AssetMaterialization]: The materializations for the tables in the Stitch output.
    """
    # extraction_completion_time = stitch_output.extraction_details["completion_time"]
    account_id = stitch_output.extraction_details["stitch_client_id"]
    data_source_id = stitch_output.extraction_details["source_id"]
    data_source_name = stitch_output.source_metadata["name"]

    for stream_id, stream_properties in stitch_output.stream_schema.items():
        stream_name = stream_properties["name"]

        stream_url = get_stitch_connector_url(account_id, data_source_id, stream_id, stream_name)

        metadata = {"connector_url": MetadataValue.url(stream_url)}
        table_columns = []

        for column, column_type in stream_properties["schema"].items():
            table_columns.append(TableColumn(name=column, type=column_type))

        metadata["table_schema"] = MetadataValue.table_schema(TableSchema(table_columns))
        asset_key = AssetKey([*asset_key_prefix, data_source_name, stream_name])

        materialization = AssetMaterialization(
            asset_key=asset_key,
            description=f"Table generated via Stitch sync: {data_source_name}.{stream_name}",
            metadata=metadata,
        )
        yield materialization
