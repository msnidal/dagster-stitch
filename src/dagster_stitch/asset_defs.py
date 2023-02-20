from typing import Optional, Sequence, Mapping
from dagster import AssetKey, AssetOut, AssetsDefinition, _check as check, multi_asset, Output

from dagster._core.definitions.metadata import MetadataUserInput
from dagster._core.definitions.resource_definition import ResourceDefinition

from dagster_stitch.resources import DEFAULT_POLL_INTERVAL
from dagster_stitch.utils import generate_materializations


def build_stitch_assets(
    data_source_id: str,
    destination_tables: Sequence[str],
    poll_interval: Optional[int] = DEFAULT_POLL_INTERVAL,
    extraction_timeout: Optional[int] = None,
    load_timeout: Optional[int] = None,
    io_manager_key: Optional[str] = None,
    asset_key_prefix: Optional[Sequence[str]] = None,
    metadata_by_table_name: Optional[Mapping[str, MetadataUserInput]] = None,
    table_to_asset_key_map: Optional[Mapping[str, AssetKey]] = None,
    resource_defs: Optional[Mapping[str, ResourceDefinition]] = None,
    group_name: Optional[str] = None,
) -> Sequence[AssetsDefinition]:
    """Build a set of assets for a given Stitch data source.

    Args:
        data_source_id (str): The Stitch data source ID.
        destination_tables (Sequence[str]): The destination tables to sync.
        poll_interval (Optional[int], optional): The interval to poll the Stitch API for job status.
            Defaults to DEFAULT_POLL_INTERVAL.
        extraction_timeout (Optional[int], optional): The maximum time to wait for the extraction
            stage to complete. Defaults to None.
        load_timeout (Optional[int], optional): The maximum time to wait for the load stage to
            complete. Defaults to None.
        io_manager_key (Optional[str], optional): The key of the io_manager to use for the
            materializations. Defaults to None.
        asset_key_prefix (Optional[Sequence[str]], optional): The prefix to use for the asset keys
            of the materializations. Defaults to None.
        metadata_by_table_name (Optional[Mapping[str, MetadataUserInput]], optional): A mapping
            from table name to metadata to use for the materializations. Defaults to None.
        table_to_asset_key_map (Optional[Mapping[str, AssetKey]], optional): A mapping from table
            name to asset key to use for the materializations. Defaults to None.
        resource_defs (Optional[Mapping[str, ResourceDefinition]], optional): A mapping from resource
            key to resource definition to use for the assets. Defaults to None.
        group_name (Optional[str], optional): The group name to use for the assets. Defaults to
            None.

    Returns:
        Sequence[AssetsDefinition]: A sequence of assets modeling the given data source.
    """
    asset_key_prefix = check.opt_sequence_param(asset_key_prefix, "asset_key_prefix", of_type=str)
    tracked_asset_keys = {
        table: AssetKey([*asset_key_prefix, *table.split(".")]) for table in destination_tables
    }
    user_facing_asset_keys = table_to_asset_key_map or tracked_asset_keys
    metadata_by_table_name = check.opt_mapping_param(
        metadata_by_table_name, "metadata_by_table_name", key_type=str
    )

    @multi_asset(
        name=f"stitch_sync_{data_source_id}",
        outs={
            "_".join(key.path): AssetOut(
                io_manager_key=io_manager_key,
                key=user_facing_asset_keys[table],
                metadata=metadata_by_table_name.get(table, {}),
            )
            for table, key in tracked_asset_keys.items()
        },
        required_resource_keys={"stitch"},
        compute_kind="stitch",
        resource_defs=resource_defs,
        group_name=group_name,
    )
    def _assets(context):
        stitch_output = context.resources.stitch.start_replication_job_and_poll(
            data_source_id=data_source_id,
            poll_interval=poll_interval,
            extraction_timeout=extraction_timeout,
            load_timeout=load_timeout,
        )
        for materialization in generate_materializations(stitch_output, asset_key_prefix):
            if materialization.asset_key in tracked_asset_keys.values():
                yield Output(
                    value=None,
                    output_name="_".join(materialization.asset_key.path),
                    metadata={
                        entry.label: entry.entry_data for entry in materialization.metadata_entries
                    },
                )
            else:
                yield materialization

    return [_assets]
