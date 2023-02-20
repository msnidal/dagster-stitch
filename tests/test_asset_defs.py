import pytest
import responses

from dagster import AssetKey, materialize_to_memory
from dagster_stitch.asset_defs import build_stitch_assets
from dagster_stitch import stitch_resource

from utils import DATA_SOURCE_ID, DATA_SOURCE_NAME, API_KEY, ACCOUNT_ID, STREAM_ID, STREAM_NAME, mock_sync_requests


def test_stitch_asset_keys():
    assets = build_stitch_assets(
        data_source_id=DATA_SOURCE_ID, destination_tables=["x.foo", "y.bar"]
    )
    assert assets[0].keys == {AssetKey(["x", "foo"]), AssetKey(["y", "bar"])}


def test_stitch_asset_run():
    resource = stitch_resource.configured({"api_key": API_KEY, "account_id": ACCOUNT_ID})
    tables = [f"{DATA_SOURCE_NAME}.{STREAM_NAME}"]

    assets = build_stitch_assets(data_source_id=DATA_SOURCE_ID, destination_tables=tables)

    with responses.RequestsMock() as response_mock:
        mock_sync_requests(response_mock)
        result = materialize_to_memory(
            assets,
            resources={"stitch": resource},
        )
        assert result.success

        # make sure we only have outputs for the explicit asset keys
        outputs = [
            event
            for event in result.events_for_node(f"stitch_sync_{DATA_SOURCE_ID}")
            if event.event_type_value == "STEP_OUTPUT"
        ]
        assert len(outputs) == len(tables)

        # make sure we have asset materializations for all the schemas/tables that were actually sync'd
        asset_materializations = [
            event
            for event in result.events_for_node(f"stitch_sync_{DATA_SOURCE_ID}")
            if event.event_type_value == "ASSET_MATERIALIZATION"
        ]
        assert len(asset_materializations) == len(tables)
        found_asset_keys = set(
            mat.event_specific_data.materialization.asset_key for mat in asset_materializations
        )
        assert found_asset_keys == {AssetKey([DATA_SOURCE_NAME, STREAM_NAME])}
