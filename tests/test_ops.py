import pytest
import responses

from dagster import op, job, AssetKey
from dagster_stitch import stitch_resource, replicate_data_source_op, StitchOutput

from constants import (
    mock_sync_requests,
    ACCOUNT_ID,
    API_KEY,
    DATA_SOURCE_ID,
    STREAM_NAME,
)


def test_replicate_data_source_op():
    resource = stitch_resource.configured({"api_key": API_KEY, "account_id": ACCOUNT_ID})

    @job(
        resource_defs={"stitch": resource},
        config={
            "ops": {"replicate_data_source_op": {"config": {"data_source_id": DATA_SOURCE_ID}}}
        },
    )
    def replicate_data_source_job():
        replicate_data_source_op()

    with responses.RequestsMock() as response_mock:
        mock_sync_requests(response_mock)

        result = replicate_data_source_job.execute_in_process()
        assert result.success

        assert type(result.output_for_node("replicate_data_source_op")) == StitchOutput
        asset_materializations = [
            event
            for event in result.events_for_node("replicate_data_source_op")
            if event.event_type_value == "ASSET_MATERIALIZATION"
        ]
        assert len(asset_materializations) == 1
        asset_keys = set(
            materialization.event_specific_data.materialization.asset_key
            for materialization in asset_materializations
        )
        assert asset_keys == set(
            [
                AssetKey([f"stitch_{DATA_SOURCE_ID}", STREAM_NAME]),
            ]
        )
