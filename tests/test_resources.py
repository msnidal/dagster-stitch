import dagster_stitch

import pytest
import responses

from dagster import Failure, build_init_resource_context
from dagster_stitch.resources import stitch_resource

from constants import (
    ACCOUNT_ID,
    API_KEY,
    DATA_SOURCE_ID,
    JOB_ID,
    STREAM_NAME,
    get_extraction_response,
    get_list_loads_response,
    get_list_streams_response,
    get_stream_schema_response,
)


def test_start_replication_job():
    """Test that the start_replication_job method works as expected."""
    resource = stitch_resource(
        build_init_resource_context(config={"api_key": API_KEY, "account_id": ACCOUNT_ID})
    )

    with responses.RequestsMock() as response_mock:
        json_response = {"job_name": JOB_ID}

        response_mock.add(
            responses.POST,
            f"https://api.stitchdata.com/v4/sources/{DATA_SOURCE_ID}/sync",
            json=json_response,
        )

        assert resource.start_replication_job(DATA_SOURCE_ID) == json_response


@pytest.mark.parametrize("max_retries,actual_retries", [(2, 1), (2, 3), (4, 4)])
def test_get_replication_job_retries(max_retries: int, actual_retries: int):
    """Test behaviour of the get_replication_job method on request errors"""
    resource = stitch_resource(
        build_init_resource_context(
            config={
                "api_key": API_KEY,
                "account_id": ACCOUNT_ID,
                "request_max_retries": max_retries,
                "request_retry_delay": 0,
            }
        )
    )
    json_response = {"job_name": JOB_ID}

    def _mock_response():
        with responses.RequestsMock() as response_mock:
            for _ in range(actual_retries):
                response_mock.add(
                    responses.POST,
                    f"https://api.stitchdata.com/v4/sources/{DATA_SOURCE_ID}/sync",
                    status=500,
                )
            response_mock.add(
                responses.POST,
                f"https://api.stitchdata.com/v4/sources/{DATA_SOURCE_ID}/sync",
                json={"job_name": JOB_ID},
            )

            return resource.start_replication_job(DATA_SOURCE_ID)

    if actual_retries < max_retries:
        assert _mock_response() == json_response
    else:
        with pytest.raises(Failure):
            _mock_response()


def test_list_streams():
    """Test that the list_streams method works as expected.
    We want to verify this one in particular because the Stitch API returns a list, not a dict per usual
    """
    resource = stitch_resource(
        build_init_resource_context(config={"api_key": API_KEY, "account_id": ACCOUNT_ID})
    )

    with responses.RequestsMock() as response_mock:
        json_response = get_list_streams_response()

        response_mock.add(
            responses.GET,
            f"https://api.stitchdata.com/v4/sources/{DATA_SOURCE_ID}/streams",
            json=json_response,
        )

        streams = resource.list_streams(DATA_SOURCE_ID)
        assert STREAM_NAME in streams, "Stream name not found in list of streams"
        assert (
            streams[STREAM_NAME] == json_response[0]
        ), "Stream metadata not found in list of streams"


def test_get_stream_schema():
    """Test that the get_stream_schema method works as expected."""
    resource = stitch_resource(
        build_init_resource_context(config={"api_key": API_KEY, "account_id": ACCOUNT_ID})
    )

    with responses.RequestsMock() as response_mock:
        json_response = get_stream_schema_response()

        response_mock.add(
            responses.GET,
            f"https://api.stitchdata.com/v4/sources/{DATA_SOURCE_ID}/streams/{STREAM_NAME}",
            json=json_response,
        )

        stream_schema = resource.get_stream_schema(DATA_SOURCE_ID, STREAM_NAME)
        assert stream_schema == {"stream_id": STREAM_NAME, "schema": ["author", "description"]}


@pytest.mark.parametrize("failure_stage", ["start", "extract", None])
def test_start_replication_job_and_poll(failure_stage):
    """Test that the start_replication_job_and_poll method works as expected."""
    resource = stitch_resource(
        build_init_resource_context(config={"api_key": API_KEY, "account_id": ACCOUNT_ID})
    )

    with responses.RequestsMock() as response_mock:
        # Start replication job
        sync_response = {"job_name": JOB_ID}
        response_mock.add(
            responses.POST,
            f"https://api.stitchdata.com/v4/sources/{DATA_SOURCE_ID}/sync",
            json={"error": "Ouchie owie"} if failure_stage == "start" else sync_response,
        )

        if failure_stage != "start":
            # Get extractions
            extraction_response = get_extraction_response(failure_stage == "extract")
            response_mock.add(
                responses.GET,
                f"https://api.stitchdata.com/v4/sources/{ACCOUNT_ID}/extractions",
                json=extraction_response,
            )

        if failure_stage not in ["start", "extract"]:
            # List streams
            list_stream_response = get_list_streams_response()
            response_mock.add(
                responses.GET,
                f"https://api.stitchdata.com/v4/sources/{DATA_SOURCE_ID}/streams",
                json=list_stream_response,
            )

            # List recent account loads
            # TODO: failure load not found for data source
            list_loads_response = get_list_loads_response()
            response_mock.add(
                responses.GET,
                f"https://api.stitchdata.com/v4/sources/{ACCOUNT_ID}/loads",
                json=list_loads_response,
            )

            # Get stream schemas
            stream_schema_response = get_stream_schema_response()
            response_mock.add(
                responses.GET,
                f"https://api.stitchdata.com/v4/sources/{DATA_SOURCE_ID}/streams/{STREAM_NAME}",
                json=stream_schema_response,
            )

        if failure_stage:
            with pytest.raises(Failure):
                resource.start_replication_job_and_poll(DATA_SOURCE_ID)
        else:
            resource.start_replication_job_and_poll(DATA_SOURCE_ID)
