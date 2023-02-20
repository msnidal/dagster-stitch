import datetime
import responses

API_KEY = "foo"
DATA_SOURCE_ID = "bing"
ACCOUNT_ID = "bar"
JOB_ID = "baz"
STREAM_NAME = "qux"
CONNECTOR_ID = "lingo"


def mock_sync_requests(response_mock):
    """Mocking requests for a nominal sync operation run-place

    TODO: Extend with various failure or retry cases

    Args:
        response_mock (responses.RequestsMock): The response mock to add the requests to
    """
    response_mock.add(
        responses.POST,
        f"https://api.stitchdata.com/v4/sources/{DATA_SOURCE_ID}/sync",
        json={"job_name": JOB_ID},
    )
    response_mock.add(
        responses.GET,
        f"https://api.stitchdata.com/v4/sources/{ACCOUNT_ID}/extractions",
        json=get_extraction_response(),
    )
    response_mock.add(
        responses.GET,
        f"https://api.stitchdata.com/v4/sources/{DATA_SOURCE_ID}/streams",
        json=get_list_streams_response(),
    )
    response_mock.add(
        responses.GET,
        f"https://api.stitchdata.com/v4/sources/{ACCOUNT_ID}/loads",
        json=get_list_loads_response(),
    )
    response_mock.add(
        responses.GET,
        f"https://api.stitchdata.com/v4/sources/{DATA_SOURCE_ID}/streams/{STREAM_NAME}",
        json=get_stream_schema_response(),
    )


def get_extraction_response(failure=False):
    return {
        "data": [
            {
                "target_exit_status": 0,
                "job_name": JOB_ID,
                "start_time": "2023-02-19T03:11:48Z",
                "stitch_client_id": 204845,
                "tap_exit_status": 0,
                "source_type": "tap-github",
                "target_description": None,
                "discovery_exit_status": 0,
                "discovery_description": None,
                "tap_description": None,
                "completion_time": "2023-02-19T03:11:56Z",
                "source_id": DATA_SOURCE_ID if not failure else "wrong",
            }
        ],
        "page": 1,
        "total": 1,
        "links": {},
    }


def get_list_streams_response():
    return [
        {
            "selected": True,
            "stream_id": 29749020,
            "tap_stream_id": STREAM_NAME,
            "stream_name": STREAM_NAME,
            "metadata": {
                "forced-replication-method": "FULL_TABLE",
                "selected": True,
                "inclusion": "available",
                "table-key-properties": ["id"],
            },
        },
    ]


def get_list_loads_response(loaded_at: datetime.datetime = None):
    if loaded_at is None:
        loaded_at = datetime.datetime.now() + datetime.timedelta(days=1)
    loaded_at = loaded_at.strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "data": [
            {
                "stitch_client_id": 204845,
                "source_name": DATA_SOURCE_ID,
                "stream_name": STREAM_NAME,
                "last_batch_loaded_at": loaded_at,
                "error_state": None,
            }
        ],
        "page": 1,
        "total": 10,
        "links": {},
    }


def get_stream_schema_response():
    return {
        "schema": (
            '{"type": "object", "properties": {"id": {"type": "integer"}, "name": {"type":'
            ' "string"}}}'
        ),
        "metadata": [
            {
                "breadcrumb": [],
                "metadata": {
                    "forced-replication-method": "INCREMENTAL",
                    "inclusion": "available",
                    "selected": True,
                    "table-key-properties": ["sha"],
                    "valid-replication-keys": "updated_at",
                },
            },
            {
                "breadcrumb": ["properties", "author"],
                "metadata": {"inclusion": "available", "selected": True},
            },
            {
                "breadcrumb": ["properties", "description"],
                "metadata": {"inclusion": "available", "selected": True},
            },
        ],
        "non-discoverable-metadata-keys": ["selected"],
    }
