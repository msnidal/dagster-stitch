import datetime
import responses

API_KEY = "foo"
DATA_SOURCE_ID = 12345
ACCOUNT_ID = 67890
STREAM_ID = 54321
DESTINATION_ID = 98765
DATA_SOURCE_NAME = "boo"
STREAM_NAME = "bar"
JOB_ID = "baz"
CONNECTOR_ID = "lingo"


def mock_sync_requests(response_mock):
    """Mocking requests for a nominal sync operation run-place

    TODO: Extend with various failure or retry cases

    Args:
        response_mock (responses.RequestsMock): The response mock to add the requests to
    """
    response_mock.add(
        responses.GET,
        f"https://api.stitchdata.com/v4/sources/{DATA_SOURCE_ID}",
        json={"name": DATA_SOURCE_NAME},
    )
    response_mock.add(
        responses.POST,
        f"https://api.stitchdata.com/v4/sources/{DATA_SOURCE_ID}/sync",
        json={"job_name": JOB_ID},
    )
    response_mock.add(
        responses.GET,
        f"https://api.stitchdata.com/v4/{ACCOUNT_ID}/extractions",
        json=get_extraction_response(),
    )
    response_mock.add(
        responses.GET,
        f"https://api.stitchdata.com/v4/sources/{DATA_SOURCE_ID}/streams",
        json=get_list_streams_response(),
    )
    response_mock.add(
        responses.GET,
        f"https://api.stitchdata.com/v4/{ACCOUNT_ID}/loads",
        json=get_list_loads_response(),
    )
    response_mock.add(
        responses.GET,
        f"https://api.stitchdata.com/v4/sources/{DATA_SOURCE_ID}/streams/{STREAM_ID}",
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


def get_sources_response():
    return [
        {
            "description": None,
            "properties": {
                "anchor_time": "2023-02-19T02:11:48.084Z",
                "base_url": None,
                "cron_expression": None,
                "frequency_in_minutes": "60",
                "image_version": "2.latest",
                "product": "pipeline",
                "repository": "bingo/bongo",
                "start_date": "2022-02-19T00:00:00Z"
            },
            "updated_at": "2023-02-19T19:52:25Z",
            "schedule": {
                "type": "interval",
                "unit": "minute",
                "interval": 60.0,
                "next_fire_time": "2023-02-19T22:11:48Z"
            },
            "name": DATA_SOURCE_NAME,
            "mapped_destination_ids": [
                DESTINATION_ID
            ],
            "type": "platform.github",
            "deleted_at": None,
            "system_paused_at": None,
            "stitch_client_id": ACCOUNT_ID,
            "paused_at": None,
            "id": DATA_SOURCE_ID,
            "display_name": "Testy test test",
            "created_at": "2023-02-19T01:38:02Z",
            "report_card": {
                "type": "platform.github",
                "current_step": 5,
                "current_step_type": "fully_configured",
                "steps": [
                    {
                        "type": "form",
                        "properties": [
                            {
                                "is_required": False,
                                "name": "anchor_time",
                                "tap_mutable": False,
                                "property_type": "user_provided",
                                "system_provided": False,
                                "clonable": True,
                                "json_schema": {
                                    "type": "string",
                                    "format": "date-time"
                                },
                                "is_credential": False,
                                "provided": True
                            },
                         ]
                    },
                    {
                        "type": "oauth",
                        "properties": [
                            {
                                "is_required": True,
                                "name": "access_token",
                                "tap_mutable": False,
                                "property_type": "user_provided",
                                "system_provided": False,
                                "clonable": True,
                                "json_schema": {
                                    "type": "string"
                                },
                                "is_credential": True,
                                "provided": True
                            }
                        ]
                    },
                    {
                        "type": "discover_schema",
                        "properties": []
                    },
                    {
                        "type": "field_selection",
                        "properties": []
                    },
                    {
                        "type": "fully_configured",
                        "properties": []
                    }
                ]
            }
        }
    ]


def get_list_streams_response():
    return [
        {
            "selected": True,
            "stream_id": STREAM_ID,
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
                "source_name": DATA_SOURCE_NAME,
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
