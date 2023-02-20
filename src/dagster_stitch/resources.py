import time
import logging
from typing import Optional
from urllib.parse import urljoin
from datetime import datetime


import requests
from dagster import get_dagster_logger, resource, Failure, Field, StringSource, __version__

from dagster_stitch.utils import BearerAuth
from dagster_stitch.types import StitchOutput


STITCH_API_BASE = "https://api.stitchdata.com/"
STITCH_API_VERSION = "v4/"

DEFAULT_POLL_INTERVAL = 10.0

API_BASE_URL = urljoin(STITCH_API_BASE, STITCH_API_VERSION)

STITCH_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class StitchResource:
    """Exposes the Stitch REST API as a Dagster resource.

    Consists of various methods for interacting with the Stitch API, including methods for starting
    a sync, polling for the status of a sync, and retrieving the details of a sync.

    Args:
        api_key (str): The API key for the Stitch account that this resource will interact with.
        account_id (str): The account ID for the Stitch account that this resource will interact with.
        request_max_retries (int): The maximum number of times to retry a failed request.
        default_poll_interval (float): The default number of seconds to wait between polling the
            Stitch API for a request status.
        default_extraction_timeout (Optional[float]): The default maximum time that will waited
            before a sync is timed out during the extraction phase. By default, this will never time
            out.
        default_load_timeout (Optional[float]): The default maximum time that will waited before a
            sync is timed out during the load phase. By default, this will never time out.
        request_retry_delay (float): The number of seconds to wait between retries of a failed
            request.
        log (logging.Logger): The logger to use for logging messages.
    """

    def __init__(
        self,
        api_key: str,
        account_id: int,
        request_max_retries: int = 3,
        default_poll_interval: float = DEFAULT_POLL_INTERVAL,
        default_extraction_timeout: Optional[float] = None,
        default_load_timeout: Optional[float] = None,
        request_retry_delay: float = 0.25,
        log: logging.Logger = get_dagster_logger(),
    ):
        self._auth = BearerAuth(api_key)
        self._account_id = account_id

        self._request_max_retries = request_max_retries
        self._default_poll_interval = default_poll_interval
        self._default_extraction_timeout = default_extraction_timeout
        self._default_load_timeout = default_load_timeout
        self._request_retry_delay = request_retry_delay
        self._log = log

    def make_request(self, method: str, endpoint: str, data: Optional[str] = None) -> dict:
        """Make a request to the Stitch API.

        Args:
            method (str): The HTTP method to use for the request.
            endpoint (str): The endpoint to make the request to.
            data (Optional[str]): The data to send with the request.

        Returns:
            Dict[str, Any]: The JSON-parsed response from the Stitch API.

        Raises:
            Failure: If the request fails after the maximum number of retries.
        """
        url = urljoin(API_BASE_URL, endpoint)
        headers = {
            "User-Agent": f"Dagster/{__version__}",
            "Content-Type": "application/json",
        }

        retries = 0
        while retries < self._request_max_retries:
            try:
                response = requests.request(
                    method, url, headers=headers, auth=self._auth, data=data
                )
                response.raise_for_status()
                response_json = response.json()

                if type(response_json) is not dict:
                    return response_json
                elif "next" in response_json.get("links", {}):
                    self._log.warning("Pagination not yet implemented")

                return response_json["data"] if "data" in response_json else response_json
            except requests.exceptions.RequestException as e:
                self._log.error(f"Request to Stitch API at <{url}> failed: {str(e)}")
                retries += 1
                time.sleep(self._request_retry_delay)

        raise Failure(
            f"Request to Stitch API at <{url}> failed after reaching max retries"
            f" {self._request_max_retries}"
        )

    def get_data_source(self, data_source_id: int) -> dict:
        """Get data source metadata

        Retrieves data-source-specific metadata applicable to all streams in the data source.
        See https://www.stitchdata.com/docs/developers/stitch-connect/api#retrieve-a-source

        Args:
            data_source_id (int): The ID of the data source to retrieve metadata for.

        Returns:
            Dict[str, Any]: The data source metadata object.
        """
        return self.make_request("GET", f"sources/{data_source_id}")

    def list_all_sources(self) -> dict:
        """List all data sources

        Lists all data sources associated with the authenticated account.
        See https://www.stitchdata.com/docs/developers/stitch-connect/api#list-sources

        Returns:
            Dict[str, Dict[int, Any]]: A dictionary mapping data source IDs to data source metadata
        """
        sources = self.make_request("GET", "sources")
        return {source["id"]: source for source in sources}

    def list_streams(self, data_source_id: int) -> dict:
        """List all streams for a given data source

        Streams are the individual tables or collections of data that are extracted from a data source.
        See https://www.stitchdata.com/docs/developers/stitch-connect/api#list-streams

        Args:
            data_source_id (int): The ID of the data source to list streams for.

        Returns:
            Dict[str, Dict[int, Any]]: A dictionary mapping stream IDs to stream metadata objects.
        """
        streams = self.make_request("GET", f"sources/{data_source_id}/streams")
        return {stream["stream_id"]: stream for stream in streams}

    def get_stream_schema(self, data_source_id: int, stream_id: int) -> dict:
        """Get the schema for a given stream

        The schema for a stream is a list of the properties that are extracted from the stream.
        See https://www.stitchdata.com/docs/developers/stitch-connect/api#retrieve-a-streams-schema

        Args:
            data_source_id (int): The ID of the data source that the stream belongs to.
            stream_id (int): The ID of the stream to get the schema for.

        Returns:
            Dict[str, Any]: The stream schema object.
        """
        schema = self.make_request("GET", f"sources/{data_source_id}/streams/{stream_id}")
        schema_string = schema[
            "schema"
        ]  # TODO some string parsing (jsonparse?) to get the types, just validate with below
        return {
            "schema": [
                property["breadcrumb"][1]
                for property in schema["metadata"]
                if "properties" in property["breadcrumb"] and property["metadata"]["selected"]
            ],
        }

    def list_recent_loads(self, data_source_name: Optional[str] = None) -> dict:
        """Get the recent loads, optionally filtered by data source

        Stitch will return all of the recent loads for any given stream in the account.
        The output is nested by data source and stream name for convenience.
        See https://www.stitchdata.com/docs/developers/stitch-connect/api#list-last-loads-for-account

        Args:
            data_source_id (Optional[int]): The ID of the data source to filter loads by.

        Returns:
            Dict[str, Dict[str, Dict[str, Any]]]: A nested dictionary mapping data source names to
                stream names to load metadata objects.
        """
        loads = self.make_request("GET", f"{self._account_id}/loads")

        # Nested dict of loads by data source then stream name
        all_loads = {}
        for load in loads:
            if load["source_name"] not in all_loads:
                all_loads[load["source_name"]] = {}

            all_loads[load["source_name"]][load["stream_name"]] = load

        if data_source_name:
            return all_loads.get(data_source_name, {})
        else:
            return all_loads

    def start_replication_job(self, data_source_id: int):
        """Starts a replication job for the given data source, consisting of extract and load

        While Stitch considers both stages (extract & load) separately, they are always run together.
        The output of this command is the job_id for the extract job - association with load job is by source and time
        See https://www.stitchdata.com/docs/developers/stitch-connect/api#start-a-job

        Args:
            data_source_id (int): The ID of the data source to start a replication job for.

        Returns:
            Dict[str, Any]: The response from the Stitch API, containing the job_id for the extract job.
        """
        response = self.make_request("POST", f"sources/{data_source_id}/sync")
        if "error" in response:
            raise Failure(response["error"])

        return response

    def get_extractions(self, data_source_id: Optional[int] = None) -> dict:
        """Lists all extractions for the account in array, optionally filtered by data source

        Maps from Stitch's list of extractions to a dict of extractions keyed by the (unique) data source_id
        See https://www.stitchdata.com/docs/developers/stitch-connect/api#list-last-extractions

        Args:
            data_source_id (Optional[int]): The ID of the data source to filter extractions by.

        Returns:
            Dict[str, Dict[str, Any]]: A dictionary mapping data source IDs to the extractions for that data source.
        """
        extractions = self.make_request("GET", f"{self._account_id}/extractions")
        extraction_map = {extractions["source_id"]: extractions for extractions in extractions}

        self._log.info(f"Extractions: {extraction_map}")
        if data_source_id is not None:
            return extraction_map.get(data_source_id, {})
        else:
            return extraction_map

    def start_replication_job_and_poll(
        self,
        data_source_id: int,
        poll_interval: Optional[float] = None,
        extraction_timeout: Optional[float] = None,
        load_timeout: Optional[float] = None,
    ) -> StitchOutput:
        """Start and poll for completion of a complete replication job consisting of extract and load

        Replicates all selected streams in the given data source, and polls for completion of the extract and load stages.
        Since Stitch decouples the extract and load stages, after extraction is complete we simply wait for the loads to
        either update, raise an error, or time out. This is a blocking call.

        Args:
            data_source_id (int): The ID of the data source to start a replication job for.
            poll_interval (Optional[float]): The interval in seconds to poll for job completion.
            extraction_timeout (Optional[float]): The timeout in seconds for the extract stage.
            load_timeout (Optional[float]): The timeout in seconds for the load stage.

        Returns:
            StitchOutput: The output of the replication job, containing the load, extract and stream information. See types.py for details.
        """
        if poll_interval is None:
            poll_interval = self._default_poll_interval
        if extraction_timeout is None:
            extraction_timeout = self._default_extraction_timeout

        # Extraction
        extraction_start = datetime.now()
        source_metadata = self.get_data_source(data_source_id)
        if not source_metadata:
            raise Failure(f"Data source {data_source_id} not found")

        replication_response = self.start_replication_job(data_source_id)

        while True:
            extraction = self.get_extractions(data_source_id)
            if not extraction:  # TODO: Test first-run behaviour
                raise Failure(f"Extraction not found for data source {data_source_id}")

            self._log.info(
                f"Polled extractions for source {data_source_id}: Found job"
                f" {extraction['job_name']} started at {extraction['start_time']}"
            )

            if (
                extraction["job_name"] == replication_response["job_name"]
                or datetime.strptime(extraction["start_time"], STITCH_DATETIME_FORMAT)
                >= extraction_start  # In case another job completes during polling (unlikely)
            ):
                # TODO: Fix timezone handling (currently assumes UTC)
                self._log.info(
                    f"Extraction job for source {data_source_id} completed:"
                    f" {extraction['job_name']}\nCompare {extraction['start_time']} >="
                    f" {extraction_start}"
                )
                break

            if extraction_timeout and datetime.now() - extraction_start > extraction_timeout:
                raise Failure(
                    f"Extraction job for source {data_source_id} timed out after"
                    f" {extraction_timeout} seconds"
                )

            time.sleep(poll_interval)

        for failure_mode in ("discovery", "tap", "target"):
            if not extraction[f"{failure_mode}_exit_status"]:
                continue  # Still in progress
            if extraction[f"{failure_mode}_exit_status"] != 0:
                raise Failure(
                    f"{failure_mode.title()} failed with exit status"
                    f" {extraction[f'{failure_mode}_exit_status']}:"
                    f" {extraction[f'{failure_mode}_description']}"
                )

        # Load
        streams = self.list_streams(data_source_id)
        load_start = datetime.now()

        loading_complete = False
        while not loading_complete:
            loads = self.list_recent_loads(source_metadata["name"])
            if not loads:
                raise Failure(f"Load not found for data source {source_metadata['name']}")

            self._log.info(f"Polled loads for source {data_source_id}")

            loading_complete = True
            for stream in streams:
                if streams[stream]["metadata"]["selected"]:
                    # TODO: Handle first-time loads waiting for them to show up
                    if streams[stream]["stream_name"] not in loads:
                        # Valid in the case that there is no data
                        self._log.warning(
                            f"Load for stream {streams[stream]['stream_name']} not found"
                        )
                    elif loads[streams[stream]["stream_name"]]["error_state"]:
                        self._log.warning(
                            f"Load for stream {streams[stream]['stream_name']} failed:"
                            f" {loads[streams[stream]['stream_name']]['error_state']['notification_data']['message']}"
                        )
                    elif (
                        loads[streams[stream]["stream_name"]]["last_batch_loaded_at"] is None
                        or datetime.strptime(
                            loads[streams[stream]["stream_name"]]["last_batch_loaded_at"],
                            STITCH_DATETIME_FORMAT,
                        )
                        < load_start
                    ):
                        # TODO: Parse ExtractionLogs to determine whether new records are inserted to a load so we know whether to keep waiting
                        self._log.info(
                            f"Load for stream {streams[stream]['stream_name']} not yet complete:"
                            f" {loads[streams[stream]['stream_name']]['last_batch_loaded_at']} <"
                            f" {load_start}"
                        )
                        loading_complete = False

            if not loading_complete:
                if load_timeout and datetime.now() - load_start > load_timeout:
                    raise Failure(
                        f"Load for source {data_source_id} timed out after {load_timeout} seconds"
                    )
                time.sleep(poll_interval)

        stream_schemas = {
            stream_id: self.get_stream_schema(data_source_id, stream_id)
            | {"name": streams[stream_id]["stream_name"]}
            for stream_id in streams.keys()
        }

        return StitchOutput(source_metadata, extraction, loads, stream_schemas)


@resource(
    config_schema={
        "api_key": Field(
            StringSource,
            is_required=True,
            description=(
                "Stitch API access key. You can generate one in your Stitch account settings page"
                " under 'API Access Keys'."
            ),
        ),
        "account_id": Field(
            int,
            is_required=True,
            description=(
                "Stitch account ID. You can find this in your Stitch account settings page under"
                " 'Account ID'."
            ),
        ),
        "request_max_retries": Field(
            int,
            default_value=3,
            description="The maximum number of times to retry a failed request to the Stitch API.",
        ),
        "default_poll_interval": Field(
            float,
            default_value=10,
            description=(
                "The number of seconds to wait between polling the Stitch API for a request status."
            ),
        ),
        "default_extraction_timeout": Field(
            float,
            default_value=600,
            description=(
                "The number of seconds to wait for a replication job to complete before timing out."
            ),
        ),
        "default_load_timeout": Field(
            float,
            default_value=600,
            description=(
                "The number of seconds to wait for a replication job to complete before timing out."
            ),
        ),
        "request_retry_delay": Field(
            float,
            default_value=0.25,
            description=(
                "The number of seconds to wait before retrying a failed request to the Stitch API."
            ),
        ),
    },
    description="This resource manages Stitch data sources and replication jobs.",
)
def stitch_resource(context) -> StitchResource:
    """Dagster resource for interacting with the Stitch API.

    Args:
        context (ResourceDefinition.Context): The Dagster resource context.

    Returns:
        StitchResource: The Dagster-managed Stitch resource API wrapper.
    """
    return StitchResource(
        api_key=context.resource_config["api_key"],
        account_id=context.resource_config["account_id"],
        request_max_retries=context.resource_config["request_max_retries"],
        default_poll_interval=context.resource_config["default_poll_interval"],
        default_extraction_timeout=context.resource_config["default_extraction_timeout"],
        default_load_timeout=context.resource_config["default_load_timeout"],
        request_retry_delay=context.resource_config["request_retry_delay"],
        log=context.log,
    )
