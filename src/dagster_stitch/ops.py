from dagster import op, Nothing, In, Out, Field, Nothing, Noneable, Output

from dagster_stitch.types import StitchOutput
from dagster_stitch.utils import generate_materializations


@op(
    required_resource_keys={"stitch"},
    ins={"start_after": In(Nothing)},
    out=Out(StitchOutput, description="The output of the Stitch sync."),
    config_schema={
        "data_source_id": Field(
            str,
            is_required=True,
            description=(
                "The Stitch data source ID that this op will sync. You can retrieve this value from"
                " the 'Setup' tab of a given data source in the Stitch UI."
            ),
        ),
        "poll_interval": Field(
            float,
            default_value=10,
            description=(
                "The number of seconds to wait between polling the Stitch API for a request status."
            ),
        ),
        "extraction_timeout": Field(
            Noneable(float),
            default_value=None,
            description=(
                "The maximum time that will waited before the extraction stage times out. By"
                " default, this will never time out."
            ),
        ),
        "load_timeout": Field(
            Noneable(float),
            default_value=None,
            description=(
                "The maximum time that will waited before the load stage times out. By default,"
                " this will never time out."
            ),
        ),
        "yield_materializations": Field(
            config=bool,
            default_value=True,
            description=(
                "Whether or not to yield materializations for the tables created by the sync."
            ),
        ),
        "asset_key_prefix": Field(
            Noneable(str),
            default_value="stitch",
            description=(
                "The prefix to use for the asset keys of the materializations yielded by this op."
            ),
        ),
    },
    tags={"kind": "stitch"},
)
def replicate_data_source_op(context):
    # Start the sync
    sync_request = context.resources.stitch.start_replication_job_and_poll(
        context.op_config["data_source_id"],
        context.op_config["poll_interval"],
        context.op_config["extraction_timeout"],
        context.op_config["load_timeout"],
    )

    if context.op_config["yield_materializations"]:
        asset_key_prefix = context.op_config["asset_key_prefix"]
        yield from generate_materializations(sync_request, asset_key_prefix)

    # Yield the sync status
    yield Output(sync_request)
