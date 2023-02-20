from typing import Any, Mapping, NamedTuple


class StitchOutput(
    NamedTuple(
        "_StitchOutput",
        [
            ("extraction_details", Mapping[str, Any]),
            ("load_details", Mapping[str, Any]),
            ("stream_schema", Mapping[str, Any]),
        ],
    )
):
    """Stitch Output type

    Consists of a set of dictionaries containing information about the state of a Stitch connector
    after a sync completes.

    Attributes:
        extraction_details (Dict[str, Any]):
            The raw Stitch API response containing the details of the extraction across all streams in the
            data source. General information, not stream-specific: For more on the schema of this dictionary, see:
            https://www.stitchdata.com/docs/developers/stitch-connect/api#extraction--object
        load_details (Dict[str, Any]):
            The raw Stitch API response containing the details of the ETL load to the target database. Consists of an
            object with a key for each stream in the data source, whose value contains the load details for that stream.
            For more details, see: https://www.stitchdata.com/docs/developers/stitch-connect/api#load--object
        stream_schema (Dict[str, Any]):
            The raw Stitch API response containing the schema of each stream in the data source. Consists of an object
            with a key for each stream in the data source, whose value is a list of values specified in the schema. For more,
            see: https://www.stitchdata.com/docs/developers/stitch-connect/api#retrieve-a-streams-schema
    """
