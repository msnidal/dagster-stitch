# dagster-stitch

[![PyPI](https://img.shields.io/pypi/v/dagster-stitch?color=gr)](https://pypi.org/project/dagster-stitch/#description)
![PyPI - Python Version](https://img.shields.io/badge/dynamic/json?query=info.requires_python&label=python&url=https%3A%2F%2Fpypi.org%2Fpypi%2Fdagster-stitch%2Fjson)
![PyPI - License](https://img.shields.io/pypi/l/dagster-stitch)
![Code Style - Black](https://img.shields.io/badge/code%20style-black-black)

This library provides a Dagster integration for Stitch, a managed batch data ingestion service similar to Fivetran and Airbyte.

## Disclaimer

Please note this library is under active development and should be used cautiously! Currently it supports triggering replication jobs and materializing the resulting tables as Dagster assets that can be used in downstream jobs. Reconciliation and other features are not yet included but would be great additions.

## Installation

To install the library, run:

```bash
$ pip install dagster-stitch
```

For development, it can be installed locally and tested with:

```bash
$ pip install -e .[lint,test]
$ pytest
```

## Configuration

### Setup

To use the library, you must configure a `stitch` resource in your Dagster instance. The resource requires a `client_id` and `client_secret` to authenticate with Stitch. You can find these values in the Stitch UI under `Settings > API Keys`.

You will also need to note your Stitch account ID and the ID of the data source you want to replicate to be used in the asset or operation configuration. These can be found by navigating to the data source in the Stitch UI and looking at the URL. For example, if the URL is `https://app.stitchdata.com/client/12345/pipeline/v2/sources/67890/summary`, then the account ID is `12345` and the data source ID is `67890`.

### Usage

Here's an example of how to instantiate an asset that will materialize the `table_name` table from the `data_source_id` data source:

```python
from dagster import Definitions
from dagster_stitch import stitch_resource, build_stitch_assets

stitch_instance = stitch_resource.configured(
    {
        "api_key": "your_stitch_api_key",
        "account_id": 12345,
    }
)

stitch_assets = build_stitch_assets(
    data_source_id=54321,
    destination_tables=["table_name"],
    resource_defs={"stitch": stitch_instance},
)
```
