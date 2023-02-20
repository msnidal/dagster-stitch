from dagster._core.utils import check_dagster_package_version

from .asset_defs import build_stitch_assets as build_stitch_assets
from .ops import replicate_data_source_op
from .resources import (
    StitchResource as StitchResource,
    stitch_resource as stitch_resource,
)
from .types import StitchOutput
from .version import __version__
