"""
Conditional imports used to make ``dask-cloudprovider``
compatible with a wide range of versions of its dependencies.
"""

# distributed.utils.serialize_for_cli was moved to dask
# as of https://github.com/dask/distributed/pull/4966
try:
    from dask.config import serialize as dask_serialize
except ImportError:
    from distributed.utils import serialize_for_cli as dask_serialize
