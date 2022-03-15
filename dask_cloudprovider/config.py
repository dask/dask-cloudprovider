from __future__ import print_function, division, absolute_import

import os

import dask
import yaml


class ClusterConfig(dict):
    """Simple config interface for dask-cloudprovider clusters, such as `AzureVMCluster`.

    Enables '.' notation for nested access, as per `dask.config.get`.

    Example
    -------

    >>> from dask_cloudprovider.config import ClusterConfig
    >>> class RandomCluster(VMCluster):
    ...     def __init__(self, option=None):
    ...         self.config = ClusterConfig(dask.config.get("cloudprovider.random", {}))
    ...         self.option = self.config.get("option", override_with=option)

    """

    def __new__(cls, d):
        return super().__new__(cls, d)

    def get(self, key, default=None, override_with=None):
        return dask.config.get(
            key, default=default, config=self, override_with=override_with
        )


fn = os.path.join(os.path.dirname(__file__), "cloudprovider.yaml")
dask.config.ensure_file(source=fn)

with open(fn) as f:
    defaults = yaml.safe_load(f)

dask.config.update_defaults(defaults)
