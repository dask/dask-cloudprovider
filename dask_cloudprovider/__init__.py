from . import config

from ._version import get_versions

__version__ = get_versions()["version"]

del get_versions


def __getattr__(name):
    if name in ["EC2Cluster", "ECSCluster", "FargateCluster"]:
        raise ImportError(
            "AWS cluster managers have been moved into the aws subpackage. "
            f"Please import dask_cloudprovider.aws.{name}"
        )

    if name in ["AzureMLCluster"]:
        raise ImportError(
            "Azure cluster managers have been moved into the azure subpackage. "
            f"Please import dask_cloudprovider.azure.{name}"
        )

    if name in ["DropletCluster"]:
        raise ImportError(
            "DigitalOcean cluster managers have been moved into the digitalocean subpackage. "
            f"Please import dask_cloudprovider.digitalocean.{name}"
        )
