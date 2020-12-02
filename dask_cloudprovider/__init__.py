from . import config

from ._version import get_versions

__version__ = get_versions()["version"]

del get_versions


def __getattr__(name):
    """As of dask_cloudprovider v0.5.0 all cluster managers are in cloud provider specific submodules.

    This allows us to more easily separate out optional dependencies. However we maintain some helpful
    errors at the top level.

    This is both to help migrate users of any cluster managers that existed before this was changed
    and also to help anyone who incorrectly tries to import a cluster manager from the top level.
    Perhaps because they saw it used in some documentation but didn't see the import.

    """

    if name in ["EC2Cluster", "ECSCluster", "FargateCluster"]:
        raise ImportError(
            "AWS cluster managers must be imported from the aws subpackage. "
            f"Please import dask_cloudprovider.aws.{name}"
        )

    if name in ["AzureVMCluster"]:
        raise ImportError(
            "Azure cluster managers must be imported from the the azure subpackage. "
            f"Please import dask_cloudprovider.azure.{name}"
        )

    if name in ["GCPCluster"]:
        raise ImportError(
            "Google Cloud cluster managers must be imported from the the gcp subpackage. "
            f"Please import dask_cloudprovider.gcp.{name}"
        )

    if name in ["DropletCluster"]:
        raise ImportError(
            "DigitalOcean cluster managers must be imported from the digitalocean subpackage. "
            f"Please import dask_cloudprovider.digitalocean.{name}"
        )
