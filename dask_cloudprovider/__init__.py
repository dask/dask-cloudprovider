from . import config
from .providers.aws.ecs import ECSCluster, FargateCluster
from .providers.azure.azureml import AzureMLCluster, AzureMLSSHCluster

__all__ = ["ECSCluster", "FargateCluster", "AzureMLCluster", "AzureMLSSHCluster"]

from ._version import get_versions

__version__ = get_versions()["version"]

del get_versions
