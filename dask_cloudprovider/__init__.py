from . import config
from .providers.aws.ecs import ECSCluster, FargateCluster
from .providers.azureml.azureml import AzureMLCluster

__all__ = ["ECSCluster", "FargateCluster", "AzureMLCluster"]

from ._version import get_versions

__version__ = get_versions()["version"]

del get_versions
