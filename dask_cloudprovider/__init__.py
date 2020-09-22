from . import config

try:
    from .providers.aws.ecs import ECSCluster, FargateCluster
    from .providers.aws.ec2 import EC2Cluster
except ImportError:
    pass
try:
    from .providers.azure.azureml import AzureMLCluster
except ImportError:
    pass

__all__ = ["ECSCluster", "EC2Cluster", "FargateCluster", "AzureMLCluster"]

from ._version import get_versions

__version__ = get_versions()["version"]

del get_versions
