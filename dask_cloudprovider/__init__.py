from . import config
from .providers.aws.ecs import ECSCluster, FargateCluster

__all__ = ["ECSCluster", "FargateCluster"]

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions
