import dask
from dask_cloudprovider.config import ClusterConfig
from dask_cloudprovider.generic.vmcluster import (
    VMCluster,
    VMInterface,
    SchedulerMixin,
    WorkerMixin,
)

class IBMCodeEngine(VMInterface):


class IBMCodeEngineScheduler(SchedulerMixin, IBMCodeEngine):


class IBMCodeEngineWorker(WorkerMixin, IBMCodeEngine):


class IBMCodeEngineVMCluster(VMCluster):
