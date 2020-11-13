import asyncio

import dask
from dask_cloudprovider.generic.vmcluster import (
    VMCluster,
    VMInterface,
    SchedulerMixin,
    WorkerMixin,
)
from dask_cloudprovider.exceptions import ConfigError

try:
    from azure.common.credentials import ServicePrincipalCredentials
    from azure.mgmt.resource import ResourceManagementClient

    # from azure.mgmt.network import NetworkManagementClient
    # from azure.mgmt.compute import ComputeManagementClient
    # from azure.mgmt.compute.models import DiskCreateOption

    # from msrestazure.azure_exceptions import CloudError
except ImportError as e:
    msg = (
        "Dask Cloud Provider Azure requirements are not installed.\n\n"
        "Please either conda or pip install as follows:\n\n"
        "  conda install dask-cloudprovider                             # either conda install\n"
        '  python -m pip install "dask-cloudprovider[azure]" --upgrade  # or python -m pip install'
    )
    raise ImportError(msg) from e


class AzureVM(VMInterface):
    def __init__(
        self,
        cluster: str,
        config,
        *args,
        location: str = None,
        vnet: str = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.vm = None
        self.cluster = cluster
        self.config = config
        self.location = location
        self.gpu_instance = False
        self.bootstrap = True

    async def create_vm(self):
        raise NotImplementedError()
        self.nic = None  # TODO Create NIC (in Azure a NIC is not created for you and must be done manually first)
        self.vm = None  # TODO Create VM
        ip = None  # TODO Get IP
        self.cluster._log(f"Created VM {self.name}")
        return ip

    async def destroy_vm(self):
        self.vm.destroy()  # TODO Destroy VM
        self.cluster._log(f"Terminated droplet {self.name}")


class AzureVMScheduler(SchedulerMixin, AzureVM):
    """Scheduler running on an Azure VM."""


class AzureVMWorker(WorkerMixin, AzureVM):
    """Worker running on an AzureVM."""


class AzureVMCluster(VMCluster):
    """Cluster running on Azure Virtual machines.

    TODO Document config

    Parameters
    ----------
    TODO Update parameters
    location: str
        The DO location to launch you cluster in. A full list can be obtained with ``doctl compute location list``.
    size: str
        The VM size slug. You can get a full list with ``doctl compute size list``.
        The default is ``s-1vcpu-1gb`` which is 1GB RAM and 1 vCPU
    image: str
        The image ID to use for the host OS. This should be a Ubuntu variant.
        You can list available images with ``doctl compute image list --public | grep ubuntu.*x64``.
    worker_module: str
        The Dask worker module to start on worker VMs.
    n_workers: int
        Number of workers to initialise the cluster with. Defaults to ``0``.
    worker_module: str
        The Python module to run for the worker. Defaults to ``distributed.cli.dask_worker``
    worker_options: dict
        Params to be passed to the worker class.
        See :class:`distributed.worker.Worker` for default worker class.
        If you set ``worker_module`` then refer to the docstring for the custom worker class.
    scheduler_options: dict
        Params to be passed to the scheduler class.
        See :class:`distributed.scheduler.Scheduler`.
    docker_image: string (optional)
        The Docker image to run on all instances.

        This image must have a valid Python environment and have ``dask`` installed in order for the
        ``dask-scheduler`` and ``dask-worker`` commands to be available. It is recommended the Python
        environment matches your local environment where ``EC2Cluster`` is being created from.

        For GPU instance types the Docker image much have NVIDIA drivers and ``dask-cuda`` installed.

        By default the ``daskdev/dask:latest`` image will be used.
    silence_logs: bool
        Whether or not we should silence logging when setting up the cluster.
    asynchronous: bool
        If this is intended to be used directly within an event loop with
        async/await
    security : Security or bool, optional
        Configures communication security in this cluster. Can be a security
        object, or True. If True, temporary self-signed credentials will
        be created automatically.

    Examples
    --------

    TODO Examples

    """

    def __init__(
        self,
        location: str = None,
        resource_group: str = None,
        vnet: str = None,
        **kwargs,
    ):
        self.config = dask.config.get("cloudprovider.azure", {})
        self.scheduler_class = AzureVMScheduler
        self.worker_class = AzureVMWorker
        self.resource_group = (
            resource_group
            if resource_group is not None
            else self.config.get("azurevm.resource_group")
        )
        if self.resource_group is None:
            raise ConfigError("You must configure a resource_group")
        self.vnet = vnet if vnet is not None else self.config.get("azurevm.vnet")
        if self.vnet is None:
            raise ConfigError("You must configure a vnet")

        self.options = {
            "cluster": self,
            "config": self.config,
            "location": location
            if location is not None
            else self.config.get("location"),
        }
        self.scheduler_options = {**self.options}
        self.worker_options = {**self.options}
        super().__init__(**kwargs)
