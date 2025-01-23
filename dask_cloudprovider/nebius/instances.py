import dask

from dask_cloudprovider.generic.vmcluster import (
    VMCluster,
    VMInterface,
    SchedulerMixin,
    WorkerMixin,
)

try:
    from nebius.api.nebius.common.v1 import ResourceMetadata
    from nebius.api.nebius.vpc.v1 import SubnetServiceClient, ListSubnetsRequest
    from nebius.sdk import SDK
    from nebius.api.nebius.compute.v1 import (
        InstanceServiceClient,
        CreateInstanceRequest,
        DiskServiceClient,
        CreateDiskRequest,
        DiskSpec,
        SourceImageFamily,
        InstanceSpec,
        AttachedDiskSpec,
        ExistingDisk,
        ResourcesSpec,
        NetworkInterfaceSpec,
        IPAddress,
        PublicIPAddress,
        GetInstanceRequest,
        DeleteInstanceRequest,
        DeleteDiskRequest,
    )
except ImportError as e:
    msg = (
        "Dask Cloud Provider Nebius requirements are not installed.\n\n"
        "Please pip install as follows:\n\n"
        '  pip install "dask-cloudprovider[nebius]" --upgrade  # or python -m pip install'
    )
    raise ImportError(msg) from e


class NebiusInstance(VMInterface):
    def __init__(
        self,
        cluster: str,
        config,
        env_vars: dict = None,
        bootstrap=None,
        extra_bootstrap=None,
        docker_image: str = None,
        image_family: str = None,
        project_id: str = None,
        server_platform: str = None,
        server_preset: str = None,
        disk_size: int = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.cluster = cluster
        self.config = config
        self.extra_bootstrap = extra_bootstrap
        self.env_vars = env_vars
        self.bootstrap = bootstrap
        self.image_family = image_family
        self.project_id = project_id
        self.docker_image = docker_image
        self.server_platform = server_platform
        self.server_preset = server_preset
        self.sdk = SDK(credentials=self.config.get("token"))
        self.disk_size = disk_size
        self.instance_id = None
        self.disk_id = None

    async def create_vm(self, user_data=None):
        service = DiskServiceClient(self.sdk)
        operation = await service.create(
            CreateDiskRequest(
                metadata=ResourceMetadata(
                    parent_id=self.project_id,
                    name=self.name + "-disk",
                ),
                spec=DiskSpec(
                    source_image_family=SourceImageFamily(
                        image_family=self.image_family
                    ),
                    size_gibibytes=self.disk_size,
                    type=DiskSpec.DiskType.NETWORK_SSD,
                ),
            )
        )
        await operation.wait()
        self.disk_id = operation.resource_id

        service = SubnetServiceClient(self.sdk)
        sub_net = await service.list(ListSubnetsRequest(parent_id=self.project_id))
        subnet_id = sub_net.items[0].metadata.id

        service = InstanceServiceClient(self.sdk)
        operation = await service.create(
            CreateInstanceRequest(
                metadata=ResourceMetadata(
                    parent_id=self.project_id,
                    name=self.name,
                ),
                spec=InstanceSpec(
                    boot_disk=AttachedDiskSpec(
                        attach_mode=AttachedDiskSpec.AttachMode(2),
                        existing_disk=ExistingDisk(id=self.disk_id),
                    ),
                    cloud_init_user_data=self.cluster.render_process_cloud_init(self),
                    resources=ResourcesSpec(
                        platform=self.server_platform, preset=self.server_preset
                    ),
                    network_interfaces=[
                        NetworkInterfaceSpec(
                            subnet_id=subnet_id,
                            ip_address=IPAddress(),
                            name="network-interface-0",
                            public_ip_address=PublicIPAddress(),
                        )
                    ],
                ),
            )
        )
        self.instance_id = operation.resource_id

        self.cluster._log(f"Creating Nebius instance {self.name}")
        await operation.wait()
        service = InstanceServiceClient(self.sdk)
        operation = await service.get(
            GetInstanceRequest(
                id=self.instance_id,
            )
        )
        internal_ip = operation.status.network_interfaces[0].ip_address.address.split(
            "/"
        )[0]
        external_ip = operation.status.network_interfaces[
            0
        ].public_ip_address.address.split("/")[0]
        self.cluster._log(
            f"Created Nebius instance {self.name} with internal IP {internal_ip} and external IP {external_ip}"
        )
        return internal_ip, external_ip

    async def destroy_vm(self):
        if self.instance_id:
            service = InstanceServiceClient(self.sdk)
            operation = await service.delete(
                DeleteInstanceRequest(
                    id=self.instance_id,
                )
            )
        await operation.wait()

        if self.disk_id:
            service = DiskServiceClient(self.sdk)
            await service.delete(
                DeleteDiskRequest(
                    id=self.disk_id,
                )
            )
        self.cluster._log(
            f"Terminated instance {self.name} ({self.instance_id}) and deleted disk {self.disk_id}"
        )
        self.instance_id = None
        self.disk_id = None


class NebiusScheduler(SchedulerMixin, NebiusInstance):
    """Scheduler running on a Nebius server."""


class NebiusWorker(WorkerMixin, NebiusInstance):
    """Worker running on a Nebius server."""


class NebiusCluster(VMCluster):
    """Cluster running on Nebius AI Cloud instances.

    VMs in Nebius AI Cloud are referred to as instances. This cluster manager constructs a Dask cluster
    running on VMs.

    When configuring your cluster you may find it useful to install the ``nebius`` tool for querying the
    Nebius API for available options.

    https://docs.nebius.com/cli/quickstart

    Parameters
    ----------
    image_family: str
        The image to use for the host OS. This should be a Ubuntu variant.
        You find list available images here https://docs.nebius.com/compute/storage/manage#parameters-boot.
    project_id: str
        The Nebius AI Cloud project id. You can find in Nebius AI Cloud console.
    server_platform: str
        List of all platforms and presets here https://docs.nebius.com/compute/virtual-machines/types/.
    server_preset: str
        List of all platforms and presets here https://docs.nebius.com/compute/virtual-machines/types/.
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
    env_vars: dict
        Environment variables to be passed to the worker.
    extra_bootstrap: list[str] (optional)
        Extra commands to be run during the bootstrap phase.

    Example
    --------

    >>> from dask_cloudprovider.nebius import NebiusCluster
    >>> cluster = NebiusCluster(n_workers=1)

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    >>> import dask.array as da
    >>> arr = da.random.random((1000, 1000), chunks=(100, 100))
    >>> arr.mean().compute()

    >>> client.close()
    >>> cluster.close()

    """

    def __init__(
        self,
        bootstrap: str = None,
        image_family: str = None,
        project_id: str = None,
        disk_size: int = None,
        server_platform: str = None,
        server_preset: str = None,
        docker_image: str = None,
        debug: bool = False,
        **kwargs,
    ):
        self.config = dask.config.get("cloudprovider.nebius", {})

        self.scheduler_class = NebiusScheduler
        self.worker_class = NebiusWorker

        self.image_family = dask.config.get(
            "cloudprovider.nebius.image_family", override_with=image_family
        )
        self.docker_image = dask.config.get(
            "cloudprovider.nebius.docker_image", override_with=docker_image
        )
        self.project_id = dask.config.get(
            "cloudprovider.nebius.project_id", override_with=project_id
        )
        self.server_platform = dask.config.get(
            "cloudprovider.nebius.server_platform", override_with=server_platform
        )
        self.server_preset = dask.config.get(
            "cloudprovider.nebius.server_preset", override_with=server_preset
        )
        self.bootstrap = dask.config.get(
            "cloudprovider.nebius.bootstrap", override_with=bootstrap
        )
        self.disk_size = dask.config.get(
            "cloudprovider.nebius.disk_size", override_with=disk_size
        )
        self.debug = debug

        self.options = {
            "bootstrap": self.bootstrap,
            "cluster": self,
            "config": self.config,
            "docker_image": self.docker_image,
            "image_family": self.image_family,
            "project_id": self.project_id,
            "server_platform": self.server_platform,
            "server_preset": self.server_preset,
            "disk_size": self.disk_size,
        }
        self.scheduler_options = {**self.options}
        self.worker_options = {**self.options}
        super().__init__(debug=debug, **kwargs)
