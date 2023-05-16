import asyncio

import dask
from dask_cloudprovider.generic.vmcluster import (
    VMCluster,
    VMInterface,
    SchedulerMixin,
    WorkerMixin,
)

try:
    import fly_python_sdk
except ImportError as e:
    msg = (
        "Dask Cloud Provider Fly.io requirements are not installed.\n\n"
        "Please pip install as follows:\n\n"
        '  pip install "dask-cloudprovider[fly]" --upgrade  # or python -m pip install'
    )
    raise ImportError(msg) from e


class FlyMachine(VMInterface):
    def __init__(
        self,
        cluster: str,
        config,
        *args,
        region: str = None,
        # size: str = None,
        docker_image = None,
        env_vars = None,
        extra_bootstrap = None,
        machine_id = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.machine = None
        self.cluster = cluster
        self.config = config
        self.region = region
        self.size = self.config.get("size", "shared-cpu-1x")
        self.cpus = self.config.get("cpus", 1)
        self.memory_mb = self.config.get("memory_mb", 1024)
        self.image = None
        self.gpu_instance = False
        self.bootstrap = True
        self.extra_bootstrap = extra_bootstrap
        self.docker_image = docker_image
        self.env_vars = env_vars

    async def create_vm(self):
        config = fly_python_sdk.models.machines.FlyMachineConfig(
            env=self.env_vars,
            init=fly_python_sdk.models.machines.FlyMachineConfigInit(
                cmd=[self.command],
            ),
            image=self.image,
            metadata=None,
            restart=None,
            services=[
                fly_python_sdk.models.machines.FlyMachineConfigServices(
                    ports=[
                        fly_python_sdk.models.machines.FlyMachineConfigServicesPorts(
                            port=8786,
                            handlers=["http", "tls"]
                        ),
                    ],
                    protocol="tcp",
                    internal_port=8786,
                ),
                fly_python_sdk.models.machines.FlyMachineConfigServices(
                    ports=[
                        fly_python_sdk.models.machines.FlyMachineConfigServicesPorts(
                            port=8787,
                            handlers=["http", "tls"]
                        ),
                    ],
                    protocol="tcp",
                    internal_port=8787,
                ),
            ],
            guest=fly_python_sdk.models.machines.FlyMachineConfigGuest(
                cpu_kind="shared",
                cpus=self.cpus,
                memory_mb=self.memory_mb,
            ),
            size=self.size,
            metrics=None,
            processes=[
                fly_python_sdk.models.machines.FlyMachineConfigProcess(
                    name="app",
                    cmd=[self.command],
                    env=self.env_vars,
                )
            ]
        )
        self.machine = fly_python_sdk.create_machine(
            app_name=self.config.get("app_name"),
            token=self.config.get("token"),
            name=self.name,
            region=self.region,
            image=self.image,
            size_slug=self.size,
            backups=False,
            user_data=self.cluster.render_process_cloud_init(self),
        )
        self.cluster._log(f"Created machine {self.name}")
        return self.machine.private_ip, None

    async def destroy_vm(self):
        self.destroy_machine(
            app_name=self.config.get("app_name"),
            machine_id=self.machine.id,
        )
        self.cluster._log(f"Terminated droplet {self.name}")


class FlyMachineScheduler(SchedulerMixin, FlyMachine):
    """Scheduler running on a Fly.io Machine."""


class FlyMachineWorker(WorkerMixin, FlyMachine):
    """Worker running on a Fly.io Machine."""


class FlyMachineCluster(VMCluster):
    """Cluster running on Fly.io Machines.

    VMs in Fly.io (FLY) are referred to as machines. This cluster manager constructs a Dask cluster
    running on VMs.

    When configuring your cluster you may find it useful to install the ``flyctl`` tool for querying the
    CLY API for available options.

    https://fly.io/docs/hands-on/install-flyctl/

    Parameters
    ----------
    region: str
        The DO region to launch you cluster in. A full list can be obtained with ``doctl compute region list``.
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
    docker_args: string (optional)
        Extra command line arguments to pass to Docker.
    extra_bootstrap: list[str] (optional)
        Extra commands to be run during the bootstrap phase.
    env_vars: dict (optional)
        Environment variables to be passed to the worker.
    silence_logs: bool
        Whether or not we should silence logging when setting up the cluster.
    asynchronous: bool
        If this is intended to be used directly within an event loop with
        async/await
    security : Security or bool, optional
        Configures communication security in this cluster. Can be a security
        object, or True. If True, temporary self-signed credentials will
        be created automatically. Default is ``True``.
    debug: bool, optional
        More information will be printed when constructing clusters to enable debugging.

    Examples
    --------

    Create the cluster.

    >>> from dask_cloudprovider.digitalocean import DropletCluster
    >>> cluster = DropletCluster(n_workers=1)
    Creating scheduler instance
    Created droplet dask-38b817c1-scheduler
    Waiting for scheduler to run
    Scheduler is running
    Creating worker instance
    Created droplet dask-38b817c1-worker-dc95260d

    Connect a client.

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    Do some work.

    >>> import dask.array as da
    >>> arr = da.random.random((1000, 1000), chunks=(100, 100))
    >>> arr.mean().compute()
    0.5001550986751964

    Close the cluster

    >>> client.close()
    >>> cluster.close()
    Terminated droplet dask-38b817c1-worker-dc95260d
    Terminated droplet dask-38b817c1-scheduler

    You can also do this all in one go with context managers to ensure the cluster is
    created and cleaned up.

    >>> with DropletCluster(n_workers=1) as cluster:
    ...     with Client(cluster) as client:
    ...         print(da.random.random((1000, 1000), chunks=(100, 100)).mean().compute())
    Creating scheduler instance
    Created droplet dask-48efe585-scheduler
    Waiting for scheduler to run
    Scheduler is running
    Creating worker instance
    Created droplet dask-48efe585-worker-5181aaf1
    0.5000558682356162
    Terminated droplet dask-48efe585-worker-5181aaf1
    Terminated droplet dask-48efe585-scheduler

    """

    def __init__(
        self,
        region: str = None,
        size: str = None,
        image: str = None,
        debug: bool = False,
        **kwargs,
    ):
        self.config = dask.config.get("cloudprovider.fly", {})
        self.scheduler_class = FlyMachineScheduler
        self.worker_class = FlyMachineWorker
        self.debug = debug
        self.options = {
            "cluster": self,
            "config": self.config,
            "region": region if region is not None else self.config.get("region"),
            "size": size if size is not None else self.config.get("size"),
            "image": image if image is not None else self.config.get("image"),
        }
        self.scheduler_options = {**self.options}
        self.worker_options = {**self.options}
        super().__init__(debug=debug, **kwargs)
