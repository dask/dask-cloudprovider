import uuid
import dask
from dask_cloudprovider.generic.vmcluster import (
    VMCluster,
    VMInterface,
    SchedulerMixin,
    WorkerMixin,
)

try:
    from .sdk.models.machines import (
        FlyMachineConfig,
        FlyMachineConfigInit,
        FlyMachineConfigServices,
        FlyMachineRequestConfigServicesPort,
        FlyMachineConfigGuest,
        FlyMachineConfigProcess,
    )
    from .sdk.fly import Fly
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
        region: str = "sjc",
        vm_size: str = "shared-cpu-1x",
        memory_mb=1024,
        cpus=1,
        image="daskdev/dask:latest",
        env_vars={},
        extra_bootstrap=None,
        metadata={},
        restart={},
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.machine = None
        self.cluster = cluster
        self.config = config
        self.region = self.config.get("region", region or "sjc")
        self.vm_size = self.config.get("vm_size", vm_size or "shared-cpu-1x")
        self.cpus = self.config.get("cpus", cpus or 1)
        self.memory_mb = self.config.get("memory_mb", memory_mb or 1024)
        self.image = self.config.get("image", image or "daskdev/dask:latest")
        self.gpu_instance = False
        self.bootstrap = True
        self.extra_bootstrap = extra_bootstrap
        self.env_vars = self.config.get("env_vars", env_vars or {})
        self.metadata = self.config.get("metadata", metadata or {})
        self.restart = restart or {}
        self.app_name = self.cluster.app_name
        # We need the token
        self.api_token = config.get("token")
        if self.api_token is None:
            raise ValueError("Fly.io API token must be provided")
        self.fly = Fly(api_token=self.api_token)

    async def create_vm(self):
        machine_config = FlyMachineConfig(
            env=self.env_vars,
            init=FlyMachineConfigInit(
                cmd=[self.command],
            ),
            image=self.image,
            metadata=self.metadata,
            restart=self.restart,
            services=[
                FlyMachineConfigServices(
                    ports=[
                        FlyMachineRequestConfigServicesPort(port=80, handlers=["http"]),
                        FlyMachineRequestConfigServicesPort(
                            port=443, handlers=["http", "tls"]
                        ),
                        FlyMachineRequestConfigServicesPort(
                            port=8786, handlers=["http", "tls"]
                        ),
                    ],
                    protocol="tcp",
                    internal_port=8786,
                ),
                FlyMachineConfigServices(
                    ports=[
                        FlyMachineRequestConfigServicesPort(
                            port=8787, handlers=["http", "tls"]
                        ),
                    ],
                    protocol="tcp",
                    internal_port=8787,
                ),
            ],
            guest=FlyMachineConfigGuest(
                cpu_kind="shared",
                cpus=self.cpus,
                memory_mb=self.memory_mb,
            ),
            size=self.size,
            metrics=None,
            processes=[
                FlyMachineConfigProcess(
                    name="app",
                    cmd=[self.command],
                    env=self.env_vars,
                )
            ],
        )
        self.machine = self.fly.create_machine(
            app_name=self.config.get("app_name"),  # The name of the new Fly.io app.
            config=machine_config,  # A FlyMachineConfig object containing creation details.
            name=self.name,  # The name of the machine.
            region=self.region,  # The deployment region for the machine.
        )
        self.cluster._log(f"Created machine {self.name}")
        return self.machine.private_ip, None

    async def destroy_vm(self):
        self.fly.destroy_machine(
            app_name=self.config.get("app_name"),
            machine_id=self.machine.id,
        )
        self.cluster._log(f"Terminated machine {self.name}")


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
        The FLY region to launch your cluster in. A full list can be obtained with ``flyctl platform regions``.
    vm_size: str
        The VM size slug. You can get a full list with ``flyctl platform sizes``.
        The default is ``shared-cpu-1x`` which is 256GB RAM and 1 vCPU
    image: str
        The Docker image to run on all instances.

        This image must have a valid Python environment and have ``dask`` installed in order for the
        ``dask-scheduler`` and ``dask-worker`` commands to be available. It is recommended the Python
        environment matches your local environment where ``FlyMachineCluster`` is being created from.

        By default the ``daskdev/dask:latest`` image will be used.
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

    >>> from dask_cloudprovider.fly import FlyMachineCluster
    >>> cluster = FlyMachineCluster(n_workers=1)
    Creating scheduler instance
    Created machine dask-38b817c1-scheduler
    Waiting for scheduler to run
    Scheduler is running
    Creating worker instance
    Created machine dask-38b817c1-worker-dc95260d

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
    Terminated machine dask-38b817c1-worker-dc95260d
    Terminated machine dask-38b817c1-scheduler

    You can also do this all in one go with context managers to ensure the cluster is
    created and cleaned up.

    >>> with FlyMachineCluster(n_workers=1) as cluster:
    ...     with Client(cluster) as client:
    ...         print(da.random.random((1000, 1000), chunks=(100, 100)).mean().compute())
    Creating scheduler instance
    Created machine dask-48efe585-scheduler
    Waiting for scheduler to run
    Scheduler is running
    Creating worker instance
    Created machine dask-48efe585-worker-5181aaf1
    0.5000558682356162
    Terminated machine dask-48efe585-worker-5181aaf1
    Terminated machine dask-48efe585-scheduler

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
        self.app_name = f"dask-{str(uuid.uuid4())[:8]}"
        self.api_token = self.config.get("token")
        self.app = None
        super().__init__(debug=debug, **kwargs)

    def create_app(self):
        """Create a Fly.io app."""
        if self.app is None:
            self._log("Not creating app as it already exists")
            return
        self.app = self.fly.create_app(name=self.app_name)
        self._log(f"Created app {self.app_name}")

    def delete_app(self):
        """Delete a Fly.io app."""
        if self.app is None:
            self._log("Not deleting app as it does not exist")
            return
        self.fly.delete_app(name=self.app_name)
        self._log(f"Deleted app {self.app_name}")
