import asyncio

import dask
from dask_cloudprovider.generic.vmcluster import (
    VMCluster,
    VMInterface,
    SchedulerMixin,
    WorkerMixin,
)

try:
    import digitalocean
except ImportError as e:
    msg = (
        "Dask Cloud Provider Digital Ocean requirements are not installed.\n\n"
        "Please pip install as follows:\n\n"
        '  pip install "dask-cloudprovider[digitalocean]" --upgrade  # or python -m pip install'
    )
    raise ImportError(msg) from e


class Droplet(VMInterface):
    def __init__(
        self,
        cluster: str,
        config,
        *args,
        region: str = None,
        size: str = None,
        image: str = None,
        docker_image=None,
        env_vars=None,
        extra_bootstrap=None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.droplet = None
        self.cluster = cluster
        self.config = config
        self.region = region
        self.size = size
        self.image = image
        self.gpu_instance = False
        self.bootstrap = True
        self.extra_bootstrap = extra_bootstrap
        self.docker_image = docker_image
        self.env_vars = env_vars

    async def create_vm(self):
        self.droplet = digitalocean.Droplet(
            token=self.config.get("token"),
            name=self.name,
            region=self.region,
            image=self.image,
            size_slug=self.size,
            backups=False,
            user_data=self.cluster.render_process_cloud_init(self),
        )
        await self.cluster.call_async(self.droplet.create)
        for action in self.droplet.get_actions():
            while action.status != "completed":
                action.load()
                await asyncio.sleep(0.1)
        while self.droplet.ip_address is None:
            await self.cluster.call_async(self.droplet.load)
            await asyncio.sleep(0.1)
        self.cluster._log(f"Created droplet {self.name}")

        return self.droplet.ip_address, None

    async def destroy_vm(self):
        await self.cluster.call_async(self.droplet.destroy)
        self.cluster._log(f"Terminated droplet {self.name}")


class DropletScheduler(SchedulerMixin, Droplet):
    """Scheduler running on a DigitalOcean Droplet."""


class DropletWorker(WorkerMixin, Droplet):
    """Worker running on a DigitalOcean Droplet."""


class DropletCluster(VMCluster):
    """Cluster running on Digital Ocean droplets.

    VMs in DigitalOcean (DO) are referred to as droplets. This cluster manager constructs a Dask cluster
    running on VMs.

    When configuring your cluster you may find it useful to install the ``doctl`` tool for querying the
    DO API for available options.

    https://www.digitalocean.com/docs/apis-clis/doctl/how-to/install/

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
        self.config = dask.config.get("cloudprovider.digitalocean", {})
        self.scheduler_class = DropletScheduler
        self.worker_class = DropletWorker
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
