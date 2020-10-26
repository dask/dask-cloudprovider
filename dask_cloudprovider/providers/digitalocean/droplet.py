import asyncio

import dask
from dask_cloudprovider.providers.generic.vmcluster import (
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
        "Please either conda or pip install as follows:\n\n"
        "  conda install dask-cloudprovider                           # either conda install\n"
        '  python -m pip install "dask-cloudprovider[digitalocean]" --upgrade  # or python -m pip install'
    )
    raise ImportError(msg) from e


class Droplet(VMInterface):
    def __init__(
        self, cluster, config, *args, region=None, size=None, image=None, **kwargs
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

    async def create_vm(self):
        self.droplet = digitalocean.Droplet(
            token=self.config.get("token"),
            name=self.name,
            region=self.region,
            image=self.image,
            size_slug=self.size,
            backups=False,
            user_data=self.cluster.render_cloud_init(
                image=self.docker_image,
                command=self.command,
                gpu_instance=self.gpu_instance,
                bootstrap=self.bootstrap,
            ),
        )
        self.droplet.create()
        for action in self.droplet.get_actions():
            while action.status != "completed":
                action.load()
                await asyncio.sleep(0.1)
        while self.droplet.ip_address is None:
            self.droplet.load()
            await asyncio.sleep(0.1)
        self.cluster._log(f"Created droplet {self.name}")

        return self.droplet.ip_address

    async def close(self):
        self.droplet.destroy()
        self.cluster._log(f"Terminated droplet {self.name}")
        await super().close()


class DropletScheduler(Droplet, SchedulerMixin):
    """Scheduler running in an EC2 instance.

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.init_scheduler()

    async def start(self):
        await super().start()
        await self.start_scheduler()


class DropletWorker(Droplet, WorkerMixin):
    """Worker running in an EC2 instance.

    """

    def __init__(self, scheduler, *args, worker_command=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.init_worker(scheduler, *args, worker_command=worker_command, **kwargs)

    async def start(self):
        await super().start()
        await self.start_worker()


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
    worker_command: str
        The Dask worker command to start on worker VMs.

    Examples
    --------

    Create the cluster.

    >>> from dask_cloudprovider import DropletCluster
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
        region="nyc3",
        size="s-1vcpu-1gb",  # 1GB RAM, 1 vCPU
        image="ubuntu-20-04-x64",
        worker_command="dask-worker",
        **kwargs,
    ):
        self.config = dask.config.get("cloudprovider.digitalocean", {})
        self.scheduler_class = DropletScheduler
        self.worker_class = DropletWorker
        self.options = {  # TODO get defaults from config
            "cluster": self,
            "config": self.config,
            "region": region,
            "size": size,
            "image": image,
        }
        self.scheduler_options = {**self.options}
        self.worker_options = {"worker_command": worker_command, **self.options}
        super().__init__(**kwargs)
