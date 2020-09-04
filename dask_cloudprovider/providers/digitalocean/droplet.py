import asyncio
import uuid

import dask
from dask_cloudprovider.providers.generic.vmcluster import (
    VMCluster,
    VMScheduler,
    VMWorker,
)
from dask_cloudprovider.utils.socket import is_socket_open

try:
    import digitalocean
except ImportError as e:
    msg = (
        "Dask Cloud Provider AWS requirements are not installed.\n\n"
        "Please either conda or pip install as follows:\n\n"
        "  conda install dask-cloudprovider                           # either conda install\n"
        '  python -m pip install "dask-cloudprovider[digitalocean]" --upgrade  # or python -m pip install'
    )
    raise ImportError(msg) from e


class DropletMixin:
    async def create_droplet(self):
        # FIXME make configurable
        self.droplet = digitalocean.Droplet(
            token=self.config.get("token"),
            name=self.name,
            region=self.region or "nyc3",
            image="ubuntu-20-04-x64",  # Ubuntu 20.04 x64
            size_slug=self.size or "s-1vcpu-1gb",  # 1GB RAM, 1 vCPU
            backups=False,
            user_data=self.render_cloud_init(
                image=self.docker_image, command=self.command
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

        return self.droplet.ip_address

    async def close(self):
        self.droplet.destroy()


class DropletScheduler(VMScheduler, DropletMixin):
    """Scheduler running in a Digital Ocean droplet.

    """

    def __init__(self, cluster, config, region=None, size=None, **kwargs):
        super().__init__(**kwargs)
        self.cluster = cluster
        self.config = config
        self.region = region
        self.size = size

        self.name = "dask-scheduler"
        self.address = None
        self.droplet = None
        self.command = "dask-scheduler --idle-timeout 300"

    async def start(self):
        await super().start()
        self.cluster._log("Creating scheduler droplet")
        ip = await self.create_droplet()
        self.cluster._log(f"Created droplet {self.droplet.id}")

        self.cluster._log("Waiting for scheduler to run")
        while not is_socket_open(ip, 8786):
            await asyncio.sleep(0.1)
        self.cluster._log("Scheduler is running")
        self.address = f"tcp://{ip}:8786"


class DropletWorker(VMWorker, DropletMixin):
    """Worker running in a Digital Ocean droplet.

    """

    def __init__(
        self,
        scheduler,
        cluster,
        config,
        worker_command,
        region=None,
        size=None,
        **kwargs,
    ):
        super().__init__(scheduler)
        self.scheduler = scheduler
        self.cluster = cluster
        self.config = config
        self.region = region
        self.size = size
        self.worker_command = worker_command

        self.name = f"dask-worker-{str(uuid.uuid4())[:8]}"
        self.address = None
        self.droplet = None
        self.command = (
            f"{self.worker_command} {self.scheduler}"
        )  # FIXME this is ending up as None for some reason

    async def start(self):
        await super().start()
        self.address = await self.create_droplet()


class DropletCluster(VMCluster):
    """Cluster running on Digital Ocean droplets.

    """

    def __init__(
        self,
        n_workers=0,
        region=None,
        size=None,
        worker_command="dask-worker",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.config = dask.config.get("cloudprovider.digitalocean", {})
        self._n_workers = n_workers
        self.scheduler_class = DropletScheduler
        self.worker_class = DropletWorker
        self.options = {
            "cluster": self,
            "config": self.config,
            "region": region,
            "size": size,
        }
        self.scheduler_options = {**self.options}
        self.worker_options = {"worker_command": worker_command, **self.options}
