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
