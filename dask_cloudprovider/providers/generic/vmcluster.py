import asyncio
import os

from jinja2 import Environment, FileSystemLoader

from distributed.deploy.spec import SpecCluster, ProcessInterface
from distributed.utils import warn_on_duration


class VM(ProcessInterface):
    """A superclass for VM Schedulers, Workers and Nannies.

    """

    def __init__(self, **kwargs):
        self.docker_image = "daskdev/dask:latest"
        super().__init__(**kwargs)

    def render_cloud_init(self, *args, **kwargs):
        loader = FileSystemLoader([os.path.dirname(os.path.abspath(__file__))])
        environment = Environment(loader=loader)
        template = environment.get_template("cloud-init.yaml.j2")
        return template.render(*args, **kwargs)

    async def start(self):
        """Create a VM."""
        await super().start()

    async def close(self):
        """Destroy a VM."""
        await super().close()


class VMScheduler(VM):
    """A Remote Dask Scheduler running on a VM.

    """

    def __init__(self,):
        super().__init__()
        self.address = None

    async def start(self):
        await super().start()


class VMWorker(VM):
    """A Remote Dask Worker running on a VM.

    """

    def __init__(self, scheduler: str):
        super().__init__()
        self.address = None
        self.scheduler = scheduler

    async def start(self):
        await super().start()


class VMCluster(SpecCluster):
    def __init__(self, n_workers=0, **kwargs):
        self._n_workers = n_workers
        self.scheduler_class = VMScheduler
        self.worker_class = VMWorker
        self.scheduler_options = {}
        self.worker_options = {}
        super().__init__(**kwargs)

    async def _start(self,):
        while self.status == "starting":
            await asyncio.sleep(0.01)
        if self.status == "running":
            return
        if self.status == "closed":
            raise ValueError("Cluster is closed")

        self.scheduler_spec = {
            "cls": self.scheduler_class,
            "options": self.scheduler_options,
        }
        self.new_spec = {"cls": self.worker_class, "options": self.worker_options}
        self.worker_spec = {i: self.new_spec for i in range(self._n_workers)}

        with warn_on_duration(
            "10s",
            "Creating your cluster is taking a surprisingly long time. "
            "This is likely due to pending resources. "
            "Hang tight! ",
        ):
            await super()._start()
