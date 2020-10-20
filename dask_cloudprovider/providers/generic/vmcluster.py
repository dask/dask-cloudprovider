import asyncio
import os
import uuid

from jinja2 import Environment, FileSystemLoader

from distributed.deploy.spec import SpecCluster, ProcessInterface
from distributed.utils import warn_on_duration

from dask_cloudprovider.utils.socket import is_socket_open


class VMInterface(ProcessInterface):
    """A superclass for VM Schedulers, Workers and Nannies.

    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.name = None
        self.command = None
        self.address = None
        self.cluster = None
        self.gpu_instance = None
        self.bootstrap = None
        self.docker_image = "daskdev/dask:latest"

    def create_vm(self):
        raise NotImplementedError("create_vm is a required method of the VMInterface")

    async def wait_for_scheduler(self):
        _, address = self.address.split("://")
        ip, port = address.split(":")

        self.cluster._log("Waiting for scheduler to run")
        while not is_socket_open(ip, port):
            await asyncio.sleep(0.1)
        self.cluster._log("Scheduler is running")

    async def start(self):
        """Create a VM."""
        await super().start()

    async def close(self):
        """Destroy a VM."""
        await super().close()


class SchedulerMixin(object):
    """A mixin for Schedulers.

    """

    def init_scheduler(self,):
        self.name = "dask-scheduler"
        self.command = "dask-scheduler --idle-timeout 300"

    async def start_scheduler(self):
        self.cluster._log("Creating scheduler instance")
        ip = await self.create_vm()
        self.address = f"tcp://{ip}:8786"
        await self.wait_for_scheduler()


class WorkerMixin(object):
    """A Remote Dask Worker running on a VM.

    """

    def init_worker(self, scheduler: str, *args, worker_command=None, **kwargs):
        self.scheduler = scheduler
        self.worker_command = worker_command

        self.name = f"dask-worker-{str(uuid.uuid4())[:8]}"
        self.command = f"{self.worker_command} {self.scheduler}"

    async def start_worker(self):
        self.cluster._log("Creating worker instance")
        self.address = await self.create_vm()


class VMCluster(SpecCluster):
    def __init__(self, n_workers=0, **kwargs):
        self._n_workers = n_workers
        self.scheduler_class = None
        self.worker_class = None
        self.scheduler_options = {}
        self.worker_options = {}
        self.docker_image = None
        self.command = None
        self.gpu_instance = None
        self.bootstrap = None
        self.auto_shutdown = None
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

    def render_cloud_init(self, *args, **kwargs):
        loader = FileSystemLoader([os.path.dirname(os.path.abspath(__file__))])
        environment = Environment(loader=loader)
        template = environment.get_template("cloud-init.yaml.j2")
        return template.render(**kwargs)

    @classmethod
    def get_cloud_init(cls, *args, **kwargs):
        cluster = cls(*args, asynchronous=True, **kwargs)
        cluster.auto_shutdown = False
        return cluster.render_cloud_init(
            image=cluster.docker_image,
            command="dask-scheduler --version",
            gpu_instance=cluster.gpu_instance,
            bootstrap=cluster.bootstrap,
            auto_shutdown=cluster.auto_shutdown,
        )
