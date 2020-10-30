import asyncio
import os
import uuid

from jinja2 import Environment, FileSystemLoader

import dask.config
from distributed.worker import Worker as _Worker
from distributed.scheduler import Scheduler as _Scheduler
from distributed.deploy.spec import SpecCluster, ProcessInterface
from distributed.utils import warn_on_duration, serialize_for_cli, cli_keywords

from dask_cloudprovider.utils.socket import is_socket_open


class VMInterface(ProcessInterface):
    """A superclass for VM Schedulers, Workers and Nannies."""

    def __init__(self, **kwargs):
        super().__init__()
        self.name = None
        self.command = None
        self.address = None
        self.cluster = None
        self.gpu_instance = None
        self.bootstrap = None
        self.docker_image = "daskdev/dask:latest"
        self.set_env = 'env DASK_INTERNAL_INHERIT_CONFIG="{}"'.format(
            serialize_for_cli(dask.config.global_config)
        )
        self.kwargs = kwargs

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
    """A mixin for Schedulers."""

    def init_scheduler(
        self,
        *args,
        scheduler_options: dict = {},
        **kwargs,
    ):
        self.name = f"dask-{self.cluster.uuid}-scheduler"
        self.command = " ".join(
            [
                self.set_env,
                "python",
                "-m",
                "distributed.cli.dask_scheduler",
            ]
            + cli_keywords(scheduler_options, cls=_Scheduler)
        )

    async def start_scheduler(self):
        self.cluster._log("Creating scheduler instance")
        ip = await self.create_vm()
        self.address = f"tcp://{ip}:8786"
        await self.wait_for_scheduler()


class WorkerMixin(object):
    """A Remote Dask Worker running on a VM."""

    def init_worker(
        self,
        scheduler: str,
        *args,
        worker_module: str = "distributed.cli.dask_worker",
        worker_options: dict = {},
        **kwargs,
    ):
        self.scheduler = scheduler
        self.worker_module = worker_module

        self.name = f"dask-{self.cluster.uuid}-worker-{str(uuid.uuid4())[:8]}"
        self.command = " ".join(
            [
                self.set_env,
                "python",
                "-m",
                self.worker_module,
                self.scheduler,
                "--name",
                str(self.name),
            ]
            + cli_keywords(worker_options, cls=_Worker, cmd=self.worker_module)
        )

    async def start_worker(self):
        self.cluster._log("Creating worker instance")
        self.address = await self.create_vm()


class VMCluster(SpecCluster):
    """A base class for Virtual Machine based cluster managers.

    This class holds logic around starting a scheduler and workers as VMs. This class
    is not intended to be used directly but instead should be subclassed and the attributes
    ``scheduler_class`` and ``worker_class`` should be set.

    The scheduler class should be a subclass of ``VMInterface`` with the ``SchedulerMixin``.
    The worker class should be a subclass of ``VMInterface`` with the ``WorkerMixin``.

    See ``VMInterface`` docstring for required methods.

    For a reference implementation see :class:`DropletCluster`.

    The following paramaters section should be copied to the subclass docstring and appended
    to the provider specific paramaters.

    Parameters
    ----------
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
    silence_logs: bool
        Whether or not we should silence logging when setting up the cluster.
    asynchronous: bool
        If this is intended to be used directly within an event loop with
        async/await
    security : Security or bool, optional
        Configures communication security in this cluster. Can be a security
        object, or True. If True, temporary self-signed credentials will
        be created automatically.

    """

    scheduler_class = None
    worker_class = None
    scheduler_options = {}
    worker_options = {}
    docker_image = None
    command = None
    gpu_instance = None
    bootstrap = None
    auto_shutdown = None

    def __init__(
        self,
        n_workers: int = 0,
        worker_module: str = "distributed.cli.dask_worker",
        worker_options: dict = {},
        scheduler_options: dict = {},
        **kwargs,
    ):
        if self.scheduler_class is None or self.worker_class is None:
            raise RuntimeError(
                "VMCluster is not intended to be used directly. See docstring for more info."
            )
        self._n_workers = n_workers
        self.worker_options["worker_module"] = worker_module
        self.worker_options["worker_options"] = worker_options
        self.scheduler_options["scheduler_options"] = scheduler_options
        self.uuid = str(uuid.uuid4())[:8]
        super().__init__(**kwargs)

    async def _start(
        self,
    ):
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
