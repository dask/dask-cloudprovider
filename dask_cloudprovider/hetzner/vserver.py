import asyncio
import dask

from dask_cloudprovider.generic.vmcluster import (
    VMCluster,
    VMInterface,
    SchedulerMixin,
    WorkerMixin,
)

try:
    import hcloud
except ImportError as e:
    msg = (
        "Dask Cloud Provider Hetzner requirements are not installed.\n\n"
        "Please pip install as follows:\n\n"
        '  pip install "dask-cloudprovider[hcloud]" --upgrade  # or python -m pip install'
    )
    raise ImportError(msg) from e

from hcloud.images.domain import Image
from hcloud.server_types.domain import ServerType
from hcloud.actions.domain import Action


class VServer(VMInterface):
    def __init__(
        self,
        cluster: str,
        config,
        env_vars: dict = None,
        bootstrap=None,
        extra_bootstrap=None,
        docker_image: str = None,
        image: str = None,
        location: str = None,
        server_type: str = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.cluster = cluster
        self.config = config
        self.location = location
        self.bootstrap = bootstrap
        self.extra_bootstrap = extra_bootstrap
        self.env_vars = env_vars
        self.client = hcloud.Client(self.config.get("token"))
        self.server_type = ServerType(server_type)
        self.image = Image(name=image)
        self.docker_image = docker_image

    async def create_vm(self):
        await self.cluster.call_async(
            self.client.servers.create,
            server_type=self.server_type,
            image=self.image,
            name=self.name,
            user_data=self.cluster.render_process_cloud_init(self),
        )

        self.server = self.client.servers.get_by_name(self.name)
        for action in self.server.get_actions():
            while action.status != Action.STATUS_SUCCESS:
                await self.cluster.call_async(action.reload)
                await asyncio.sleep(0.1)
        self.cluster._log(f"Created Hetzner vServer {self.name}")

        return self.server.public_net.ipv4.ip, None

    async def destroy_vm(self):
        await self.cluster.call_async(self.client.servers.delete, server=self.server)
        self.cluster._log(f"Terminated vServer {self.name}")


class HetznerScheduler(SchedulerMixin, VServer):
    """Scheduler running on a Hetzner server."""


class HetznerWorker(WorkerMixin, VServer):
    """Worker running on a Hetzner server."""


class HetznerCluster(VMCluster):
    """Cluster running on Hetzner cloud vServers.

    VMs in Hetzner are referred to as vServers. This cluster manager constructs a Dask cluster
    running on VMs.

    When configuring your cluster you may find it useful to install the ``hcloud`` tool for querying the
    Hetzner API for available options.

    https://github.com/hetznercloud/cli

    Parameters
    ----------
    image: str
        The image to use for the host OS. This should be a Ubuntu variant.
        You can list available images with ``hcloud image list|grep Ubuntu``.
    location: str
        The Hetzner location to launch you cluster in. A full list can be obtained with ``hcloud location list``.
    server_type: str
        The VM server type. You can get a full list with ``hcloud server-type list``.
        The default is ``cx11`` which is vServer with 2GB RAM and 1 vCPU.
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

    >>> from dask_cloudprovider.hetzner import HetznerCluster
    >>> cluster = HetznerCluster(n_workers=1)

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
        image: str = None,
        location: str = None,
        server_type: str = None,
        docker_image: str = None,
        **kwargs,
    ):
        self.config = dask.config.get("cloudprovider.hetzner", {})

        self.scheduler_class = HetznerScheduler
        self.worker_class = HetznerWorker

        self.image = dask.config.get("cloudprovider.hetzner.image", override_with=image)
        self.docker_image = dask.config.get(
            "cloudprovider.hetzner.docker_image", override_with=docker_image
        )
        self.location = dask.config.get(
            "cloudprovider.hetzner.location", override_with=location
        )
        self.server_type = dask.config.get(
            "cloudprovider.hetzner.server_type", override_with=server_type
        )
        self.bootstrap = dask.config.get(
            "cloudprovider.hetzner.bootstrap", override_with=bootstrap
        )

        self.options = {
            "bootstrap": self.bootstrap,
            "cluster": self,
            "config": self.config,
            "docker_image": self.docker_image,
            "image": self.image,
            "location": self.location,
            "server_type": self.server_type,
        }
        self.scheduler_options = {**self.options}
        self.worker_options = {**self.options}
        super().__init__(**kwargs)
