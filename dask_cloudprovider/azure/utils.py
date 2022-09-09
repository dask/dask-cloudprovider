import asyncio
import datetime
import json
import subprocess
import logging

import aiohttp
from distributed.diagnostics.plugin import WorkerPlugin
from tornado.ioloop import IOLoop, PeriodicCallback


logger = logging.getLogger(__name__)

AZURE_EVENTS_METADATA_URL = (
    "http://169.254.169.254/metadata/scheduledevents?api-version=2019-08-01"
)


def _get_default_subscription() -> str:
    """
    Get the default Azure subscription ID, as configured by the Azure CLI.
    """
    out = subprocess.check_output(["az", "account", "list", "--query", "[?isDefault]"])
    accounts = json.loads(out)
    if accounts:
        subscription_id = accounts[0]["id"]
        return subscription_id
    raise ValueError(
        "Could not find a default subscription. "
        "Run 'az account set' to set a default subscription."
    )


class AzurePreemptibleWorkerPlugin(WorkerPlugin):
    """A worker plugin for azure spot instances

    This worker plugin will poll azure's metadata service for preemption notifications.
    When a node is preempted, the plugin will attempt to shutdown gracefully all workers
    on the node.

    This plugin can be used on any worker running on azure spot instances, not just the
    ones created by ``dask-cloudprovider``.

    For more details on azure spot instances see:
    https://docs.microsoft.com/en-us/azure/virtual-machines/linux/scheduled-events

    Parameters
    ----------
    poll_interval_s: int (optional)
        The rate at which the plugin will poll the metadata service in seconds.

        Defaults to ``1``

    metadata_url: str (optional)
        The url of the metadata service to poll.

        Defaults to "http://169.254.169.254/metadata/scheduledevents?api-version=2019-08-01"

    termination_events: List[str] (optional)
        The type of events that will trigger the gracefull shutdown

        Defaults to ``['Preempt', 'Terminate']``

    termination_offset_minutes: int (optional)
        Extra offset to apply to the premption date. This may be negative, to start
        the gracefull shutdown before the ``NotBefore`` date. It can also be positive, to
        start the shutdown after the ``NotBefore`` date, but this is at your own risk.

        Defaults to ``0``

    Examples
    --------

    Let's say you have cluster and a client instance.
    For example using :class:`dask_kubernetes.KubeCluster`

    >>> from dask_kubernetes import KubeCluster
    >>> from distributed import Client
    >>> cluster = KubeCluster()
    >>> client = Client(cluster)

    You can add the worker plugin using the following:

    >>> from dask_cloudprovider.azure import AzurePreemptibleWorkerPlugin
    >>> client.register_worker_plugin(AzurePreemptibleWorkerPlugin())
    """

    def __init__(
        self,
        poll_interval_s=1,
        metadata_url=None,
        termination_events=None,
        termination_offset_minutes=0,
    ):
        self.callback = None
        self.loop = None
        self.worker = None
        self.poll_interval_s = poll_interval_s
        self.metadata_url = metadata_url or AZURE_EVENTS_METADATA_URL
        self.termination_events = termination_events or ["Preempt", "Terminate"]
        self.termination_offset = datetime.timedelta(minutes=termination_offset_minutes)

        self.terminating = False
        self.not_before = None
        self._session = None
        self._lock = None

    async def _is_terminating(self):
        preempt_started = False
        async with self._session.get(self.metadata_url) as response:
            try:
                data = await response.json()
            # Sometime azure responds with text/plain mime type
            except aiohttp.ContentTypeError:
                return
            # Sometimes the response doesn't contain the Events key
            events = data.get("Events", [])
            if events:
                logger.debug(
                    "Worker {}, got metadata events {}".format(self.worker.name, events)
                )
            for evt in events:
                event_type = evt["EventType"]
                if event_type not in self.termination_events:
                    continue

                event_status = evt.get("EventStatus")
                if event_status == "Started":
                    logger.info(
                        "Worker {}, node preemption started".format(self.worker.name)
                    )
                    preempt_started = True
                    break

                not_before = evt.get("NotBefore")
                if not not_before:
                    continue

                not_before = datetime.datetime.strptime(
                    not_before, "%a, %d %b %Y %H:%M:%S GMT"
                )
                if self.not_before is None:
                    logger.info(
                        "Worker {}, node deletion scheduled not before {}".format(
                            self.worker.name, self.not_before
                        )
                    )
                    self.not_before = not_before
                    break
                if self.not_before < not_before:
                    logger.info(
                        "Worker {}, node deletion re-scheduled not before {}".format(
                            self.worker.name, not_before
                        )
                    )
                    self.not_before = not_before
                    break

        return preempt_started or (
            self.not_before
            and (self.not_before + self.termination_offset < datetime.datetime.utcnow())
        )

    async def poll_status(self):
        if self.terminating:
            return
        if self._session is None:
            self._session = aiohttp.ClientSession(headers={"Metadata": "true"})
        if self._lock is None:
            self._lock = asyncio.Lock()

        async with self._lock:
            is_terminating = await self._is_terminating()
            if not is_terminating:
                return

            logger.info(
                "Worker {}, node is being deleted, attempting graceful shutdown".format(
                    self.worker.name
                )
            )
            self.terminating = True
            await self._session.close()
            await self.worker.close_gracefully()

    def setup(self, worker):
        self.worker = worker
        self.loop = IOLoop.current()
        self.callback = PeriodicCallback(
            self.poll_status, callback_time=self.poll_interval_s * 1_000
        )
        self.loop.add_callback(self.callback.start)
        logger.debug(
            "Worker {}, registering preemptible plugin".format(self.worker.name)
        )

    def teardown(self, worker):
        logger.debug("Worker {}, tearing down plugin".format(self.worker.name))
        if self.callback:
            self.callback.stop()
            self.callback = None
