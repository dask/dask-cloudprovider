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
    import googleapiclient.discovery
    from googleapiclient.errors import HttpError
except ImportError as e:
    msg = (
        "Dask Cloud Provider GCP requirements are not installed.\n\n"
        "Please either conda or pip install as follows:\n\n"
        "  conda install google-api-python-client                        # either conda install\n"
        '  python -m pip install "dask-cloudprovider[gcp]" --upgrade  # or python -m pip install'
    )
    raise ImportError(msg) from e


class GCPMixin:
    async def create_vm(self):
        try:
            inst = (
                self.compute.instances()
                .insert(project=self.projectid, zone=self.zone, body=self.config)
                .execute()
            )
            self.gcp_inst = inst
            self.id = self.gcp_inst['id']
        except HttpError as e:
            # something failed
            print(str(e))
        while self.status() != "RUNNING":
            await asyncio.sleep(0.1)
        return await self.get_ip()

    async def close(self):
        self.droplet.destroy()

    async def get_ip(self):
        return (
            self.compute.instances()
            .list(project=self.projectid, zone=self.zone, filter=f"name={self.name}")
            .execute()["items"][0]["networkInterfaces"][0]["accessConfigs"][0]["natIP"]
        )

    async def status(self):
        d = (
            compute.instances()
            .list(project=self.projectid, zone=self.zone, filter=f"name={self.name}")
            .execute()
        )
        self.gcp_inst = d
        return d["items"][0]["status"]

    async def close(self):
        self.compute.instances().delete(
            project=projectid, zone=zone, instance=self.name
        ).execute()


class GCPScheduler(VMScheduler, GCPMixin):
    """Scheduler running in a Digital Ocean droplet.

    """

    def __init__(self, cluster, config=None, zone=None, projectid=None, size=None, **kwargs):
        super().__init__(**kwargs)
        self.compute = googleapiclient.discovery.build("compute", "v1")
        self.config =  config
        self.cluster = cluster
        self.projectid = projectid
        self.zone = zone
        self.size = size

        self.name = "dask-scheduler"
        self.address = None
        self.command = "dask-scheduler --idle-timeout 300"

    async def start(self):
        await super().start()
        self.cluster._log("Creating scheduler gcp instance")
        ip = await self.create_vm()
        self.cluster._log(f"Created GCP Instance {self.id}")

        self.cluster._log("Waiting for scheduler to run")
        while not is_socket_open(ip, 8786):
            await asyncio.sleep(0.1)
        self.cluster._log("Scheduler is running")
        self.address = f"tcp://{ip}:8786"


class GCPWorker(VMWorker, GCPMixin):
    """Worker running in a GCP Instance.

    """

    def __init__(
        self,
        scheduler,
        cluster,
        config,
        worker_command,
        project=None,
        zone=None,
        size=None,
        **kwargs,
    ):
        super().__init__(scheduler)
        self.scheduler = scheduler
        self.cluster = cluster
        self.config = config
        self.projectid = projectid
        self.zone = zone
        self.size = size
        self.worker_command = worker_command

        self.name = f"dask-worker-{str(uuid.uuid4())[:8]}"
        self.address = None
        self.droplet = None
        self.command = f"{self.worker_command} {self.scheduler}"  # FIXME this is ending up as None for some reason

    async def start(self):
        await super().start()
        self.address = await self.create_vm()


class GCPCluster(VMCluster):
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
        self.scheduler_class = GCPScheduler
        self.worker_class = GCPWorker
        self.options = {
            "cluster": self,
            "config": self.config,
            "region": region,
            "size": size,
        }
        self.scheduler_options = {**self.options}
        self.worker_options = {"worker_command": worker_command, **self.options}


_gcp_config = {
    'name': name,
    'machineType': machine_type,
    'tags': {
        'items': ["http-server", "https-server"]
    },
    'guestAccelerators': [
        {
        "acceleratorCount": 1,
        "acceleratorType": f"projects/{projectid}/zones/{zone}/acceleratorTypes/nvidia-tesla-t4"
        }
    ],

    # Specify the boot disk and the image to use as a source.
    "disks": [
    {
      "kind": "compute#attachedDisk",
      "type": "PERSISTENT",
      "boot": "true",
      "mode": "READ_WRITE",
      "autoDelete": "true",
      "deviceName": "dask-rapids-gcp-test",
      "initializeParams": {
        "sourceImage": "projects/debian-cloud/global/images/debian-9-stretch-v20200902",
        "diskType": f"projects/{projectid}/zones/us-central1-a/diskTypes/pd-standard",
        "diskSizeGb": "10",
        "labels": {}
      },
      "diskEncryptionKey": {}
    }
   ],

   "canIpForward": "false",
   "networkInterfaces": [
    {
      "kind": "compute#networkInterface",
      "subnetwork": f"projects/{projectid}/regions/us-central1/subnetworks/default",
      "accessConfigs": [
        {
          "kind": "compute#accessConfig",
          "name": "External NAT",
          "type": "ONE_TO_ONE_NAT",
          "networkTier": "PREMIUM"
        }
      ],
      "aliasIpRanges": []
    }
   ],

    # Allow the instance to access cloud storage and logging.
    'serviceAccounts': [{
        'email': 'default',
        'scopes': [
            'https://www.googleapis.com/auth/devstorage.read_write',
            'https://www.googleapis.com/auth/logging.write'
        ]
    }],



    # Metadata is readable from the instance and allows you to
    # pass configuration from deployment scripts to instances.
    'metadata': {
        'items': [{
            # Startup script is automatically executed by the
            # instance upon startup.
            'key': 'install-nvidia-driver',
            'value': 'True'
        }]
    },
    "scheduling": {
      "preemptible": "false",
      "onHostMaintenance": "TERMINATE",
      "automaticRestart": "true",
      "nodeAffinities": []
    },
}