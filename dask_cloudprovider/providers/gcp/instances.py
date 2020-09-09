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
    def create_docker_space(self):
        spec =f"""spec:
  containers:
    - name: {self.name}
    image: '{self.docker_image}'
    command:
        - {self.command}
    stdin: false
    tty: false
  restartPolicy: Always

# This container declaration format is not public API and may change without notice. Please
# use gcloud command-line tool or Google Cloud Console to run Containers on Google Compute Engine.
"""
        return str(spec)

    def create_gcp_config(self):
        config = {
            "name": self.name,
            "machineType": self.machine_type,
            "tags": {"items": ["http-server", "https-server"]},
            "guestAccelerators": [
                {
                    "acceleratorCount": 1,
                    "acceleratorType": f"projects/{self.projectid}/zones/{self.zone}/acceleratorTypes/nvidia-tesla-t4",
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
                    "deviceName": self.name,
                    "initializeParams": {
                        # "sourceImage": "projects/ubuntu-os-cloud/global/images/ubuntu-minimal-1804-bionic-v20200908",
                        "sourceImage": "projects/nvidia-ngc-public/global/images/nvidia-gpu-cloud-image-20200730",
                        "diskType": f"projects/{self.projectid}/zones/us-central1-a/diskTypes/pd-standard",
                        "diskSizeGb": "32", # nvidia-gpu-cloud cannot be smaller than 32 GB
                        "labels": {},
                    },
                    "diskEncryptionKey": {},
                }
            ],
            "canIpForward": "false",
            "networkInterfaces": [
                {
                    "kind": "compute#networkInterface",
                    "subnetwork": f"projects/{self.projectid}/regions/us-central1/subnetworks/default",
                    "accessConfigs": [
                        {
                            "kind": "compute#accessConfig",
                            "name": "External NAT",
                            "type": "ONE_TO_ONE_NAT",
                            "networkTier": "PREMIUM",
                        }
                    ],
                    "aliasIpRanges": [],
                }
            ],
            # Allow the instance to access cloud storage and logging.
            "serviceAccounts": [
                {
                    "email": "default",
                    "scopes": [
                        "https://www.googleapis.com/auth/devstorage.read_write",
                        "https://www.googleapis.com/auth/logging.write",
                    ],
                }
            ],
            # Metadata is readable from the instance and allows you to
            # pass configuration from deployment scripts to instances.
            "metadata": {
                "items": [
                    {
                        # Startup script is automatically executed by the
                        # instance upon startup.
                        "key": "install-nvidia-driver",
                        "value": "True",
                    },
                    {"key": "gce-container-declaration",
                     "value": self.docker_spec
                    },
                ]
            },
            "scheduling": {
                "preemptible": "false",
                "onHostMaintenance": "TERMINATE",
                "automaticRestart": "true",
                "nodeAffinities": [],
            },
        }
        return config

    async def create_vm(self):
        self.docker_spec = self.create_docker_space()
        self.gcp_config = self.create_gcp_config()
        breakpoint()
        try:
            inst = (
                self.compute.instances()
                .insert(project=self.projectid, zone=self.zone, body=self.gcp_config)
                .execute()
            )
            self.gcp_inst = inst
            self.id = self.gcp_inst["id"]
        except HttpError as e:
            # something failed
            print(str(e))
            raise Exception(str(e))
        while self.update_status() != "RUNNING":
            await asyncio.sleep(0.5)
        return self.get_ip()

    def get_ip(self):
        return (
            self.compute.instances()
            .list(project=self.projectid, zone=self.zone, filter=f"name={self.name}")
            .execute()["items"][0]["networkInterfaces"][0]["accessConfigs"][0]["natIP"]
        )

    def update_status(self):
        d = (
            self.compute.instances()
            .list(project=self.projectid, zone=self.zone, filter=f"name={self.name}")
            .execute()
        )
        self.gcp_inst = d

        if not d.get('items', None):
            print("FAILURE")
            print(self.gcp_inst)
            raise Exception(f"Missing Instance {self.name}")

        return d["items"][0]["status"]

    async def close(self):
        self.compute.instances().delete(
            project=self.projectid, zone=self.zone, instance=self.name
        ).execute()


class GCPScheduler(VMScheduler, GCPMixin):
    """Scheduler running in a GCP Instances."""

    def __init__(
        self,
        cluster,
        name,
        config=None,
        zone=None,
        projectid=None,
        machine_type=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.compute = googleapiclient.discovery.build("compute", "v1")
        self.config = config
        self.cluster = cluster
        self.projectid = projectid
        self.zone = zone
        self.machine_type = f"zones/{zone}/machineTypes/{machine_type}"

        self.name = name + "-scheduler"
        self.address = None
        self.command = "dask-scheduler --idle-timeout 300"

    async def start(self):
        await super().start()
        print("Creating scheduler gcp instance")
        ip = await self.create_vm()
        print(f"Created GCP Instance {self.id}")

        print("Waiting for scheduler to run")
        while not is_socket_open(ip, 8786):
            await asyncio.sleep(0.1)
        print("Scheduler is running")
        self.address = f"tcp://{ip}:8786"


class GCPWorker(VMWorker, GCPMixin):
    """Worker running in a GCP Instance."""

    def __init__(
        self,
        name,
        scheduler,
        cluster,
        config,
        worker_command,
        projectid=None,
        zone=None,
        machine_type=None,
        **kwargs,
    ):
        super().__init__(scheduler)
        self.scheduler = scheduler
        self.cluster = cluster
        self.config = config
        self.projectid = projectid
        self.zone = zone
        self.machine_type = f"zones/{zone}/machineTypes/{machine_type}"
        self.worker_command = worker_command

        self.name = name+f"dask-worker-{str(uuid.uuid4())[:8]}"
        self.address = None
        self.command = f"{self.worker_command} {self.scheduler}"  # FIXME this is ending up as None for some reason

    async def start(self):
        await super().start()
        self.address = await self.create_vm()


class GCPCluster(VMCluster):
    """Cluster running on GCP Instances."""

    def __init__(
        self,
        n_workers=0,
        name="dask-gcp-example",
        zone=None,
        machine_type=None,
        projectid=None,
        worker_command="dask-worker",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.config = dask.config.get("cloudprovider.gcp", {})
        self._n_workers = n_workers
        self.name = name
        self.scheduler_class = GCPScheduler
        self.worker_class = GCPWorker
        self.options = {
            "name": self.name,
            "cluster": self,
            "config": self.config,
            "projectid": projectid,
            "zone": zone,
            "machine_type": machine_type,
        }
        self.scheduler_options = {**self.options}
        self.worker_options = {"worker_command": worker_command, **self.options}


# sudo apt-get update
# sudo apt-get install software-properties-common
# curl -O https://developer.download.nvidia.com/compute/cuda/repos/ubuntu1804/x86_64/cuda-ubuntu1804.pin
# sudo mv cuda-ubuntu1804.pin /etc/apt/preferences.d/cuda-repository-pin-600
# sudo apt-key adv --fetch-keys https://developer.download.nvidia.com/compute/cuda/repos/ubuntu1804/x86_64/7fa2af80.pub
# sudo add-apt-repository "deb http://developer.download.nvidia.com/compute/cuda/repos/ubuntu1804/x86_64/ /"
# sudo apt-get update
# sudo apt-get install cuda-toolkit-11-0 cuda

# nvidia-gpu-cloud-image-20200730
# gcloud compute images list --project=nvidia-ngc-public
# long wait time for nvidia drivers to install

"""
spec:
  containers:
    - name: {self.name}
      image: '{self.docker_image}'
      command:
        - {self.command}
      args:
        - '--ip=0.0.0.0'
        - '--no-bokeh'
      securityContext:
        privileged: true
      env:
        - name: UCX_NVLINK_ENABLED
          value: 'False'
      stdin: false
      tty: false
  restartPolicy: Always

# This container declaration format is not public API and may change without notice. Please
# use gcloud command-line tool or Google Cloud Console to run Containers on Google Compute Engine.
"""