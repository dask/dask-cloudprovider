import asyncio
import uuid

import dask
from dask_cloudprovider.providers.generic.vmcluster import (
    VMCluster,
    VMInterface,
    SchedulerMixin,
    WorkerMixin,
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


class GCPInstance(VMInterface):
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
        self.cluster = cluster
        self.name = name
        self.config = config
        self.projectid = projectid
        self.zone = zone
        self.machine_type = f"zones/{zone}/machineTypes/{machine_type}"

    def create_docker_space(self):
        spec =f"""spec:
  containers:
    - name: {self.name}
      image: 'gcr.io/nv-ai-infra/rapidsai-nightly:0.16-cuda11.0-runtime-ubuntu18.04'
      command:
        - {self.command}
      securityContext:
        privileged: true
      stdin: false
      tty: false
  restartPolicy: Always

# This container declaration format is not public API and may change without notice. Please
# use gcloud command-line tool or Google Cloud Console to run Containers on Google Compute Engine.
"""
        return str(spec)

    def create_cloud_init(self):
        spec = f"""#cloud-config
packages:
  - apt-transport-https
  - ca-certificates
  - curl
  - gnupg-agent
  - software-properties-common
  - tmux

runcmd:
  # Run container
  - docker run --gpus=all --net=host rapidsai/rapidsai-nightly:0.16-cuda11.0-runtime-ubuntu18.04 {self.command}
"""
        return spec


    def create_gcp_config(self):
        config = {
            "name": self.name,
            "machineType": self.machine_type,
            "displayDevice": {
                "enableDisplay": "false"
            },
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
                        "sourceImage": "projects/nv-ai-infra/global/images/ngc-docker-11-20200916",
                        # "sourceImage": "projects/cos-cloud/global/images/cos-beta-85-13310-1040-0", # has to be cos 85
                        # "sourceImage": "projects/nvidia-ngc-public/global/images/nvidia-gpu-cloud-image-20200730",
                        # "sourceImage": "projects/nvidia-ngc-public/global/images/nvidia-gpu-cloud-image-pytorch-20200730",
                        "diskType": f"projects/{self.projectid}/zones/{self.zone}/diskTypes/pd-standard",
                        "diskSizeGb": "50", # nvidia-gpu-cloud cannot be smaller than 32 GB
                        "labels": {},
                        "source": "projects/nv-ai-infra/zones/us-east1-c/disks/ngc-gpu-dask-rapids-docker-experiment",
                    },
                    "diskEncryptionKey": {},
                }
            ],
            "canIpForward": "false",
            "networkInterfaces": [
                {
                    "kind": "compute#networkInterface",
                    "subnetwork": f"projects/{self.projectid}/regions/us-east1/subnetworks/default",
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
                        "key": "google-logging-enabled",
                        "value": "true",
                    },
                    {"key": "gce-container-declaration",
                     "value": self.docker_spec
                    },
                    {"key": "user-data",
                     "value": self.cloud_init
                    }
                ]
            },
            "labels": {
                "container-vm": "cos-stable-81-12871-1196-0"
            },
            "scheduling": {
                "preemptible": "false",
                "onHostMaintenance": "TERMINATE",
                "automaticRestart": "true",
                "nodeAffinities": [],
            },
            "shieldedInstanceConfig": {
                "enableSecureBoot": "false",
                "enableVtpm": "true",
                "enableIntegrityMonitoring": "true"
            },
            "deletionProtection": "false",
            "reservationAffinity": {
                "consumeReservationType": "ANY_RESERVATION"
            },
        }
        return config

    async def create_vm(self):
        self.docker_spec = self.create_docker_space()
        self.cloud_init = self.create_cloud_init()
        self.gcp_config = self.create_gcp_config()
        try:
            inst = (
                self.cluster.compute.instances()
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

        self.internal_ip = self.get_internal_ip()
        self.external_ip = self.get_external_ip()
        print("IP ADDR: ", self.internal_ip, self.external_ip)
        return self.internal_ip, self.external_ip

    def get_internal_ip(self):
        return (
            self.cluster.compute.instances()
            .list(project=self.projectid, zone=self.zone, filter=f"name={self.name}")
            .execute()["items"][0]["networkInterfaces"][0]['networkIP']
        )

    def get_external_ip(self):
        return (
            self.cluster.compute.instances()
            .list(project=self.projectid, zone=self.zone, filter=f"name={self.name}")
            .execute()["items"][0]["networkInterfaces"][0]["accessConfigs"][0]["natIP"]
        )

    def update_status(self):
        d = (
            self.cluster.compute.instances()
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
        self.cluster.compute.instances().delete(
            project=self.projectid, zone=self.zone, instance=self.name
        ).execute()


class GCPScheduler(GCPInstance, SchedulerMixin):
    """Scheduler running in a GCP instance.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.command = "dask-scheduler --host 0.0.0.0"
        self.name = "aa-dask-scheduler"

    async def start(self):
        await super().start()
        await self.start_scheduler()


    async def start_scheduler(self):
        self.cluster._log("Creating scheduler instance")
        self.internal_ip, self.external_ip = await self.create_vm()
        self.address = f"tcp://{self.external_ip}:8786"
        await self.wait_for_scheduler()

        # need to reserve internal IP for workers
        # gcp docker containers can't see resolve ip address
        self.cluster.scheduler_internal_ip = self.internal_ip
        self.cluster.scheduler_external_ip = self.external_ip



class GCPWorker(GCPInstance, WorkerMixin):
    """Worker running in an GCP instance.
    """

    def __init__(self, scheduler, *args, worker_command=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.init_worker(scheduler, *args, worker_command=worker_command, **kwargs)

    async def start(self):
        await super().start()
        await self.start_worker()

    def init_worker(self, scheduler: str, *args, worker_command=None, **kwargs):
        self.scheduler = scheduler
        self.worker_command = worker_command

        print('# use internal address for workers')
        self.name = f"dask-worker-{str(uuid.uuid4())[:8]}"
        self.command = f"{self.worker_command} {self.cluster.scheduler_internal_ip}:8786"
        print(self.command)


    async def start_worker(self):
        self.cluster._log("Creating worker instance")
        self.internal_ip, self.external_ip = await self.create_vm()
        self.address = self.external_ip


# class GCPScheduler(GCPInstance):
#     """Scheduler running in a GCP Instances."""

#     def __init__(
#         self,
#         cluster,
#         name,
#         config=None,
#         zone=None,
#         projectid=None,
#         machine_type=None,
#         **kwargs,
#     ):
#         super().__init__(**kwargs)
#         self.compute = googleapiclient.discovery.build("compute", "v1")
#         self.config = config
#         self.cluster = cluster
#         self.projectid = projectid
#         self.zone = zone
#         self.machine_type = f"zones/{zone}/machineTypes/{machine_type}"

#         self.name = name + "-scheduler"
#         self.address = None
#         self.command = "dask-scheduler --host 0.0.0.0"

    # async def start(self):
    #     await super().start()
    #     print("Creating scheduler gcp instance")
    #     self.internal_ip, self.external_ip = await self.create_vm()
    #     print(f"Created GCP Instance {self.id}")

    #     print("Waiting for scheduler to run")
    #     while not is_socket_open(self.external_ip, 8786):
    #         await asyncio.sleep(0.1)
    #     print("Scheduler is running")
    #     print(f"Command: {self.command}")

    #     self.address = f"tcp://{self.external_ip}:8786"


# class GCPWorker(VMWorker, GCPMixin):
#     """Worker running in a GCP Instance."""

#     def __init__(
#         self,
#         scheduler,
#         cluster,
#         config,
#         worker_command,
#         projectid=None,
#         zone=None,
#         machine_type=None,
#         **kwargs,
#     ):
#         super().__init__(scheduler)
#         self.scheduler = scheduler
#         self.compute = googleapiclient.discovery.build("compute", "v1")
#         self.cluster = cluster
#         self.config = config
#         self.projectid = projectid
#         self.zone = zone
#         self.machine_type = f"zones/{zone}/machineTypes/{machine_type}"
#         self.worker_command = worker_command

#         self.name = f"dask-worker-{str(uuid.uuid4())[:8]}"
#         self.address = None
#         breakpoint()
#         self.command = f"{self.worker_command} {self.scheduler}"  # FIXME this is ending up as None for some reason

#     async def start(self):
#         await super().start()
#         breakpoint()
#         self.address = await self.create_vm()
#         # print(f"Command: {self.command}")



class GCPCluster(VMCluster):
    """Cluster running on GCP Instances."""

    def __init__(
        self,
        name="dask-gcp-example",
        zone=None,
        machine_type=None,
        projectid=None,
        worker_command="dask-cuda-worker",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.compute = googleapiclient.discovery.build("compute", "v1")
        self.config = dask.config.get("cloudprovider.gcp", {})
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
# docker pull rapidsai/rapidsai-nightly:0.16-cuda11.0-runtime-ubuntu18.04
# sudo journalctl -u konlet-startup
# #! /bin/bash
# cos-extensions install gpu
# sudo mount --bind /var/lib/nvidia /var/lib/nvidia
# sudo mount -o remount,exec /var/lib/nvidia
# /var/lib/nvidia/bin/nvidia-smi"
# tail -f /var/log/cloud-init.log /var/log/cloud-init-output.log

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
