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
    from google.auth.exceptions import DefaultCredentialsError
except ImportError as e:
    msg = (
        "Dask Cloud Provider GCP requirements are not installed.\n\n"
        "Please either conda or pip install as follows:\n\n"
        "  conda install -c conda-forge google-api-python-client  # either conda install\n"
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
        filesystem_size=None,
        source_image=None,
        docker_image=None,
        ngpus=None,
        gpu_type=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cluster = cluster
        self.name = name
        self.config = config
        self.projectid = projectid or self.config.get("projectid")
        self.zone = zone or self.config.get("zone")

        self.machine_type = machine_type or self.config.get("machine_type")

        self.source_image = source_image or self.config.get("source_image")
        self.docker_image = docker_image or self.config.get("docker_image")
        self.filesystem_size = filesystem_size or self.config.get("filesystem_size")
        self.ngpus = ngpus or self.config.get("ngpus")
        self.gpu_type = gpu_type or self.config.get("gpu_type")

        self.general_zone = "-".join(self.zone.split("-")[:2])  # us-east1-c -> us-east1

    def create_gcp_config(self):
        config = {
            "name": self.name,
            "machineType": f"zones/{self.zone}/machineTypes/{self.machine_type}",
            "displayDevice": {"enableDisplay": "false"},
            "tags": {"items": ["http-server", "https-server"]},
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
                        "sourceImage": self.source_image,
                        "diskType": f"projects/{self.projectid}/zones/{self.zone}/diskTypes/pd-standard",
                        "diskSizeGb": "50",  # nvidia-gpu-cloud cannot be smaller than 32 GB
                        "labels": {},
                        # "source": "projects/nv-ai-infra/zones/us-east1-c/disks/ngc-gpu-dask-rapids-docker-experiment",
                    },
                    "diskEncryptionKey": {},
                }
            ],
            "canIpForward": "false",
            "networkInterfaces": [
                {
                    "kind": "compute#networkInterface",
                    "subnetwork": f"projects/{self.projectid}/regions/{self.general_zone}/subnetworks/default",
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
                    {"key": "user-data", "value": self.cloud_init},
                ]
            },
            "labels": {"container-vm": "dask-cloudprovider"},
            "scheduling": {
                "preemptible": "false",
                "onHostMaintenance": "TERMINATE",
                "automaticRestart": "true",
                "nodeAffinities": [],
            },
            "shieldedInstanceConfig": {
                "enableSecureBoot": "false",
                "enableVtpm": "true",
                "enableIntegrityMonitoring": "true",
            },
            "deletionProtection": "false",
            "reservationAffinity": {"consumeReservationType": "ANY_RESERVATION"},
        }

        if self.ngpus:
            config["guestAccelerators"] = [
                {
                    "acceleratorCount": self.ngpus,
                    "acceleratorType": f"projects/{self.projectid}/zones/{self.zone}/acceleratorTypes/{self.gpu_type}",
                }
            ]

        return config

    async def create_vm(self):
        self.cloud_init = self.cluster.render_cloud_init(
            image=self.docker_image,
            command=self.command,
            gpu_instance=bool(self.ngpus),
            bootstrap=False,
            auto_shutdown=self.cluster.auto_shutdown,
        )

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
        self.cluster._log(f"{self.name}\n\tInternal IP: {self.internal_ip}\n\tExternal IP: {self.external_ip}")
        return self.internal_ip, self.external_ip

    def get_internal_ip(self):
        return (
            self.cluster.compute.instances()
            .list(project=self.projectid, zone=self.zone, filter=f"name={self.name}")
            .execute()["items"][0]["networkInterfaces"][0]["networkIP"]
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

        if not d.get("items", None):
            self.cluster._log("Failed to find running VMI...")
            self.cluster._log(self.gcp_inst)
            raise Exception(f"Missing Instance {self.name}")

        return d["items"][0]["status"]

    async def close(self):
        self.cluster._log(f"Closing Instance: {self.name}")
        self.cluster.compute.instances().delete(
            project=self.projectid, zone=self.zone, instance=self.name
        ).execute()


class GCPScheduler(GCPInstance, SchedulerMixin):
    """Scheduler running in a GCP instance."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.command = "dask-scheduler --host 0.0.0.0"
        self.name = f"dask-scheduler-{str(uuid.uuid4())[:8]}"

    async def start(self):
        await super().start()
        await self.start_scheduler()

    async def start_scheduler(self):
        self.cluster._log(f"Launching cluster with the following configuration: " \
                           f"\n  Source Image: {self.source_image} " \
                           f"\n  Docker Image: {self.docker_image} " \
                           f"\n  Machine Type: {self.machine_type} " \
                           f"\n  Filesytsem Size: {self.filesystem_size} " \
                           f"\n  N-GPU Type: {self.ngpus} {self.gpu_type}" \
                           f"\n  Zone: {self.zone} " \
                           )
        self.cluster._log("Creating scheduler instance")
        self.internal_ip, self.external_ip = await self.create_vm()
        self.address = f"tcp://{self.external_ip}:8786"
        await self.wait_for_scheduler()

        # need to reserve internal IP for workers
        # gcp docker containers can't see resolve ip address
        self.cluster.scheduler_internal_ip = self.internal_ip
        self.cluster.scheduler_external_ip = self.external_ip


class GCPWorker(GCPInstance, WorkerMixin):
    """Worker running in an GCP instance."""

    def __init__(self, scheduler, *args, worker_command=None, worker_extra_args=None, **kwargs):
        super().__init__(*args, **kwargs)

        self.worker_extra_args = self.config.get('worker_extra_args', [])
        worker_command = ' '.join([worker_command] + self.worker_extra_args)
        self.init_worker(scheduler, *args, worker_command=worker_command, **kwargs)

    async def start(self):
        await super().start()
        await self.start_worker()

    def init_worker(self, scheduler: str, *args, worker_command=None, **kwargs):
        self.scheduler = scheduler
        self.worker_command = worker_command

        self.name = f"dask-worker-{str(uuid.uuid4())[:8]}"
        self.command = (
            f"{self.worker_command} {self.cluster.scheduler_internal_ip}:8786"
        )

        self.cluster._log(f"Starting worker: {self.name} with command: {self.command}")

    async def start_worker(self):
        self.cluster._log("Creating worker instance")
        self.internal_ip, self.external_ip = await self.create_vm()
        self.address = self.external_ip


class GCPCluster(VMCluster):
    """Cluster running on GCP Instances."""

    def __init__(
        self,
        name="dask-gcp-example",
        zone=None,
        machine_type=None,
        projectid=None,
        source_image=None,
        docker_image=None,
        ngpus=None,
        gpu_type=None,
        worker_command="dask-cuda-worker",
        worker_extra_args=None,
        auto_shutdown=True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        try:
            self.compute = googleapiclient.discovery.build("compute", "v1")
        except DefaultCredentialsError as e:
            raise Exception(
                "GCP Credentials have not been provided.  Please set the following environment variable:\n export GOOGLE_APPLICATION_CREDENTIALS=<Path-To-GCP-JSON-Credentials> "
            )

        self.name = name
        self.config = dask.config.get("cloudprovider.gcp", {})
        self.auto_shutdown = auto_shutdown
        self.scheduler_class = GCPScheduler
        self.worker_class = GCPWorker
        self.options = {
            "name": self.name,
            "cluster": self,
            "config": self.config,
            "projectid": projectid,
            "source_image": source_image,
            "docker_image": docker_image,
            "zone": zone,
            "machine_type": machine_type,
            "ngpus": ngpus,
            "gpu_type": gpu_type,
        }
        self.scheduler_options = {**self.options}
        self.worker_options = {"worker_command": worker_command, "worker_extra_args": worker_extra_args, **self.options}

# Note: if you have trouble connecting make sure firewall rules in GCP are stetup for 8787,8786,22