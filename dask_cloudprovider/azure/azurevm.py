import base64
import uuid

import dask
from dask_cloudprovider.generic.vmcluster import (
    VMCluster,
    VMInterface,
    SchedulerMixin,
    WorkerMixin,
)
from dask_cloudprovider.exceptions import ConfigError

try:
    from azure.common.credentials import get_azure_cli_credentials
    from azure.mgmt.network import NetworkManagementClient
    from azure.mgmt.compute import ComputeManagementClient
except ImportError as e:
    msg = (
        "Dask Cloud Provider Azure requirements are not installed.\n\n"
        "Please either conda or pip install as follows:\n\n"
        "  conda install dask-cloudprovider                             # either conda install\n"
        '  python -m pip install "dask-cloudprovider[azure]" --upgrade  # or python -m pip install'
    )
    raise ImportError(msg) from e


class AzureVM(VMInterface):
    def __init__(
        self,
        cluster: str,
        config,
        *args,
        location: str = None,
        vnet: str = None,
        public_ingress: bool = None,
        security_group: str = None,
        env_vars: dict = {},
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.vm = None
        self.cluster = cluster
        self.config = config
        self.location = location
        self.gpu_instance = False  # TODO do not hardcode
        self.bootstrap = True  # TODO do not hardcode
        self.admin_username = "dask"
        self.admin_password = str(uuid.uuid4())[:32]
        self.security_group = security_group
        self.nic_name = f"dask-cloudprovider-nic-{str(uuid.uuid4())[:8]}"
        self.public_ingress = public_ingress
        self.public_ip = None
        self.env_vars = env_vars

    async def create_vm(self):
        [subnet_info, *_] = self.cluster.network_client.subnets.list(
            self.cluster.resource_group, self.cluster.vnet
        )

        nic_parameters = {
            "location": self.location,
            "ip_configurations": [
                {
                    "name": self.nic_name,
                    "subnet": {"id": subnet_info.id},
                }
            ],
            "networkSecurityGroup": {
                "id": self.cluster.network_client.network_security_groups.get(
                    self.cluster.resource_group, self.security_group
                ).id,
                "location": self.location,
            },
            "tags": self.cluster.get_tags(),
        }
        if self.public_ingress:
            self.public_ip = (
                self.cluster.network_client.public_ip_addresses.begin_create_or_update(
                    self.cluster.resource_group,
                    self.nic_name,
                    {
                        "location": self.location,
                        "sku": {"name": "Standard"},
                        "public_ip_allocation_method": "Static",
                        "public_ip_address_version": "IPV4",
                        "tags": self.cluster.get_tags(),
                    },
                ).result()
            )
            nic_parameters["ip_configurations"][0]["public_ip_address"] = {
                "id": self.public_ip.id
            }
            self.cluster._log("Assigned public IP")
        self.nic = (
            self.cluster.network_client.network_interfaces.begin_create_or_update(
                self.cluster.resource_group,
                self.nic_name,
                nic_parameters,
            ).result()
        )
        self.cluster._log("Network interface ready")

        cloud_init = (
            base64.b64encode(
                self.cluster.render_cloud_init(
                    image=self.docker_image,
                    command=self.command,
                    gpu_instance=self.gpu_instance,
                    bootstrap=self.bootstrap,
                    auto_shutdown=self.cluster.auto_shutdown,
                    env_vars=self.env_vars,
                ).encode("ascii")
            )
            .decode("utf-8")
            .replace("\n", "")
        )

        if len(cloud_init) > 87380:
            raise RuntimeError("Cloud init is too long")

        vm_parameters = {
            "location": self.location,
            "os_profile": {
                "computer_name": self.name,
                "admin_username": self.admin_username,
                "admin_password": self.admin_password,
                "custom_data": cloud_init,
            },
            "hardware_profile": {
                "vm_size": "Standard_DS1_v2"  # TODO Make configurable
            },
            "storage_profile": {
                "image_reference": {  # TODO Make configurable
                    "publisher": "Canonical",
                    "offer": "UbuntuServer",
                    "sku": "16.04.0-LTS",
                    "version": "latest",
                },
            },
            "network_profile": {
                "network_interfaces": [
                    {
                        "id": self.nic.id,
                    }
                ]
            },
            "tags": self.cluster.get_tags(),
        }
        self.cluster._log("Creating VM")
        async_vm_creation = (
            self.cluster.compute_client.virtual_machines.begin_create_or_update(
                self.cluster.resource_group, self.name, vm_parameters
            )
        )
        async_vm_creation.wait()
        self.vm = self.cluster.compute_client.virtual_machines.get(
            self.cluster.resource_group, self.name
        )
        self.nic = self.cluster.network_client.network_interfaces.get(
            self.cluster.resource_group, self.nic.name
        )
        self.cluster._log(f"Created VM {self.name}")
        if self.public_ingress:
            return self.public_ip.ip_address
        return self.nic.ip_configurations[0].private_ip_address

    async def destroy_vm(self):
        self.cluster.compute_client.virtual_machines.begin_delete(
            self.cluster.resource_group, self.name
        ).wait()
        self.cluster._log(f"Terminated VM {self.name}")
        for disk in self.cluster.compute_client.disks.list_by_resource_group(
            self.cluster.resource_group
        ):
            if self.name in disk.name:
                self.cluster.compute_client.disks.begin_delete(
                    self.cluster.resource_group, disk.name
                )
        self.cluster._log(f"Removed disks for VM {self.name}")
        self.cluster.network_client.network_interfaces.begin_delete(
            self.cluster.resource_group, self.nic.name
        ).wait()
        self.cluster._log("Deleted network interface")
        if self.public_ingress:
            self.cluster.network_client.public_ip_addresses.begin_delete(
                self.cluster.resource_group, self.public_ip.name
            ).wait()
            self.cluster._log("Unassigned public IP")


class AzureVMScheduler(SchedulerMixin, AzureVM):
    """Scheduler running on an Azure VM."""


class AzureVMWorker(WorkerMixin, AzureVM):
    """Worker running on an AzureVM."""


class AzureVMCluster(VMCluster):
    """Cluster running on Azure Virtual machines.

    This cluster manager constructs a Dask cluster running on Azure Virtual Machines.

    When configuring your cluster you may find it useful to install the ``az`` tool for querying the
    Azure API for available options.

    https://docs.microsoft.com/en-us/cli/azure/install-azure-cli

    Parameters
    ----------
    location: str
        The Azure location to launch you cluster in. List available locations with ``az account list-locations``.
    resource_group: str
        The resource group to create components in. List your resource groups with ``az group list``.
    vnet: str
        The vnet to attach VM network interfaces to. List your vnets with ``az network vnet list``.
    security_group: str
        The security group to apply to your VMs.
        This must allow ports 8786-8787 from wherever you are running this from.
        List your security greoups with ``az network nsg list``.
    public_ingress: bool
        Assign a public IP address to the scheduler. Default ``True``.
    worker_module: str
        The Dask worker module to start on worker VMs.
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
    docker_image: string (optional)
        The Docker image to run on all instances.

        This image must have a valid Python environment and have ``dask`` installed in order for the
        ``dask-scheduler`` and ``dask-worker`` commands to be available. It is recommended the Python
        environment matches your local environment where ``AzureVMCluster`` is being created from.

        For GPU instance types the Docker image much have NVIDIA drivers and ``dask-cuda`` installed.

        By default the ``daskdev/dask:latest`` image will be used.
    silence_logs: bool
        Whether or not we should silence logging when setting up the cluster.
    asynchronous: bool
        If this is intended to be used directly within an event loop with
        async/await
    security : Security or bool, optional
        Configures communication security in this cluster. Can be a security
        object, or True. If True, temporary self-signed credentials will
        be created automatically.

    Examples
    --------

    TODO Examples

    """

    def __init__(
        self,
        location: str = None,
        resource_group: str = None,
        vnet: str = None,
        security_group: str = None,
        public_ingress: bool = None,
        **kwargs,
    ):
        self.config = dask.config.get("cloudprovider.azure", {})
        self.scheduler_class = AzureVMScheduler
        self.worker_class = AzureVMWorker
        self.resource_group = (
            resource_group
            if resource_group is not None
            else self.config.get("azurevm").get("resource_group")
        )
        if self.resource_group is None:
            raise ConfigError("You must configure a resource_group")
        self.public_ingress = (
            public_ingress
            if public_ingress is not None
            else self.config.get("azurevm").get("public_ingress")
        )
        self.credentials, self.subscription_id = get_azure_cli_credentials()
        self.compute_client = ComputeManagementClient(
            self.credentials, self.subscription_id
        )
        self.network_client = NetworkManagementClient(
            self.credentials, self.subscription_id
        )
        self.vnet = vnet if vnet is not None else self.config.get("azurevm").get("vnet")
        if self.vnet is None:
            raise ConfigError("You must configure a vnet")
        self.security_group = (
            security_group
            if security_group is not None
            else self.config.get("azurevm").get("security_group")
        )
        if self.security_group is None:
            raise ConfigError(
                "You must configure a security group which allows traffic on 8786 and 8787"
            )

        self.options = {
            "cluster": self,
            "config": self.config,
            "security_group": self.security_group,
            "location": location
            if location is not None
            else self.config.get("location"),
        }
        self.scheduler_options = {"public_ingress": self.public_ingress, **self.options}
        self.worker_options = {**self.options}
        super().__init__(**kwargs)