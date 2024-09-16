import base64
import json
import uuid
from typing import Optional

import dask
from dask_cloudprovider.config import ClusterConfig
from dask_cloudprovider.generic.vmcluster import (
    VMCluster,
    VMInterface,
    SchedulerMixin,
    WorkerMixin,
)
from dask_cloudprovider.exceptions import ConfigError
from dask_cloudprovider.azure.utils import _get_default_subscription

try:
    from azure.mgmt.network import NetworkManagementClient
    from azure.mgmt.compute import ComputeManagementClient
    from azure.identity import DefaultAzureCredential
except ImportError as e:
    msg = (
        "Dask Cloud Provider Azure requirements are not installed.\n\n"
        "Please either conda or pip install as follows:\n\n"
        "  conda install -c conda-forge dask-cloudprovider       # either conda install\n"
        '  pip install "dask-cloudprovider[azure]" --upgrade       # or python -m pip install'
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
        subnet: str = None,
        public_ingress: bool = None,
        security_group: str = None,
        vm_size: str = None,
        vm_image: dict = {},
        disk_size: int = None,
        gpu_instance: bool = None,
        docker_image: str = None,
        env_vars: dict = {},
        bootstrap: bool = None,
        extra_bootstrap=None,
        auto_shutdown: bool = None,
        marketplace_plan: dict = {},
        extra_vm_options: Optional[dict] = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.vm = None
        self.cluster = cluster
        self.config = config
        self.location = location
        self.gpu_instance = gpu_instance
        self.bootstrap = bootstrap
        self.extra_bootstrap = extra_bootstrap
        self.admin_username = "dask"
        self.admin_password = str(uuid.uuid4())[:32]
        self.vnet = vnet
        self.subnet = subnet
        self.security_group = security_group
        self.nic_name = f"dask-cloudprovider-nic-{str(uuid.uuid4())[:8]}"
        self.docker_image = docker_image
        self.public_ingress = public_ingress
        self.public_ip = None
        self.vm_size = vm_size
        self.vm_image = vm_image
        self.disk_size = disk_size
        self.auto_shutdown = auto_shutdown
        self.env_vars = env_vars
        self.marketplace_plan = marketplace_plan
        self.extra_vm_options = extra_vm_options or {}

    async def create_vm(self):
        if not self.subnet:
            [subnet_info, *_] = await self.cluster.call_async(
                self.cluster.network_client.subnets.list,
                self.cluster.resource_group,
                self.vnet,
            )
            self.subnet = subnet_info.id

        nic_parameters = {
            "location": self.location,
            "ip_configurations": [
                {
                    "name": self.nic_name,
                    "subnet": {"id": self.subnet},
                }
            ],
            "networkSecurityGroup": {
                "id": (
                    await self.cluster.call_async(
                        self.cluster.network_client.network_security_groups.get,
                        self.cluster.resource_group,
                        self.security_group,
                    )
                ).id,
                "location": self.location,
            },
            "tags": self.cluster.get_tags(),
        }
        if self.public_ingress:
            self.public_ip = await self.cluster.call_async(
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
                ).result
            )
            nic_parameters["ip_configurations"][0]["public_ip_address"] = {
                "id": self.public_ip.id
            }
            self.cluster._log("Assigned public IP")
        self.nic = await self.cluster.call_async(
            self.cluster.network_client.network_interfaces.begin_create_or_update(
                self.cluster.resource_group,
                self.nic_name,
                nic_parameters,
            ).result
        )
        self.cluster._log("Network interface ready")

        cloud_init = (
            base64.b64encode(
                self.cluster.render_process_cloud_init(self).encode("ascii")
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
            "hardware_profile": {"vm_size": self.vm_size},
            "storage_profile": {
                "image_reference": self.vm_image,
                "os_disk": {
                    "disk_size_gb": self.disk_size,
                    "create_option": "FromImage",
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

        if self.marketplace_plan:
            # Ref: https://docs.microsoft.com/en-us/rest/api/compute/virtual-machines/create-or-update#create-a-vm-with-a-marketplace-image-plan. # noqa
            # Creating a marketplace VM with a plan will override default vm_image values.
            vm_parameters["plan"] = self.marketplace_plan
            vm_parameters["storage_profile"]["image_reference"][
                "sku"
            ] = self.marketplace_plan["name"]
            vm_parameters["storage_profile"]["image_reference"][
                "publisher"
            ] = self.marketplace_plan["publisher"]
            vm_parameters["storage_profile"]["image_reference"][
                "offer"
            ] = self.marketplace_plan["product"]
            vm_parameters["storage_profile"]["image_reference"]["version"] = "latest"
            self.cluster._log("Using Marketplace VM image with a Plan")

        repeated = self.extra_vm_options.keys() & vm_parameters.keys()
        if repeated:
            raise TypeError(
                f"Parameters are passed in both 'extra_vm_options' and as regular parameters: {repeated}"
            )
        vm_parameters = {**self.extra_vm_options, **vm_parameters}

        self.cluster._log("Creating VM")
        if self.cluster.debug:
            self.cluster._log(
                f"with parameters\n{json.dumps(vm_parameters, sort_keys=True, indent=2)}"
            )
        await self.cluster.call_async(
            self.cluster.compute_client.virtual_machines.begin_create_or_update(
                self.cluster.resource_group, self.name, vm_parameters
            ).wait
        )
        self.vm = await self.cluster.call_async(
            self.cluster.compute_client.virtual_machines.get,
            self.cluster.resource_group,
            self.name,
        )
        self.nic = await self.cluster.call_async(
            self.cluster.network_client.network_interfaces.get,
            self.cluster.resource_group,
            self.nic.name,
        )
        self.cluster._log(f"Created VM {self.name}")

        private_ip_address = self.nic.ip_configurations[0].private_ip_address
        if self.public_ingress:
            return private_ip_address, self.public_ip.ip_address
        else:
            return private_ip_address, None

    async def destroy_vm(self):
        await self.cluster.call_async(
            self.cluster.compute_client.virtual_machines.begin_delete(
                self.cluster.resource_group, self.name
            ).wait
        )
        self.cluster._log(f"Terminated VM {self.name}")
        for disk in await self.cluster.call_async(
            self.cluster.compute_client.disks.list_by_resource_group,
            self.cluster.resource_group,
        ):
            if self.name in disk.name:
                await self.cluster.call_async(
                    self.cluster.compute_client.disks.begin_delete(
                        self.cluster.resource_group,
                        disk.name,
                    ).wait
                )
        self.cluster._log(f"Removed disks for VM {self.name}")
        await self.cluster.call_async(
            self.cluster.network_client.network_interfaces.begin_delete(
                self.cluster.resource_group, self.nic.name
            ).wait
        )
        self.cluster._log("Deleted network interface")
        if self.public_ingress:
            await self.cluster.call_async(
                self.cluster.network_client.public_ip_addresses.begin_delete(
                    self.cluster.resource_group, self.public_ip.name
                ).wait
            )
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
    subnet: str (optional)
        The vnet subnet to attach VM network interfaces to.
        If omitted it will automatically use the first subnet in your vnet.
    security_group: str
        The security group to apply to your VMs.
        This must allow ports 8786-8787 from wherever you are running this from.
        List your security groups with ``az network nsg list``.
    public_ingress: bool
        Assign a public IP address to the scheduler. Default ``True``.
    vm_size: str
        Azure VM size to use for scheduler and workers. Default ``Standard_DS1_v2``.
        List available VM sizes with ``az vm list-sizes --location <location>``.
    disk_size: int
        Specifies the size of the VM host OS disk in gigabytes.
        Default is ``50``. This value cannot be larger than ``1023``.
    scheduler_vm_size: str
        Azure VM size to use for scheduler. If not set will use the ``vm_size``.
    vm_image: dict
        By default all VMs will use the latest Ubuntu LTS release with the following configuration

        ``{"publisher": "Canonical", "offer": "UbuntuServer","sku": "18.04-LTS", "version": "latest"}``

        You can override any of these options by passing a dict with matching keys here.
        For example if you wish to try Ubuntu 19.04 you can pass ``{"sku": "19.04"}`` and the ``publisher``,
        ``offer`` and ``version`` will be used from the default.
    bootstrap: bool (optional)
        It is assumed that the ``VHD`` will not have Docker installed (or the NVIDIA drivers for GPU instances).
        If ``bootstrap`` is ``True`` these dependencies will be installed on instance start. If you are using
        a custom VHD which already has these dependencies set this to ``False.``
    auto_shutdown: bool (optional)
        Shutdown the VM if the Dask process exits. Default ``True``.
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
    docker_args: string (optional)
        Extra command line arguments to pass to Docker.
    extra_bootstrap: list[str] (optional)
        Extra commands to be run during the bootstrap phase.
    silence_logs: bool
        Whether or not we should silence logging when setting up the cluster.
    asynchronous: bool
        If this is intended to be used directly within an event loop with
        async/await
    security : Security or bool, optional
        Configures communication security in this cluster. Can be a security
        object, or True. If True, temporary self-signed credentials will
        be created automatically. Default is ``True``.
    debug: bool, optional
        More information will be printed when constructing clusters to enable debugging.
    marketplace_plan: dict (optional)
        Plan information dict necessary for creating a virtual machine from Azure Marketplace image or
        a custom image sourced from a Marketplace image with a plan. Default is {}.

        All three fields "name", "publisher", "product" must be passed in the dictionary if set. For e.g.

        ``{"name": "ngc-base-version-21-02-2", "publisher": "nvidia","product": "ngc_azure_17_11"}``
    subscription_id: str (optional)
        The ID of the Azure Subscription to create the virtual machines in. If not specified, then
        dask-cloudprovider will attempt to use the configured default for the Azure CLI. List your
        subscriptions with ``az account list``.
    extra_vm_options: dict[str, Any]:
        Additional arguments to provide to Azure's ``VirtualMachinesOperations.begin_create_or_update``
        when creating the scheduler and worker VMs.

    Examples
    --------

    **Minimal example**

    Create the cluster

    >>> from dask_cloudprovider.azure import AzureVMCluster
    >>> cluster = AzureVMCluster(resource_group="<resource group>",
    ...                          vnet="<vnet>",
    ...                          security_group="<security group>",
    ...                          n_workers=1)
    Creating scheduler instance
    Assigned public IP
    Network interface ready
    Creating VM
    Created VM dask-5648cc8b-scheduler
    Waiting for scheduler to run
    Scheduler is running
    Creating worker instance
    Network interface ready
    Creating VM
    Created VM dask-5648cc8b-worker-e1ebfc0e

    Connect a client.

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    Do some work.

    >>> import dask.array as da
    >>> arr = da.random.random((1000, 1000), chunks=(100, 100))
    >>> arr.mean().compute()
    0.5004117488368686

    Close the cluster.

    >>> client.close()
    >>> cluster.close()
    Terminated VM dask-5648cc8b-worker-e1ebfc0e
    Removed disks for VM dask-5648cc8b-worker-e1ebfc0e
    Deleted network interface
    Terminated VM dask-5648cc8b-scheduler
    Removed disks for VM dask-5648cc8b-scheduler
    Deleted network interface
    Unassigned public IP

    You can also do this all in one go with context managers to ensure the cluster is
    created and cleaned up.

    >>> with AzureVMCluster(resource_group="<resource group>",
    ...                     vnet="<vnet>",
    ...                     security_group="<security group>",
    ...                     n_workers=1) as cluster:
    ...     with Client(cluster) as client:
    ...             print(da.random.random((1000, 1000), chunks=(100, 100)).mean().compute())
    Creating scheduler instance
    Assigned public IP
    Network interface ready
    Creating VM
    Created VM dask-1e6dac4e-scheduler
    Waiting for scheduler to run
    Scheduler is running
    Creating worker instance
    Network interface ready
    Creating VM
    Created VM dask-1e6dac4e-worker-c7c4ca23
    0.4996427609642539
    Terminated VM dask-1e6dac4e-worker-c7c4ca23
    Removed disks for VM dask-1e6dac4e-worker-c7c4ca23
    Deleted network interface
    Terminated VM dask-1e6dac4e-scheduler
    Removed disks for VM dask-1e6dac4e-scheduler
    Deleted network interface
    Unassigned public IP


    **RAPIDS example**

    You can also use ``AzureVMCluster`` to run a GPU enabled cluster and
    leverage the `RAPIDS <https://rapids.ai/>`_ accelerated libraries.

    >>> cluster = AzureVMCluster(resource_group="<resource group>",
    ...                          vnet="<vnet>",
    ...                          security_group="<security group>",
    ...                          n_workers=1,
    ...                          vm_size="Standard_NC12s_v3",  # Or any NVIDIA GPU enabled size
    ...                          docker_image="rapidsai/rapidsai:cuda11.0-runtime-ubuntu18.04-py3.9",
    ...                          worker_class="dask_cuda.CUDAWorker")
    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    Run some GPU code.

    >>> def get_gpu_model():
    ...     import pynvml
    ...     pynvml.nvmlInit()
    ...     return pynvml.nvmlDeviceGetName(pynvml.nvmlDeviceGetHandleByIndex(0))

    >>> client.submit(get_gpu_model).result()
    b'Tesla V100-PCIE-16GB'

    Close the cluster.

    >>> client.close()
    >>> cluster.close()

    """

    def __init__(
        self,
        location: str = None,
        resource_group: str = None,
        vnet: str = None,
        subnet: str = None,
        security_group: str = None,
        public_ingress: bool = None,
        vm_size: str = None,
        scheduler_vm_size: str = None,
        vm_image: dict = {},
        disk_size: int = None,
        bootstrap: bool = None,
        auto_shutdown: bool = None,
        docker_image=None,
        debug: bool = False,
        marketplace_plan: dict = {},
        subscription_id: Optional[str] = None,
        extra_vm_options: Optional[dict] = None,
        **kwargs,
    ):
        self.config = ClusterConfig(dask.config.get("cloudprovider.azure", {}))
        self.scheduler_class = AzureVMScheduler
        self.worker_class = AzureVMWorker
        self.location = self.config.get("location", override_with=location)
        if self.location is None:
            raise ConfigError("You must configure a location")
        self.resource_group = self.config.get(
            "resource_group", override_with=resource_group
        )
        if self.resource_group is None:
            raise ConfigError("You must configure a resource_group")
        self.public_ingress = self.config.get(
            "azurevm.public_ingress", override_with=public_ingress
        )
        # We prefer code > Dask config > Azure CLI
        self.subscription_id = self.config.get(
            "subscription_id", override_with=subscription_id
        )
        if self.subscription_id is None:
            self.subscription_id = _get_default_subscription()

        self.credentials = DefaultAzureCredential()
        self.compute_client = ComputeManagementClient(
            self.credentials, self.subscription_id
        )
        self.network_client = NetworkManagementClient(
            self.credentials, self.subscription_id
        )
        self.vnet = self.config.get("azurevm.vnet", override_with=vnet)
        if self.vnet is None:
            raise ConfigError("You must configure a vnet")
        self.subnet = self.config.get("azurevm.subnet", override_with=subnet)
        self.security_group = self.config.get(
            "azurevm.security_group", override_with=security_group
        )
        self.docker_image = self.config.get(
            "azurevm.docker_image", override_with=docker_image
        )
        if self.security_group is None:
            raise ConfigError(
                "You must configure a security group which allows traffic on 8786 and 8787"
            )
        self.vm_size = self.config.get("azurevm.vm_size", override_with=vm_size)
        self.disk_size = self.config.get("azurevm.disk_size", override_with=disk_size)
        if self.disk_size > 1023:
            raise ValueError(
                "VM OS disk canot be larger than 1023. Please change the ``disk_size`` config option."
            )
        self.scheduler_vm_size = self.config.get(
            "azurevm.scheduler_vm_size", override_with=scheduler_vm_size
        )
        if self.scheduler_vm_size is None:
            self.scheduler_vm_size = self.vm_size
        self.gpu_instance = (
            "_NC" in self.vm_size.upper() or "_ND" in self.vm_size.upper()
        )
        self.vm_image = self.config.get("azurevm.vm_image")
        for key in vm_image:
            self.vm_image[key] = vm_image[key]
        self.bootstrap = self.config.get("azurevm.bootstrap", override_with=bootstrap)
        self.auto_shutdown = self.config.get(
            "azurevm.auto_shutdown", override_with=auto_shutdown
        )
        self.debug = debug
        self.marketplace_plan = marketplace_plan or self.config.get(
            "azurevm.marketplace_plan"
        )
        if self.marketplace_plan:
            # Check that self.marketplace_plan contains the right options with values
            if not all(
                self.marketplace_plan.get(item, "") != ""
                for item in ["name", "publisher", "product"]
            ):
                raise ConfigError(
                    """To create a virtual machine from Marketplace image or a custom image sourced
                from a Marketplace image with a plan, all 3 fields 'name', 'publisher' and 'product' must be passed."""
                )
        self.extra_vm_options = extra_vm_options or self.config.get(
            "azurevm.extra_vm_options"
        )
        self.options = {
            "cluster": self,
            "config": self.config,
            "subnet": self.subnet,
            "vnet": self.vnet,
            "security_group": self.security_group,
            "location": self.location,
            "vm_image": self.vm_image,
            "disk_size": self.disk_size,
            "gpu_instance": self.gpu_instance,
            "bootstrap": self.bootstrap,
            "auto_shutdown": self.auto_shutdown,
            "docker_image": self.docker_image,
            "marketplace_plan": self.marketplace_plan,
            "extra_vm_options": self.extra_vm_options,
        }
        self.scheduler_options = {
            "vm_size": self.scheduler_vm_size,
            "public_ingress": self.public_ingress,
            **self.options,
        }
        self.worker_options = {"vm_size": self.vm_size, **self.options}
        super().__init__(debug=debug, **kwargs)
