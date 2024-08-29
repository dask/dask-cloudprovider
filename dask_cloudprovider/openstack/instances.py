import asyncio
import dask

from dask_cloudprovider.generic.vmcluster import (
    VMCluster,
    VMInterface,
    SchedulerMixin,
    WorkerMixin,
)

from distributed.core import Status

try:
    from openstack import connection
except ImportError as e:
    msg = (
        "Dask Cloud Provider OpenStack requirements are not installed.\n\n"
        "Please pip install as follows:\n\n"
        '  pip install "openstacksdk" '
    )
    raise ImportError(msg) from e


class OpenStackInstance(VMInterface):
    def __init__(
        self,
        cluster,
        config,
        region: str = None,
        size: str = None,
        image: str = None,
        docker_image: str = None,
        env_vars: str = None,
        extra_bootstrap: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.instance = None
        self.cluster = cluster
        self.config = config
        self.region = region
        self.size = size
        self.image = image
        self.env_vars = env_vars
        self.bootstrap = True
        self.docker_image = docker_image
        self.extra_bootstrap = extra_bootstrap

    async def create_vm(self):
        conn = connection.Connection(
            region_name=self.region,
            auth_url=self.config["auth_url"],
            application_credential_id=self.config["application_credential_id"],
            application_credential_secret=self.config["application_credential_secret"],
            compute_api_version="2",
            identity_interface="public",
            auth_type="v3applicationcredential",
        )

        self.instance = conn.create_server(
            name=self.name,
            image=self.image,
            flavor=self.size,  # Changed 'flavor_id' to 'flavor'
            key_name=self.config["keypair_name"],  # Add the keypair name here
            nics=[
                {"net-id": self.config["network_id"]}
            ],  # Changed from 'networks' to 'nics'
            userdata=self.cluster.render_process_cloud_init(self),
            security_groups=[self.config["security_group"]],
        )

        # Wait for the instance to be up and running
        while self.instance.status.lower() != "active":
            await asyncio.sleep(0.1)
            self.instance = conn.compute.get_server(self.instance.id)

        # Retrieve the internal IP address
        self.internal_ip = await self.get_internal_ip(conn)

        # Check if a floating IP should be created and assigned
        if self.config.get("create_floating_ip", False):
            self.external_ip = await self.create_and_assign_floating_ip(conn)
        else:
            self.external_ip = await self.get_external_ip(conn)

        self.cluster._log(
            f"{self.name}\n\tInternal IP: {self.internal_ip}\n\tExternal IP: "
            f"{self.external_ip if self.external_ip else 'None'}"
        )
        return self.internal_ip, self.external_ip

    async def get_internal_ip(self, conn):
        """Fetch the internal IP address from the OpenStack instance."""
        instance = conn.compute.get_server(self.instance.id)
        for network in instance.addresses.values():
            for addr in network:
                if addr["OS-EXT-IPS:type"] == "fixed":
                    return addr["addr"]
        return None

    async def get_external_ip(self, conn):
        """Fetch the external IP address from the OpenStack instance, if it exists."""
        instance = conn.compute.get_server(self.instance.id)
        for network in instance.addresses.values():
            for addr in network:
                if addr["OS-EXT-IPS:type"] == "floating":
                    return addr["addr"]
        return None

    async def create_and_assign_floating_ip(self, conn):
        """Create and assign a floating IP to the instance."""
        try:
            # Create a floating IP
            floating_ip = await self.cluster.call_async(
                conn.network.create_ip,
                floating_network_id=self.config["external_network_id"],
            )

            # Assign the floating IP to the server
            await self.cluster.call_async(
                conn.compute.add_floating_ip_to_server,
                server=self.instance.id,
                address=floating_ip.floating_ip_address,
            )

            return floating_ip.floating_ip_address
        except Exception as e:
            self.cluster._log(f"Failed to create or assign floating IP: {str(e)}")
            return None

    async def destroy_vm(self):
        conn = connection.Connection(
            region_name=self.region,
            auth_url=self.config["auth_url"],
            application_credential_id=self.config["application_credential_id"],
            application_credential_secret=self.config["application_credential_secret"],
            compute_api_version="2",
            identity_interface="public",
            auth_type="v3applicationcredential",
        )

        # Handle floating IP disassociation and deletion if applicable
        if self.config.get(
            "create_floating_ip", False
        ):  # Checks if floating IPs were configured to be created
            try:
                # Retrieve all floating IPs associated with the instance
                floating_ips = conn.network.ips(port_id=self.instance.id)
                for ip in floating_ips:
                    # Disassociate and delete the floating IP
                    conn.network.update_ip(ip, port_id=None)
                    conn.network.delete_ip(ip.id)
                    self.cluster._log(f"Deleted floating IP {ip.floating_ip_address}")
            except Exception as e:
                self.cluster._log(
                    f"Failed to clean up floating IPs for instance {self.name}: {str(e)}"
                )
                return  # Exit if floating IP cleanup fails

        # Then, attempt to delete the instance
        try:
            instance = conn.compute.get_server(self.instance.id)
            if instance:
                await self.cluster.call_async(conn.compute.delete_server, instance.id)
                self.cluster._log(f"Terminated instance {self.name}")
            else:
                self.cluster._log(f"Instance {self.name} not found or already deleted.")
        except Exception as e:
            self.cluster._log(f"Failed to terminate instance {self.name}: {str(e)}")

    async def start_vm(self):
        # Code to start the instance
        pass  # Placeholder to ensure correct indentation

    async def stop_vm(self):
        # Code to stop the instance
        pass  # Placeholder to ensure correct indentation


class OpenStackScheduler(SchedulerMixin, OpenStackInstance):
    """Scheduler running on an OpenStack Instance."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def start(self):
        await self.start_scheduler()
        self.status = Status.running

    async def start_scheduler(self):
        self.cluster._log(
            f"Launching cluster with the following configuration: "
            f"\n  OS Image: {self.image} "
            f"\n  Flavor: {self.size} "
            f"\n  Docker Image: {self.docker_image} "
            f"\n  Security Group: {self.config['security_group']} "
        )
        self.cluster._log("Creating scheduler instance")
        self.internal_ip, self.external_ip = await self.create_vm()

        # Choose the IP based on the access type configuration
        if self.config.get("create_floating_ip", True):
            # If public access is required and a floating IP is created
            self.address = f"{self.cluster.protocol}://{self.external_ip}:{self.port}"
        else:
            # Use internal IP if no external access is configured
            self.address = f"{self.cluster.protocol}://{self.internal_ip}:{self.port}"

        await self.wait_for_scheduler()

        # Storing IPs for cluster-wide use, if necessary
        self.cluster.scheduler_internal_ip = self.internal_ip
        self.cluster.scheduler_external_ip = self.external_ip
        self.cluster.scheduler_port = self.port


class OpenStackWorker(WorkerMixin, OpenStackInstance):
    """Worker running on a OpenStack Instance."""


class OpenStackCluster(VMCluster):
    """Cluster running on Openstack VM Instances

    This cluster manager constructs a Dask cluster running on generic Openstack cloud

    When configuring your cluster you may find it useful to install the 'python-openstackclient'
    client for querying the Openstack APIs for available options.

    https://github.com/openstack/python-openstackclient

    Parameters
    ----------

    region: str
        The name of the region where resources will be allocated in OpenStack.
        Typically set to 'default' unless specified in your cloud configuration.

        List available regions using: `openstack region list`.
    auth_url: str
        The authentication URL for the OpenStack Identity service (Keystone).
        Example: https://cloud.example.com:5000
    application_credential_id: str
         The application credential id created in OpenStack.

         Create application credentials using: openstack application credential create
    application_credential_secret: str
        The secret associated with the application credential ID for authentication.
    auth_type: str
        The type of authentication used, typically "v3applicationcredential" for
        using OpenStack application credentials.
    network_id: str
        The unique identifier for the internal/private network in OpenStack where the cluster
        VMs will be connected.

        List available networks using: `openstack network list`
    image: str
        The OS image name or id to use for the VM. Dask Cloudprovider will boostrap Ubuntu
        based images automatically. Other images require Docker and for GPUs
        the NVIDIA Drivers and NVIDIA Docker.

        List available images using: `openstack image list`
    keypair_name: str
        The name of the SSH keypair used for instance access. Ensure you have created a keypair
        or use an existing one.

        List available keypairs using: `openstack keypair list`
    security_group: str
        The security group name that defines firewall rules for instances.

        The default is `default`. Please ensure the follwing accesses are configured:
            - egress 0.0.0.0/0 on all ports for downloading docker images and general data access
            - ingress <internal-cidr>/8 on all ports for internal communication of workers
            - ingress 0.0.0.0/0 on 8786-8787 for external accessibility of the dashboard/scheduler
            - (optional) ingress 0.0.0.0./0 on 22 for ssh access

        List available security groups using: `openstack security group list`
    create_floating_ip: bool
        Specifies whether to assign a floating IP to each instance, enabling external
        access. Set to `True` if external connectivity is needed.
    external_network_id: str
        The ID of the external network used for assigning floating IPs.

        List available external networks using: `openstack network list --external`
    n_workers: int (optional)
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
    docker_image: string (optional)
        The Docker image to run on all instances.

        This image must have a valid Python environment and have ``dask`` installed in order for the
        ``dask-scheduler`` and ``dask-worker`` commands to be available. It is recommended the Python
        environment matches your local environment where ``OpenStackCluster`` is being created from.

        For GPU instance types the Docker image much have NVIDIA drivers and ``dask-cuda`` installed.

        By default the ``daskdev/dask:latest`` image will be used.

    Example
    --------

    >>> from dask_cloudprovider.openstack import OpenStackCluster
    >>> cluster = OpenStackCluster(n_workers=1)
    Launching cluster with the following configuration:
        OS Image: ubuntu-22-04
        Flavor: 4vcpu-8gbram-50gbdisk
        Docker Image: daskdev/dask:latest
        Security Group: all-open
    Creating scheduler instance
        dask-9b85a5f8-scheduler
                Internal IP: 10.0.30.148
                External IP: None
    Waiting for scheduler to run at 10.0.30.148:8786
    Scheduler is running
    Creating worker instance

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    >>> import dask.array as da
    >>> arr = da.random.random((1000, 1000), chunks=(100, 100))
    >>> arr.mean().compute()

    >>> client.close()
    >>> cluster.close()
    Terminated instance dask-07280176-worker-319005a2
    Terminated instance dask-07280176-scheduler
    """

    def __init__(
        self,
        region: str = None,
        size: str = None,
        image: str = None,
        docker_image: str = None,
        debug: bool = False,
        bootstrap: bool = True,
        **kwargs,
    ):
        self.config = dask.config.get("cloudprovider.openstack", {})
        self.scheduler_class = OpenStackScheduler
        self.worker_class = OpenStackWorker
        self.debug = debug
        self.bootstrap = (
            bootstrap if bootstrap is not None else self.config.get("bootstrap")
        )
        self.options = {
            "cluster": self,
            "config": self.config,
            "region": region if region is not None else self.config.get("region"),
            "size": size if size is not None else self.config.get("size"),
            "image": image if image is not None else self.config.get("image"),
            "docker_image": docker_image or self.config.get("docker_image"),
        }
        self.scheduler_options = {**self.options}
        self.worker_options = {**self.options}

        if "extra_bootstrap" not in kwargs:
            kwargs["extra_bootstrap"] = self.config.get("extra_bootstrap")

        super().__init__(debug=debug, **kwargs)
