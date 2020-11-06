import asyncio

import dask
from dask_cloudprovider.generic.vmcluster import (
    VMCluster,
    VMInterface,
    SchedulerMixin,
    WorkerMixin,
)
from dask_cloudprovider.aws.helper import (
    get_latest_ami_id,
    get_default_vpc,
    get_vpc_subnets,
    get_security_group,
)

try:
    import aiobotocore
except ImportError as e:
    msg = (
        "Dask Cloud Provider AWS requirements are not installed.\n\n"
        "Please either conda or pip install as follows:\n\n"
        "  conda install dask-cloudprovider                           # either conda install\n"
        '  python -m pip install "dask-cloudprovider[aws]" --upgrade  # or python -m pip install'
    )
    raise ImportError(msg) from e


class EC2Instance(VMInterface):
    def __init__(
        self,
        cluster,
        config,
        *args,
        region=None,
        bootstrap=None,
        ami=None,
        docker_image=None,
        env_vars=None,
        instance_type=None,
        gpu_instance=None,
        vpc=None,
        subnet_id=None,
        security_groups=None,
        filesystem_size=None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.instance = None
        self.cluster = cluster
        self.config = config
        self.region = region
        self.bootstrap = bootstrap
        self.ami = ami
        self.docker_image = docker_image or self.config.get("docker_image")
        self.env_vars = env_vars
        self.instance_type = instance_type
        self.gpu_instance = gpu_instance
        self.vpc = vpc
        self.subnet_id = subnet_id
        self.security_groups = security_groups
        self.filesystem_size = filesystem_size

    async def create_vm(self):
        """

        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.run_instances
        """
        # TODO Enable Spot support

        async with self.cluster.boto_session.create_client(
            "ec2", region_name=self.region
        ) as client:
            self.vpc = self.vpc or await get_default_vpc(client)
            self.subnet_id = (
                self.subnet_id or (await get_vpc_subnets(client, self.vpc))[0]
            )
            self.security_groups = self.security_groups or [
                await get_security_group(client, self.vpc)
            ]
            self.ami = self.ami or await get_latest_ami_id(
                client,
                "ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-*",
                "099720109477",  # Canonical
            )

            response = await client.run_instances(
                BlockDeviceMappings=[
                    {
                        "DeviceName": "/dev/sda1",
                        "VirtualName": "sda1",
                        "Ebs": {
                            "DeleteOnTermination": True,
                            "VolumeSize": self.filesystem_size,
                            "VolumeType": "gp2",
                            "Encrypted": False,
                        },
                    }
                ],
                ImageId=self.ami,
                InstanceType=self.instance_type,
                MaxCount=1,
                MinCount=1,
                Monitoring={"Enabled": False},
                UserData=self.cluster.render_cloud_init(
                    image=self.docker_image,
                    command=self.command,
                    gpu_instance=self.gpu_instance,
                    bootstrap=self.bootstrap,
                    env_vars=self.env_vars,
                ),
                InstanceInitiatedShutdownBehavior="terminate",
                NetworkInterfaces=[
                    {
                        "AssociatePublicIpAddress": True,
                        "DeleteOnTermination": True,
                        "Description": "public",
                        "DeviceIndex": 0,
                        "Groups": self.security_groups,
                        "SubnetId": self.subnet_id,
                    }
                ],
            )
            [self.instance] = response["Instances"]
            await client.create_tags(
                Resources=[self.instance["InstanceId"]],
                Tags=[
                    {"Key": "Name", "Value": self.name},
                    {"Key": "Dask Cluster", "Value": self.cluster.uuid},
                ],
            )
            self.cluster._log(
                f"Created instance {self.instance['InstanceId']} as {self.name}"
            )

            while (
                "PublicIpAddress" not in self.instance
                or self.instance["PublicIpAddress"] is None
            ):
                await asyncio.sleep(0.1)  # TODO back off correctly
                response = await client.describe_instances(
                    InstanceIds=[self.instance["InstanceId"]], DryRun=False
                )
                [reservation] = response["Reservations"]
                [self.instance] = reservation["Instances"]
            return self.instance["PublicIpAddress"]

    async def destroy_vm(self):
        async with self.cluster.boto_session.create_client(
            "ec2", region_name=self.region
        ) as client:
            await client.terminate_instances(
                InstanceIds=[self.instance["InstanceId"]], DryRun=False
            )
            self.cluster._log(f"Terminated {self.name} ({self.instance['InstanceId']})")


class EC2Scheduler(SchedulerMixin, EC2Instance):
    """Scheduler running on an EC2 instance."""


class EC2Worker(WorkerMixin, EC2Instance):
    """Worker running on an EC2 instance."""


class EC2Cluster(VMCluster):
    """Deploy a Dask cluster using EC2.

    This creates a Dask scheduler and workers on EC2 instances.

    All instances will run a single configurable Docker container which should contain
    a valid Python environment with Dask and any other dependencies.

    All optional parameters can also be configured in a `cloudprovider.yaml` file
    in your Dask configuration directory or via environment variables.

    For example ``ami`` can be set via ``DASK_CLOUDPROVIDER__EC2__AMI``.

    See https://docs.dask.org/en/latest/configuration.html for more info.

    Parameters
    ----------
    region: string (optional)
        The region to start you clusters. By default this will be detected from your config.
    bootstrap: bool (optional)
        It is assumed that the ``ami`` will not have Docker installed (or the NVIDIA drivers for GPU instances).
        If ``bootstrap`` is ``True`` these dependencies will be installed on instance start. If you are using
        a custom AMI which already has these dependencies set this to ``False.``
    worker_command: string (optional)
        The command workers should run when starting. By default this will be ``"dask-worker"`` unless
        ``instance_type`` is a GPU instance in which case ``dask-cuda-worker`` will be used.
    ami: string (optional)
        The base OS AMI to use for scheduler and workers.

        This must be a Debian flavour distribution. By default this will be the latest official
        Ubuntu 20.04 LTS release from canonical.

        If the AMI does not include Docker it will be installed at runtime.
        If the instance_type is a GPU instance the NVIDIA drivers and Docker GPU runtime will be installed
        at runtime.
    instance_type: string (optional)
        A valid EC2 instance type. This will determine the resources available to your workers.

        See https://aws.amazon.com/ec2/instance-types/.

        By default will use ``t2.micro``.
    vpc: string (optional)
        The VPC ID in which to launch the instances.

        Will detect and use the default VPC if not specified.
    subnet_id: string (optional)
        The Subnet ID in which to launch the instances.

        Will use all subnets for the VPC if not specified.
    security_groups: List(string) (optional)
        The security group ID that will be attached to the workers.

        Must allow all traffic between instances in the security group and ports 8786 and 8787 between
        the scheduler instance and wherever you are calling ``EC2Cluster`` from.

        By default a Dask security group will be created with ports 8786 and 8787 exposed to the internet.
    filesystem_size: int (optional)
        The instance filesystem size in GB.

        Defaults to ``40``.
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
        environment matches your local environment where ``EC2Cluster`` is being created from.

        For GPU instance types the Docker image much have NVIDIA drivers and ``dask-cuda`` installed.

        By default the ``daskdev/dask:latest`` image will be used.
    env_vars: dict (optional)
        Environment variables to be passed to the worker.
    silence_logs: bool
        Whether or not we should silence logging when setting up the cluster.
    asynchronous: bool
        If this is intended to be used directly within an event loop with
        async/await
    security : Security or bool, optional
        Configures communication security in this cluster. Can be a security
        object, or True. If True, temporary self-signed credentials will
        be created automatically.

    Notes
    -----

    **Resources created**

    .. csv-table::
        :header: Resource, Name, Purpose, Cost

        EC2 Instance, dask-scheduler-{cluster uuid}, Dask Scheduler, "`EC2 Pricing
        <https://aws.amazon.com/ec2/pricing/>`_"
        EC2 Instance, dask-worker-{cluster uuid}-{worker uuid}, Dask Workers, "`EC2 Pricing
        <https://aws.amazon.com/ec2/pricing/>`_"

    **Manual cleanup**

    If for some reason the cluster manager is terminated without being able to perform cleanup
    the default behaviour of ``EC2Cluster`` is for the scheduler and workers to time out. This will
    result in the host VMs shutting down. This cluster manager also creates instances with the terminate on
    shutdown setting so all resources should be removed automatically.

    If for some reason you chose to override those settings and disable auto cleanup you can destroy resources with the
    following CLI command.

    .. code-block:: bash

        export CLUSTER_ID="cluster id printed during creation"
        aws ec2 describe-instances \\
            --filters "Name=tag:Dask Cluster,Values=${CLUSTER_ID}" \\
            --query "Reservations[*].Instances[*].[InstanceId]" \\
            --output text | xargs aws ec2 terminate-instances --instance-ids

    Examples
    --------

    Regular cluster.

    >>> cluster = EC2Cluster()
    >>> cluster.scale(5)

    RAPIDS Cluster.

    >>> cluster = EC2Cluster(ami="ami-0c7c7d78f752f8f17",  # Example Deep Learning AMI (Ubuntu 18.04)
                             docker_image="rapidsai/rapidsai:cuda10.1-runtime-ubuntu18.04",
                             instance_type="p3.2xlarge",
                             worker_module="dask_cuda.cli.dask_cuda_worker",
                             bootstrap=False,
                             filesystem_size=120)
    """

    def __init__(
        self,
        region=None,
        bootstrap=None,
        auto_shutdown=None,
        ami=None,
        instance_type=None,
        vpc=None,
        subnet_id=None,
        security_groups=None,
        filesystem_size=None,
        docker_image=None,
        **kwargs,
    ):
        self.boto_session = aiobotocore.get_session()
        self.config = dask.config.get("cloudprovider.ec2", {})
        self.scheduler_class = EC2Scheduler
        self.worker_class = EC2Worker
        self.region = region if region is not None else self.config.get("region")
        self.bootstrap = (
            bootstrap if bootstrap is not None else self.config.get("bootstrap")
        )
        self.auto_shutdown = (
            auto_shutdown
            if auto_shutdown is not None
            else self.config.get("auto_shutdown")
        )
        self.ami = ami if ami is not None else self.config.get("ami")
        self.instance_type = (
            instance_type
            if instance_type is not None
            else self.config.get("instance_type")
        )
        self.gpu_instance = self.instance_type.startswith(("p", "g"))
        self.vpc = vpc if vpc is not None else self.config.get("vpc")
        self.subnet_id = (
            subnet_id if subnet_id is None else self.config.get("subnet_id")
        )
        self.security_groups = (
            security_groups
            if security_groups is not None
            else self.config.get("security_groups")
        )
        self.filesystem_size = (
            filesystem_size
            if filesystem_size is not None
            else self.config.get("filesystem_size")
        )

        self.options = {
            "cluster": self,
            "config": self.config,
            "region": self.region,
            "bootstrap": self.bootstrap,
            "ami": self.ami,
            "docker_image": docker_image or self.config.get("docker_image"),
            "instance_type": self.instance_type,
            "gpu_instance": self.gpu_instance,
            "vpc": self.vpc,
            "subnet_id": self.subnet_id,
            "security_groups": self.security_groups,
            "filesystem_size": self.filesystem_size,
        }
        self.scheduler_options = {**self.options}
        self.worker_options = {**self.options}
        super().__init__(**kwargs)
