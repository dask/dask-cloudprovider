import asyncio
import random

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
    dict_to_aws,
)
from dask_cloudprovider.utils.timeout import Timeout

try:
    from aiobotocore.session import get_session
    import botocore.exceptions
    import botocore.config
except ImportError as e:
    msg = (
        "Dask Cloud Provider AWS requirements are not installed.\n\n"
        "Please either conda or pip install as follows:\n\n"
        "  conda install -c conda-forge dask-cloudprovider       # either conda install\n"
        '  pip install "dask-cloudprovider[aws]" --upgrade       # or python -m pip install'
    )
    raise ImportError(msg) from e


class EC2Instance(VMInterface):
    def __init__(
        self,
        cluster,
        config,
        *args,
        region=None,
        availability_zone=None,
        bootstrap=None,
        extra_bootstrap=None,
        ami=None,
        docker_image=None,
        env_vars=None,
        instance_type=None,
        gpu_instance=None,
        vpc=None,
        subnet_id=None,
        security_groups=None,
        filesystem_size=None,
        key_name=None,
        iam_instance_profile=None,
        instance_tags: None,
        volume_tags: None,
        use_private_ip: False,
        enable_detailed_monitoring=None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.instance = None
        self.cluster = cluster
        self.config = config
        self.region = region
        self.availability_zone = availability_zone
        self.bootstrap = bootstrap
        self.extra_bootstrap = extra_bootstrap
        self.ami = ami
        self.docker_image = docker_image or self.config.get("docker_image")
        self.env_vars = env_vars
        self.instance_type = instance_type
        self.gpu_instance = gpu_instance
        self.vpc = vpc
        self.subnet_id = subnet_id
        self.security_groups = security_groups
        self.filesystem_size = filesystem_size
        self.key_name = key_name
        self.iam_instance_profile = iam_instance_profile
        self.instance_tags = instance_tags
        self.volume_tags = volume_tags
        self.use_private_ip = use_private_ip
        self.enable_detailed_monitoring = enable_detailed_monitoring

    async def create_vm(self):
        """

        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.run_instances
        """
        # TODO Enable Spot support

        boto_config = botocore.config.Config(retries=dict(max_attempts=10))
        async with self.cluster.boto_session.create_client(
            "ec2", region_name=self.region, config=boto_config
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

            vm_kwargs = {
                "BlockDeviceMappings": [
                    {
                        "DeviceName": "/dev/sda1",
                        "VirtualName": "sda1",
                        "Ebs": {
                            "DeleteOnTermination": True,
                            "VolumeSize": self.filesystem_size,
                            "VolumeType": "gp3",
                            "Encrypted": False,
                        },
                    }
                ],
                "ImageId": self.ami,
                "InstanceType": self.instance_type,
                "MaxCount": 1,
                "MinCount": 1,
                "Monitoring": {"Enabled": self.enable_detailed_monitoring},
                "UserData": self.cluster.render_process_cloud_init(self),
                "InstanceInitiatedShutdownBehavior": "terminate",
                "NetworkInterfaces": [
                    {
                        "AssociatePublicIpAddress": False
                        if self.use_private_ip
                        else True,
                        "DeleteOnTermination": True,
                        "Description": "private" if self.use_private_ip else "public",
                        "DeviceIndex": 0,
                        "Groups": self.security_groups,
                        "SubnetId": self.subnet_id,
                    }
                ],
                "TagSpecifications": [
                    {
                        "ResourceType": "instance",
                        "Tags": dict_to_aws(self.instance_tags, upper=True),
                    },
                    {
                        "ResourceType": "volume",
                        "Tags": dict_to_aws(self.volume_tags, upper=True),
                    },
                ],
            }

            if self.key_name:
                vm_kwargs["KeyName"] = self.key_name

            if self.iam_instance_profile:
                vm_kwargs["IamInstanceProfile"] = self.iam_instance_profile

            if self.availability_zone:
                if isinstance(self.availability_zone, list):
                    self.availability_zone = random.choice(self.availability_zone)
                vm_kwargs["Placement"] = {"AvailabilityZone": self.availability_zone}

            response = await client.run_instances(**vm_kwargs)
            [self.instance] = response["Instances"]

            try:  # Ensure we tear down any resources we allocated if something goes wrong
                return await self.configure_vm(client)
            except Exception:
                self.cluster._log(
                    f"reclaiming vm because configure_vm failed {self.name}"
                )
                await self.destroy_vm()
                raise

    async def configure_vm(self, client):
        timeout = Timeout(300, f"Failed to add tags for {self.instance['InstanceId']}")
        backoff = 0.1
        while timeout.run():
            try:
                await client.create_tags(
                    Resources=[self.instance["InstanceId"]],
                    Tags=[
                        {"Key": "Name", "Value": self.name},
                        {"Key": "Dask Cluster", "Value": self.cluster.uuid},
                    ],
                )
                break
            except Exception as e:
                timeout.set_exception(e)

            await asyncio.sleep(min(backoff, 10) + backoff % 1)
            # Exponential backoff with a cap of 10 seconds and some jitter
            backoff = backoff * 2

        self.cluster._log(
            f"Created instance {self.instance['InstanceId']} as {self.name}"
        )

        address_type = "Private" if self.use_private_ip else "Public"
        ip_address_key = f"{address_type}IpAddress"

        default_error = (
            f"Failed {address_type} IP for instance {self.instance['InstanceId']}"
        )
        timeout = Timeout(300, default_error)
        backoff = 0.1
        while self.instance.get(ip_address_key) is None and timeout.run():
            try:
                response = await client.describe_instances(
                    InstanceIds=[self.instance["InstanceId"]], DryRun=False
                )
                [reservation] = response["Reservations"]
                [self.instance] = reservation["Instances"]
            except botocore.exceptions.ClientError as e:
                timeout.set_exception(e)
            await asyncio.sleep(min(backoff, 10) + backoff % 1)
            # Exponential backoff with a cap of 10 seconds and some jitter
            backoff = backoff * 2
        return self.instance[ip_address_key], None

    async def destroy_vm(self):
        boto_config = botocore.config.Config(retries=dict(max_attempts=10))
        async with self.cluster.boto_session.create_client(
            "ec2", region_name=self.region, config=boto_config
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
        The region to start your clusters. By default this will be detected from your config.
    availability_zone: string or List(string) (optional)
        The availability zone to start your clusters. By default AWS will select the AZ with most free capacity.
        If you specify more than one then scheduler and worker VMs will be randomly assigned to one of your
        chosen AZs.
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
        A valid EC2 instance type. This will determine the resources available to the scheduler and all workers.
        If supplied, you may not specify ``scheduler_instance_type`` or ``worker_instance_type``.

        See https://aws.amazon.com/ec2/instance-types/.

        By default will use ``t2.micro``.
    scheduler_instance_type: string (optional)
        A valid EC2 instance type.  This will determine the resources available to the scheduler.

        See https://aws.amazon.com/ec2/instance-types/.

        By default will use ``t2.micro``.
    worker_instance_type: string (optional)
        A valid EC2 instance type.  This will determine the resources available to all workers.

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
    key_name: str (optional)
        The SSH key name to assign to all instances created by the cluster manager.
        You can list your existing key pair names with
        ``aws ec2 describe-key-pairs  --query 'KeyPairs[*].KeyName' --output text``.

        NOTE: You will need to ensure your security group allows access on port 22. If ``security_groups``
        is not set the default group will not contain this rule and you will need to add it manually.
    iam_instance_profile: dict (optional)
        An IAM profile to assign to VMs. This can be used for allowing access to other AWS resources such as S3.
        See https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html.
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
    docker_args: string (optional)
        Extra command line arguments to pass to Docker.
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
        be created automatically. Default is ``True``.
    debug: bool, optional
        More information will be printed when constructing clusters to enable debugging.
    instance_tags: dict, optional
        Tags to be applied to all EC2 instances upon creation. By default, includes
        "createdBy": "dask-cloudprovider"
    volume_tags: dict, optional
        Tags to be applied to all EBS volumes upon creation. By default, includes
        "createdBy": "dask-cloudprovider"
    use_private_ip: bool (optional)
        Whether to use a private IP (if True) or public IP (if False).

        Default ``False``.
    enable_detailed_monitoring: bool (optional)
        Whether to enable detailed monitoring for created instances.
        See https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-cloudwatch-new.html
        Default ``False``.

    Notes
    -----

    **Resources created**

    .. csv-table::
        :header: Resource, Name, Purpose, Cost

        EC2 Instance, dask-scheduler-{cluster uuid}, Dask Scheduler, "`EC2 Pricing
        <https://aws.amazon.com/ec2/pricing/>`_"
        EC2 Instance, dask-worker-{cluster uuid}-{worker uuid}, Dask Workers, "`EC2 Pricing
        <https://aws.amazon.com/ec2/pricing/>`_"

    **Credentials**

    In order for Dask workers to access AWS resources such as S3 they will need credentials.

    The best practice way of doing this is to pass an IAM role to be used by workers. See the ``iam_instance_profile``
    keyword for more information.

    Alternatively you could read in your local credentials created with ``aws configure`` and pass them along
    as environment variables. Here is a small example to help you do that.

    >>> def get_aws_credentials():
    ...     parser = configparser.RawConfigParser()
    ...     parser.read(os.path.expanduser('~/.aws/config'))
    ...     config = parser.items('default')
    ...     parser.read(os.path.expanduser('~/.aws/credentials'))
    ...     credentials = parser.items('default')
    ...     all_credentials = {key.upper(): value for key, value in [*config, *credentials]}
    ...     with contextlib.suppress(KeyError):
    ...         all_credentials["AWS_REGION"] = all_credentials.pop("REGION")
    ...     return all_credentials
    >>> cluster = EC2Cluster(env_vars=get_aws_credentials())

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


    >>> env_vars =
    >>> cluster = EC2Cluster(ami="ami-06d62f645899df7de",  # Deep Learning AMI Ubuntu 18.04 (these are region specific)
    ...                      docker_image="rapidsai/rapidsai:cuda11.0-runtime-ubuntu18.04",
    ...                      instance_type="g4dn.xlarge",
    ...                      worker_class="dask_cuda.CUDAWorker",
    ...                      n_workers=2,
    ...                      bootstrap=False,
    ...                      filesystem_size=120,
    ...                      env_vars=get_aws_credentials()) # Pass credentials to Cluster see Notes section for info
=======

    Enable SSH for debugging

    >>> from dask_cloudprovider.aws import EC2Cluster
    >>> cluster = EC2Cluster(key_name="myawesomekey",
                             # Security group which allows ports 22, 8786, 8787 and all internal traffic
                             security_groups=["sg-aabbcc112233"])

    # You can now SSH to an instance with `ssh ubuntu@public_ip`

    >>> cluster.close()

    """

    def __init__(
        self,
        region=None,
        availability_zone=None,
        bootstrap=None,
        auto_shutdown=None,
        ami=None,
        instance_type=None,
        scheduler_instance_type=None,
        worker_instance_type=None,
        vpc=None,
        subnet_id=None,
        security_groups=None,
        filesystem_size=None,
        key_name=None,
        iam_instance_profile=None,
        docker_image=None,
        debug=False,
        instance_tags=None,
        volume_tags=None,
        use_private_ip=None,
        enable_detailed_monitoring=None,
        **kwargs,
    ):
        self.boto_session = get_session()
        self.config = dask.config.get("cloudprovider.ec2", {})
        self.scheduler_class = EC2Scheduler
        self.worker_class = EC2Worker
        self.region = region if region is not None else self.config.get("region")
        self.availability_zone = (
            availability_zone
            if availability_zone is not None
            else self.config.get("availability_zone")
        )
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
        if instance_type is None:
            self.scheduler_instance_type = (
                scheduler_instance_type
                if scheduler_instance_type is not None
                else self.config.get("scheduler_instance_type")
            )
            self.worker_instance_type = (
                worker_instance_type
                if worker_instance_type is not None
                else self.config.get("worker_instance_type")
            )
        else:
            if scheduler_instance_type is not None or worker_instance_type is not None:
                raise ValueError(
                    "If you specify instance_type, you may not specify scheduler_instance_type or worker_instance_type"
                )
            self.scheduler_instance_type = instance_type
            self.worker_instance_type = instance_type

        self.gpu_instance = self.instance_type.startswith(("p", "g"))
        self.vpc = vpc if vpc is not None else self.config.get("vpc")
        self.subnet_id = (
            subnet_id if subnet_id is not None else self.config.get("subnet_id")
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

        self.key_name = (
            key_name if key_name is not None else self.config.get("key_name")
        )
        self.iam_instance_profile = (
            iam_instance_profile
            if iam_instance_profile is not None
            else self.config.get("iam_instance_profile")
        )
        self.debug = debug

        instance_tags = instance_tags if instance_tags is not None else {}
        self.instance_tags = {**instance_tags, **self.config.get("instance_tags")}

        volume_tags = volume_tags if volume_tags is not None else {}
        self.volume_tags = {**volume_tags, **self.config.get("volume_tags")}

        self._use_private_ip = (
            use_private_ip
            if use_private_ip is not None
            else self.config.get("use_private_ip")
        )

        self.enable_detailed_monitoring = (
            enable_detailed_monitoring
            if enable_detailed_monitoring is not None
            else self.config.get("enable_detailed_monitoring")
        )

        self.options = {
            "cluster": self,
            "config": self.config,
            "region": self.region,
            "availability_zone": self.availability_zone,
            "bootstrap": self.bootstrap,
            "ami": self.ami,
            "docker_image": docker_image or self.config.get("docker_image"),
            "instance_type": self.instance_type,
            "scheduler_instance_type": self.scheduler_instance_type,
            "worker_instance_type": self.worker_instance_type,
            "gpu_instance": self.gpu_instance,
            "vpc": self.vpc,
            "subnet_id": self.subnet_id,
            "security_groups": self.security_groups,
            "filesystem_size": self.filesystem_size,
            "key_name": self.key_name,
            "iam_instance_profile": self.iam_instance_profile,
            "instance_tags": self.instance_tags,
            "volume_tags": self.volume_tags,
            "use_private_ip": self._use_private_ip,
            "enable_detailed_monitoring": self.enable_detailed_monitoring,
        }
        self.scheduler_options = {**self.options}
        self.worker_options = {**self.options}
        self.scheduler_options["instance_type"] = self.scheduler_instance_type
        self.worker_options["instance_type"] = self.worker_instance_type
        super().__init__(debug=debug, **kwargs)
