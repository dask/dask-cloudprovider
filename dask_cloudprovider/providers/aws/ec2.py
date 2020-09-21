import asyncio

import dask
from dask_cloudprovider.providers.generic.vmcluster import (
    VMCluster,
    VMInterface,
    SchedulerMixin,
    WorkerMixin,
)
from dask_cloudprovider.providers.aws.helper import (
    get_latest_ami_id,
    get_default_vpc,
    get_vpc_subnets,
    get_security_group,
)

try:
    from botocore.exceptions import ClientError
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
        ami=None,
        instance_type=None,
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
        self.region = region or self.config.get("region")
        self.ami = ami or self.config.get("ami")
        self.instance_type = instance_type or self.config.get("instance_type")
        self.vpc = vpc or self.config.get("vpc")
        self.subnet_id = subnet_id or self.config.get("subnet_id")
        self.security_groups = security_groups or self.config.get("security_groups")
        self.filesystem_size = filesystem_size or self.config.get("filesystem_size")

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
                "ubuntu/images/hvm-instance/ubuntu-bionic-18.04-amd64-server-*",
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
                UserData=self.render_cloud_init(
                    image=self.docker_image, command=self.command
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
                Tags=[{"Key": "Name", "Value": self.name}],
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

    async def close(self):
        async with self.cluster.boto_session.create_client(
            "ec2", region_name=self.region
        ) as client:
            await client.terminate_instances(
                InstanceIds=[self.instance["InstanceId"]], DryRun=False
            )
            self.cluster._log(f"Terminated {self.name} ({self.instance['InstanceId']})")
            await super().close()


class EC2Scheduler(EC2Instance, SchedulerMixin):
    """Scheduler running in an EC2 instance.

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.init_scheduler()

    async def start(self):
        await super().start()
        await self.start_scheduler()


class EC2Worker(EC2Instance, WorkerMixin):
    """Worker running in an EC2 instance.

    """

    def __init__(self, scheduler, *args, worker_command=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.init_worker(scheduler, *args, worker_command=worker_command, **kwargs)

    async def start(self):
        await super().start()
        await self.start_worker()


class EC2Cluster(VMCluster):
    """Cluster running on EC2 instances.

    """

    def __init__(
        self,
        region="eu-west-2",
        worker_command="dask-worker",
        ami=None,
        instance_type=None,
        vpc=None,
        subnet_id=None,
        security_groups=None,
        filesystem_size=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.boto_session = aiobotocore.get_session()
        self.config = dask.config.get("cloudprovider.ec2", {})
        self.scheduler_class = EC2Scheduler
        self.worker_class = EC2Worker
        self.options = {
            "cluster": self,
            "config": self.config,
            "region": region,
            "ami": ami,
            "instance_type": instance_type,
            "vpc": vpc,
            "subnet_id": subnet_id,
            "security_groups": security_groups,
            "filesystem_size": filesystem_size,
        }
        self.scheduler_options = {**self.options}
        self.worker_options = {"worker_command": worker_command, **self.options}
