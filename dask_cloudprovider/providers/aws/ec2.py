import asyncio

import dask
from dask_cloudprovider.providers.generic.vmcluster import (
    VMCluster,
    VMInterface,
    SchedulerMixin,
    WorkerMixin,
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
    def __init__(self, cluster, config, *args, region=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.instance = None
        self.cluster = cluster
        self.config = config
        self.region = region

    async def create_vm(self):
        """

        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.run_instances
        """

        # TODO get Ubuntu AMI
        ami = "ami-0287acb18b6d8efff"
        # TODO get type
        instance_type = "t2.micro"
        # TODO get security groups
        security_groups = ["sg-7706521d", "sg-08eae7ddaa4b479bd"]
        # TODO get subnet
        subnet_id = "subnet-1fea8e76"

        # TODO Enable Spot support

        async with self.cluster.boto_session.create_client(
            "ec2", region_name=self.region
        ) as client:

            response = await client.run_instances(
                BlockDeviceMappings=[
                    {
                        "DeviceName": "/dev/sda1",
                        "VirtualName": "sda1",
                        "Ebs": {
                            "DeleteOnTermination": True,
                            "VolumeSize": 40,  # TODO make configurable
                            "VolumeType": "gp2",
                            "Encrypted": False,
                        },
                    }
                ],
                ImageId=ami,
                InstanceType=instance_type,
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
                        "Groups": security_groups,
                        "SubnetId": subnet_id,
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

    def __init__(self, region="eu-west-2", worker_command="dask-worker", **kwargs):
        super().__init__(**kwargs)
        self.boto_session = aiobotocore.get_session()
        self.config = dask.config.get("cloudprovider.aws", {})
        self.scheduler_class = EC2Scheduler
        self.worker_class = EC2Worker
        self.options = {"cluster": self, "config": self.config, "region": region}
        self.scheduler_options = {**self.options}
        self.worker_options = {"worker_command": worker_command, **self.options}
