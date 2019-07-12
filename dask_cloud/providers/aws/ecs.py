import asyncio
import logging
import sys
import time
import uuid
import weakref

import boto3  # TODO use async boto
import dask

from distributed.deploy.spec import SpecCluster

logger = logging.getLogger(__name__)

# TODO allow customization of AWS credentials
ec2 = boto3.client("ec2")
ecs = boto3.client("ecs")
iam = boto3.client("iam")
logs = boto3.client("logs")


class Task:
    """ A superclass for ECS Tasks
    See Also
    --------
    Worker
    Scheduler
    """

    def __init__(
        self,
        cluster_arn,
        task_definition_arn,
        vpc_subnets,
        security_groups,
        log_group,
        log_stream_prefix,
        **kwargs
    ):
        self.lock = asyncio.Lock()
        self.cluster_arn = cluster_arn
        self.task_definition_arn = task_definition_arn
        self.task = None
        self.task_arn = None
        self.public_ip = None
        self.private_ip = None
        self.log_group = log_group
        self.log_stream_prefix = log_stream_prefix
        self.connection = None
        self.overrides = {}
        self.vpc_subnets = vpc_subnets
        self.security_groups = security_groups
        self.kwargs = kwargs
        self.status = "created"

    def __await__(self):
        async def _():
            async with self.lock:
                if not self.task:
                    await self.start()
                    assert self.task
            return self

        return _().__await__()

    async def start(self):
        attempts = 60
        while True:
            attempts -= 1
            try:
                [self.task] = ecs.run_task(
                    cluster=self.cluster_arn,
                    taskDefinition=self.task_definition_arn,
                    overrides=self.overrides,
                    count=1,
                    launchType="FARGATE",  # TODO allow non fargate
                    networkConfiguration={
                        "awsvpcConfiguration": {
                            "subnets": self.vpc_subnets,
                            "securityGroups": self.security_groups,
                            "assignPublicIp": "ENABLED",  # TODO allow private clusters
                        }
                    },
                )["tasks"]
                break
            except Exception as e:
                if attempts > 0:
                    time.sleep(1)
                else:
                    raise e
        self.task_arn = self.task["taskArn"]
        while self.task["lastStatus"] in ["PENDING", "PROVISIONING"]:
            time.sleep(1)
            [self.task] = ecs.describe_tasks(
                cluster=self.cluster_arn, tasks=[self.task_arn]
            )["tasks"]
        if self.task["lastStatus"] != "RUNNING":
            raise RuntimeError("%s failed to start" % type(self).__name__)
        [eni] = [
            attachment
            for attachment in self.task["attachments"]
            if attachment["type"] == "ElasticNetworkInterface"
        ]
        [network_interface_id] = [
            detail["value"]
            for detail in eni["details"]
            if detail["name"] == "networkInterfaceId"
        ]
        eni = ec2.describe_network_interfaces(
            NetworkInterfaceIds=[network_interface_id]
        )
        [interface] = eni["NetworkInterfaces"]
        self.public_ip = interface["Association"]["PublicIp"]
        self.private_ip = interface["PrivateIpAddresses"][0]["PrivateIpAddress"]
        while True:
            for line in self.logs():
                if "worker at" in line:
                    self.address = (
                        line.split("worker at:")[1]
                        .strip()
                        .replace(
                            self.private_ip, self.public_ip
                        )  # TODO allow private clusters
                    )
                    self.status = "running"
                    break
                if "Scheduler at" in line:
                    self.address = (
                        line.split("Scheduler at:")[1]
                        .strip()
                        .replace(
                            self.private_ip, self.public_ip
                        )  # TODO allow private clusters
                    )
                    self.status = "running"
                    break
            else:
                continue
            break
        logger.debug("%s", line)

    async def close(self, **kwargs):
        if self.task:
            self.task = ecs.stop_task(cluster=self.cluster_arn, task=self.task_arn)[
                "task"
            ]
            while self.task["lastStatus"] in ["RUNNING"]:
                time.sleep(1)
                [self.task] = ecs.describe_tasks(
                    cluster=self.cluster_arn, tasks=[self.task_arn]
                )["tasks"]
        self.status = "closed"

    @property
    def task_id(self):
        return self.task_arn.split("/")[1]

    @property
    def log_stream_name(self):
        return "{prefix}/{container}/{task_id}".format(
            prefix=self.log_stream_prefix,
            container=self.task["containers"][0]["name"],
            task_id=self.task_id,
        )

    def logs(self):
        next_token = None
        while True:
            if next_token:
                l = logs.get_log_events(
                    logGroupName=self.log_group,
                    logStreamName=self.log_stream_name,
                    nextToken=next_token,
                )
            else:
                l = logs.get_log_events(
                    logGroupName=self.log_group, logStreamName=self.log_stream_name
                )
            next_token = l["nextForwardToken"]
            if not l["events"]:
                break
            for event in l["events"]:
                yield event["message"]

    def __repr__(self):
        return "<ECS Task %s: status=%s>" % (type(self).__name__, self.status)


class Scheduler(Task):
    """ A Remote Dask Scheduler controled by ECS
    Parameters
    ----------
    kwargs:
        TODO
    """


class Worker(Task):
    """ A Remote Dask Worker controled by ECS
    Parameters
    ----------
    scheduler: str
        The address of the scheduler
    kwargs:
        TODO
    """

    def __init__(self, scheduler: str, **kwargs):
        super().__init__(**kwargs)
        self.scheduler = scheduler
        self.overrides = {
            "containerOverrides": [
                {
                    "name": "dask-worker",
                    "environment": [
                        {"name": "DASK_SCHEDULER_ADDRESS", "value": self.scheduler}
                    ],
                }
            ]
        }


class ECSCluster(SpecCluster):
    """ Deploy a Dask cluster using ECS
    Parameters
    ----------
    TODO
    kwargs:
        TODO
    ----
    """

    def __init__(
        self,
        image=None,
        scheduler_cpu=None,
        scheduler_mem=None,
        worker_cpu=None,
        worker_mem=None,
        n_workers=None,
        cluster_arn=None,
        cluster_name_template=None,
        execution_role_arn=None,
        task_role_arn=None,
        cloudwatch_logs_group=None,
        cloudwatch_logs_stream_prefix=None,
        cloudwatch_logs_default_retention=None,
        vpc=None,
        security_groups=None,
        **kwargs
    ):
        self.config = dask.config.get("cloud.ecs", {})

        self.image = image or self.config.get("image") or "daskdev/dask:1.2.0"
        self.scheduler_cpu = scheduler_cpu or self.config.get("scheduler_cpu") or 1024
        self.scheduler_mem = scheduler_mem or self.config.get("scheduler_mem") or 4096
        self.worker_cpu = worker_cpu or self.config.get("worker_cpu") or 4096
        self.worker_mem = worker_mem or self.config.get("worker_mem") or 16384
        self.n_workers = n_workers or self.config.get("n_workers") or 0

        self.cluster_name = None
        self.cluster_name_template = cluster_name_template or self.config.get(
            "cluster_name", "dask-{uuid}"
        )
        if self.cluster_name is None:
            # TODO get cluster name from API for cases where user has supplied arn
            pass

        self.cluster_arn = (
            cluster_arn or self.config.get("cluster_arn") or self.create_cluster()
        )
        self.execution_role_arn = (
            execution_role_arn
            or self.config.get("execution_role_arn")
            or self.create_execution_role()
        )
        self.task_role_arn = (
            task_role_arn or self.config.get("task_role_arn") or self.create_task_role()
        )

        self.cloudwatch_logs_stream_prefix = (
            cloudwatch_logs_stream_prefix
            or self.config.get("cloudwatch_logs_stream_prefix")
            or "{cluster_name}"
        ).format(cluster_name=self.cluster_name)
        self.cloudwatch_logs_default_retention = (
            cloudwatch_logs_default_retention
            or self.config.get("cloudwatch_logs_default_retention")
            or 30
        )
        self.cloudwatch_logs_group = (
            cloudwatch_logs_group
            or self.config.get("cloudwatch_logs_group")
            or self.create_cloudwatch_logs_group()
        )

        self.vpc = vpc or self.config.get("vpc") or "default"
        if self.vpc == "default":
            self.vpc = self.get_default_vpc()
        self.vpc_subnets = self.get_vpc_subnets()

        self.security_groups = (
            security_groups
            or self.config.get("security_groups")
            or self.create_security_groups()
        )

        self.scheduler_task_definition_arn = self.create_scheduler_task_definition_arn()
        self.worker_task_definition_arn = self.create_worker_task_definition_arn()

        options = {
            "cluster_arn": self.cluster_arn,
            "vpc_subnets": self.vpc_subnets,
            "security_groups": self.security_groups,
            "log_group": self.cloudwatch_logs_group,
            "log_stream_prefix": self.cloudwatch_logs_stream_prefix,
        }
        scheduler_options = {
            "task_definition_arn": self.scheduler_task_definition_arn,
            **options,
        }
        worker_options = {
            "task_definition_arn": self.worker_task_definition_arn,
            **options,
        }

        scheduler = {"cls": Scheduler, "options": scheduler_options}
        worker_template = {"cls": Worker, "options": worker_options}
        workers = {i: worker_template for i in range(self.n_workers)}
        super().__init__(workers, scheduler, worker_template, **kwargs)

    def create_cluster(self):
        self.cluster_name = dask.config.expand_environment_variables(
            self.cluster_name_template
        )
        self.cluster_name = self.cluster_name.format(uuid=str(uuid.uuid4())[:10])
        response = ecs.create_cluster(clusterName=self.cluster_name)
        weakref.finalize(self, self.delete_cluster)
        return response["cluster"]["clusterArn"]

    def delete_cluster(self):
        ecs.delete_cluster(cluster=self.cluster_arn)

    @property
    def execution_role_name(self):
        return "{}-{}".format(self.cluster_name, "execution-role")

    def create_execution_role(self):
        response = iam.create_role(
            RoleName=self.execution_role_name,
            AssumeRolePolicyDocument="""{
                "Version": "2012-10-17",
                "Statement": [
                    {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "ecs-tasks.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                    }
                ]
                }""",
            Description="A role for ECS to use when executing",
        )
        iam.attach_role_policy(
            RoleName=self.execution_role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
        )
        iam.attach_role_policy(
            RoleName=self.execution_role_name,
            PolicyArn="arn:aws:iam::aws:policy/CloudWatchLogsFullAccess",
        )
        iam.attach_role_policy(
            RoleName=self.execution_role_name,
            PolicyArn="arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceRole",
        )
        weakref.finalize(self, self.delete_execution_role)
        return response["Role"]["Arn"]

    def delete_execution_role(self):
        for policy in iam.list_attached_role_policies(
            RoleName=self.execution_role_name
        )["AttachedPolicies"]:
            iam.detach_role_policy(
                RoleName=self.execution_role_name, PolicyArn=policy["PolicyArn"]
            )
        iam.delete_role(RoleName=self.execution_role_name)

    @property
    def task_role_name(self):
        return "{}-{}".format(self.cluster_name, "task-role")

    def create_task_role(self):
        response = iam.create_role(
            RoleName=self.task_role_name,
            AssumeRolePolicyDocument="""{
            "Version": "2012-10-17",
            "Statement": [
                {
                "Effect": "Allow",
                "Principal": {
                    "Service": "ecs-tasks.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
                }
            ]
            }""",
            Description="A role for dask tasks to use when executing",
        )

        # TODO allow customisation of policies
        iam.attach_role_policy(
            RoleName=self.task_role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
        )

        weakref.finalize(self, self.delete_task_role)
        return response["Role"]["Arn"]

    def delete_task_role(self):
        for policy in iam.list_attached_role_policies(RoleName=self.task_role_name)[
            "AttachedPolicies"
        ]:
            iam.detach_role_policy(
                RoleName=self.task_role_name, PolicyArn=policy["PolicyArn"]
            )
        iam.delete_role(RoleName=self.task_role_name)

    def create_cloudwatch_logs_group(self):
        log_group_name = "dask-ecs"
        if log_group_name not in [
            group["logGroupName"] for group in logs.describe_log_groups()["logGroups"]
        ]:
            logs.create_log_group(logGroupName=log_group_name)
            logs.put_retention_policy(
                logGroupName=log_group_name,
                retentionInDays=self.cloudwatch_logs_default_retention,
            )
        # Note: Not cleaning up the logs here as they may be useful after the cluster is destroyed
        return log_group_name

    def get_default_vpc(self):
        [vpc] = [vpc for vpc in ec2.describe_vpcs()["Vpcs"] if vpc["IsDefault"]]
        return vpc["VpcId"]

    def get_vpc_subnets(self):
        [vpc] = [vpc for vpc in ec2.describe_vpcs()["Vpcs"] if vpc["VpcId"] == self.vpc]
        return [
            subnet["SubnetId"]
            for subnet in ec2.describe_subnets()["Subnets"]
            if subnet["VpcId"] == vpc["VpcId"]
        ]

    def create_security_groups(self):
        response = ec2.create_security_group(
            Description="A security group for dask-ecs",
            GroupName=self.cluster_name,
            VpcId=self.vpc,
            DryRun=False,
        )
        ec2.authorize_security_group_ingress(
            GroupId=response["GroupId"],
            IpPermissions=[
                {
                    "IpProtocol": "TCP",
                    "FromPort": 8786,
                    "ToPort": 8787,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "Anywhere"}],
                    "Ipv6Ranges": [{"CidrIpv6": "::/0", "Description": "Anywhere"}],
                },
                {
                    "IpProtocol": "TCP",
                    "FromPort": 0,
                    "ToPort": 65535,
                    "UserIdGroupPairs": [{"GroupName": self.cluster_name}],
                },
            ],
            DryRun=False,
        )
        weakref.finalize(self, self.delete_security_groups)
        return [response["GroupId"]]

    def delete_security_groups(self):
        # TODO Add retries
        ec2.delete_security_group(GroupName=self.cluster_name, DryRun=False)

    def create_scheduler_task_definition_arn(self):
        response = ecs.register_task_definition(
            family="{}-{}".format(self.cluster_name, "scheduler"),
            taskRoleArn=self.task_role_arn,
            executionRoleArn=self.execution_role_arn,
            networkMode="awsvpc",
            containerDefinitions=[
                {
                    "name": "dask-scheduler",
                    "image": self.image,
                    "cpu": self.scheduler_cpu,
                    "memory": self.scheduler_mem,
                    "memoryReservation": self.scheduler_mem,
                    "essential": True,
                    "command": ["dask-scheduler"],
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-region": ecs.meta.region_name,
                            "awslogs-group": self.cloudwatch_logs_group,
                            "awslogs-stream-prefix": self.cloudwatch_logs_stream_prefix,
                            "awslogs-create-group": "true",
                        },
                    },
                }
            ],
            volumes=[],
            requiresCompatibilities=["FARGATE"],  # TODO allow non fargate
            cpu=str(self.scheduler_cpu),
            memory=str(self.scheduler_mem),
        )
        weakref.finalize(self, self.delete_scheduler_task_definition_arn)
        return response["taskDefinition"]["taskDefinitionArn"]

    def delete_scheduler_task_definition_arn(self):
        ecs.deregister_task_definition(
            taskDefinition=self.scheduler_task_definition_arn
        )

    def create_worker_task_definition_arn(self):
        response = ecs.register_task_definition(
            family="{}-{}".format(self.cluster_name, "worker"),
            taskRoleArn=self.task_role_arn,
            executionRoleArn=self.execution_role_arn,
            networkMode="awsvpc",
            containerDefinitions=[
                {
                    "name": "dask-worker",
                    "image": self.image,
                    "cpu": self.worker_cpu,
                    "memory": self.worker_mem,
                    "memoryReservation": self.worker_mem,
                    "essential": True,
                    "command": [
                        "dask-worker",
                        "--nthreads",
                        "{}".format(int(self.worker_cpu / 1024)),
                        "--no-bokeh",
                        "--memory-limit",
                        "{}GB".format(int(self.worker_mem / 1024)),
                        "--death-timeout",
                        "60",
                    ],
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-region": ecs.meta.region_name,
                            "awslogs-group": self.cloudwatch_logs_group,
                            "awslogs-stream-prefix": self.cloudwatch_logs_stream_prefix,
                            "awslogs-create-group": "true",
                        },
                    },
                }
            ],
            volumes=[],
            requiresCompatibilities=["FARGATE"],  # TODO allow non fargate
            cpu=str(self.worker_cpu),
            memory=str(self.worker_mem),
        )
        weakref.finalize(self, self.delete_worker_task_definition_arn)
        return response["taskDefinition"]["taskDefinitionArn"]

    def delete_worker_task_definition_arn(self):
        ecs.deregister_task_definition(taskDefinition=self.worker_task_definition_arn)
