import asyncio
import logging
import sys
import time
import uuid
import weakref

import aiobotocore  # TODO use async boto everywhere
import dask

from dask_cloud.providers.aws.helper import dict_to_aws

from distributed.deploy.spec import SpecCluster

logger = logging.getLogger(__name__)


DEFAULT_TAGS = {"createdBy": "dask-cloud"}  # Package tags to apply to all resources


class Task:
    """ A superclass for ECS Tasks
    See Also
    --------
    Worker
    Scheduler
    """

    def __init__(
        self,
        clients,
        cluster_arn,
        task_definition_arn,
        vpc_subnets,
        security_groups,
        log_group,
        log_stream_prefix,
        fargate,
        tags,
        **kwargs
    ):
        self.lock = asyncio.Lock()
        self.clients = clients
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
        self.fargate = fargate
        self.tags = tags
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

    async def _is_long_arn_format_enabled(self):
        [response] = (
            await self.clients["ecs"].list_account_settings(
                name="taskLongArnFormat", effectiveSettings=True
            )
        )["settings"]
        return response["value"] == "enabled"

    async def _update_task(self):
        [self.task] = await self.clients["ecs"].describe_tasks(
            cluster=self.cluster_arn, tasks=[self.task_arn]
        )["tasks"]

    async def is_running(self):
        await self._update_task()
        return self.task["lastStatus"] == "RUNNING"

    async def start(self):
        attempts = 60
        while True:
            attempts -= 1
            try:
                kwargs = (
                    {"tags": dict_to_aws(self.tags)}
                    if await self._is_long_arn_format_enabled()
                    else {}
                )  # Tags are only supported if you opt into long arn format so we need to check for that
                [self.task] = (
                    await self.clients["ecs"].run_task(
                        cluster=self.cluster_arn,
                        taskDefinition=self.task_definition_arn,
                        overrides=self.overrides,
                        count=1,
                        launchType="FARGATE" if self.fargate else "EC2",
                        networkConfiguration={
                            "awsvpcConfiguration": {
                                "subnets": self.vpc_subnets,
                                "securityGroups": self.security_groups,
                                "assignPublicIp": "ENABLED",  # TODO allow private clusters
                            }
                        },
                        **kwargs
                    )
                )["tasks"]
                break
            except Exception as e:
                if attempts > 0:
                    await asyncio.sleep(1)
                else:
                    raise e
        self.task_arn = self.task["taskArn"]
        while self.task["lastStatus"] in ["PENDING", "PROVISIONING"]:
            await asyncio.sleep(1)
            await self._update_task()
        if not await self.is_running():
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
        eni = await self.clients["ec2"].describe_network_interfaces(
            NetworkInterfaceIds=[network_interface_id]
        )
        [interface] = eni["NetworkInterfaces"]
        self.public_ip = interface["Association"]["PublicIp"]
        self.private_ip = interface["PrivateIpAddresses"][0]["PrivateIpAddress"]
        while True:
            async for line in self.logs():
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
                if not await self.is_running():
                    raise RuntimeError("%s exited unexpectedly!" % type(self).__name__)
                continue
            break
        logger.debug("%s", line)

    async def close(self, **kwargs):
        if self.task:
            self.task = await self.clients["ecs"].stop_task(
                cluster=self.cluster_arn, task=self.task_arn
            )["task"]
            while self.task["lastStatus"] in ["RUNNING"]:
                await asyncio.sleep(1)
                [self.task] = (
                    await self.clients["ecs"].describe_tasks(
                        cluster=self.cluster_arn, tasks=[self.task_arn]
                    )
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

    async def logs(self):
        next_token = None
        while True:
            if next_token:
                l = await self.clients["logs"].get_log_events(
                    logGroupName=self.log_group,
                    logStreamName=self.log_stream_name,
                    nextToken=next_token,
                )
            else:
                l = await self.clients["logs"].get_log_events(
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
        TODO document Scheduler kwargs
    """


class Worker(Task):
    """ A Remote Dask Worker controled by ECS
    Parameters
    ----------
    scheduler: str
        The address of the scheduler
    kwargs:
        TODO document worker kwargs
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

    This creates a dask scheduler and workers on an ECS cluster. If you do not
    configure a cluster one will be created for you with sensible defaults.

    Parameters
    ----------
    fargate: bool (optional)
        Select whether or not to use fargate.

        Defaults to ``False``. You must provide an existing cluster.
    image: str (optional)
        The docker image to use for the scheduler and worker tasks.

        Defaults to ``daskdev/dask:latest``.
    scheduler_cpu: int (optional)
        The amount of CPU to request for the scheduler in milli-cpu (1/1024).

        Defaults to ``1024`` (one vCPU).
    scheduler_mem: int (optional)
        The amount of memory to request for the scheduler in MB.

        Defaults to ``4096`` (4GB).
    scheduler_timeout: str (optional)
        The scheduler task will exit after this amount of time if there are no clients connected.

        Defaults to ``5 minutes``.
    worker_cpu: int (optional)
        The amount of CPU to request for worker tasks in milli-cpu (1/1024).

        Defaults to ``4096`` (four vCPUs).
    worker_mem: int (optional)
        The amount of memory to request for worker tasks in MB.

        Defaults to ``16384`` (16GB).
    n_workers: int (optional)
        Number of workers to start on cluster creation.

        Defaults to ``None``.
    cluster_arn: str (optional if fargate is true)
        The ARN of an existing ECS cluster to use for launching tasks.

        Defaults to ``None`` which results in a new cluster being created for you.
    cluster_name_template: str (optional)
        A template to use for the cluster name if ``cluster_arn`` is set to
        ``None``.

        Defaults to ``'dask-{uuid}'``
    execution_role_arn: str (optional)
        The ARN of an existing IAM role to use for ECS execution.

        This ARN must have ``sts:AssumeRole`` allowed for
        ``ecs-tasks.amazonaws.com`` and allow the following permissions:

        - ``ecr:GetAuthorizationToken``
        - ``ecr:BatchCheckLayerAvailability``
        - ``ecr:GetDownloadUrlForLayer``
        - ``ecr:GetRepositoryPolicy``
        - ``ecr:DescribeRepositories``
        - ``ecr:ListImages``
        - ``ecr:DescribeImages``
        - ``ecr:BatchGetImage``
        - ``logs:*``
        - ``ec2:AuthorizeSecurityGroupIngress``
        - ``ec2:Describe*``
        - ``elasticloadbalancing:DeregisterInstancesFromLoadBalancer``
        - ``elasticloadbalancing:DeregisterTargets``
        - ``elasticloadbalancing:Describe*``
        - ``elasticloadbalancing:RegisterInstancesWithLoadBalancer``
        - ``elasticloadbalancing:RegisterTargets``

        Defaults to ``None`` (one will be created for you).
    task_role_arn: str (optional)
        The ARN for an existing IAM role for tasks to assume. This defines
        which AWS resources the dask workers can access directly. Useful if
        you need to read from S3 or a database without passing credentials
        around.

        Defaults to ``None`` (one will be created with S3 read permission only).
    cloudwatch_logs_group: str (optional)
        The name of an existing cloudwatch log group to place logs into.

        Default ``None`` (one will be created called ``dask-ecs``)
    cloudwatch_logs_stream_prefix: str (optional)
        Prefix for log streams.

        Defaults to the cluster name.
    cloudwatch_logs_default_retention: int (optional)
        Retention for logs in days. For use when log group is auto created.

        Defaults to ``30``.
    vpc: str (optional)
        The ID of the VPC you wish to launch your cluster in.

        Defaults to ``None`` (your default VPC will be used).
    security_groups: List[str] (optional)
        A list of security group IDs to use when launching tasks.

        Defaults to ``None`` (one will be created which allows all traffic
        between tasks and access to ports ``8786`` and ``8787`` from anywhere).
    tags: dict (optional)
        Tags to apply to all resources created automatically.

        Defaults to ``None``. Tags will always include ``{"createdBy": "dask-cloud"}``
    **kwargs: dict
        Additional keyword arguments to pass to ``SpecCluster``.

    Examples
    --------
    TODO write ECSCluster examples docs
    """

    # TODO clean up API, aka make private methods private

    def __init__(
        self,
        fargate=False,
        image=None,
        scheduler_cpu=None,
        scheduler_mem=None,
        scheduler_timeout=None,
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
        tags=None,
        **kwargs
    ):
        self.config = dask.config.get("cloud.ecs", {})
        self.clients = self.sync(self.get_clients())
        self.fargate = fargate or self.config.get("fargate") or False
        self._tags = tags or self.config.get("tags") or {}

        self.image = image or self.config.get("image") or "daskdev/dask:latest"
        self.scheduler_cpu = scheduler_cpu or self.config.get("scheduler_cpu") or 1024
        self.scheduler_mem = scheduler_mem or self.config.get("scheduler_mem") or 4096
        self.scheduler_timeout = (
            scheduler_timeout or self.config.get("scheduler_timeout") or "5 minutes"
        )
        self.worker_cpu = worker_cpu or self.config.get("worker_cpu") or 4096
        self.worker_mem = worker_mem or self.config.get("worker_mem") or 16384
        self.n_workers = n_workers or self.config.get("n_workers") or 0
        self.environment = {
            "DASK_DISTRIBUTED__SCHEDULER__IDLE_TIMEOUT": self.scheduler_timeout
        }

        self.cluster_name = None
        self.cluster_name_template = cluster_name_template or self.config.get(
            "cluster_name", "dask-{uuid}"
        )

        self.cluster_arn = (
            cluster_arn or self.config.get("cluster_arn") or self.create_cluster()
        )
        if self.cluster_name is None:
            [cluster_info] = self.clients["ecs"].describe_clusters(  # TODO Async
                clusters=[self.cluster_arn]
            )["clusters"]
            self.cluster_name = cluster_info["clusterName"]

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
            "clients": self.clients,
            "cluster_arn": self.cluster_arn,
            "vpc_subnets": self.vpc_subnets,
            "security_groups": self.security_groups,
            "log_group": self.cloudwatch_logs_group,
            "log_stream_prefix": self.cloudwatch_logs_stream_prefix,
            "fargate": self.fargate,
            "tags": self.tags,
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

    @property
    def tags(self):
        return {**self._tags, **DEFAULT_TAGS}

    async def get_clients(self):
        session = aiobotocore.get_session()
        return {
            "ec2": await session.create_client("ec2"),
            "ecs": await session.create_client("ecs"),
            "iam": await session.create_client("iam"),
            "logs": await session.create_client("logs"),
        }

    def create_cluster(self):
        if not self.fargate:
            raise RuntimeError("You must specify a cluster when not using Fargate.")
        self.cluster_name = dask.config.expand_environment_variables(
            self.cluster_name_template
        )
        self.cluster_name = self.cluster_name.format(uuid=str(uuid.uuid4())[:10])
        response = self.clients["ecs"].create_cluster(  # TODO Async
            clusterName=self.cluster_name, tags=dict_to_aws(self.tags)
        )
        weakref.finalize(self, self.delete_cluster)
        return response["cluster"]["clusterArn"]

    def delete_cluster(self):
        self.clients["ecs"].delete_cluster(cluster=self.cluster_arn)  # TODO Async

    @property
    def execution_role_name(self):
        return "{}-{}".format(self.cluster_name, "execution-role")

    def create_execution_role(self):
        response = self.clients["iam"].create_role(  # TODO Async
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
            Tags=dict_to_aws(self.tags, upper=True),
        )
        self.clients["iam"].attach_role_policy(  # TODO Async
            RoleName=self.execution_role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
        )
        self.clients["iam"].attach_role_policy(  # TODO Async
            RoleName=self.execution_role_name,
            PolicyArn="arn:aws:iam::aws:policy/CloudWatchLogsFullAccess",
        )
        self.clients["iam"].attach_role_policy(  # TODO Async
            RoleName=self.execution_role_name,
            PolicyArn="arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceRole",
        )
        weakref.finalize(self, self.delete_execution_role)
        return response["Role"]["Arn"]

    def delete_execution_role(self):
        for policy in self.clients["iam"].list_attached_role_policies(  # TODO Async
            RoleName=self.execution_role_name
        )["AttachedPolicies"]:
            self.clients["iam"].detach_role_policy(  # TODO Async
                RoleName=self.execution_role_name, PolicyArn=policy["PolicyArn"]
            )
        self.clients["iam"].delete_role(RoleName=self.execution_role_name)  # TODO Async

    @property
    def task_role_name(self):
        return "{}-{}".format(self.cluster_name, "task-role")

    def create_task_role(self):
        response = self.clients["iam"].create_role(  # TODO Async
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
            Tags=dict_to_aws(self.tags, upper=True),
        )

        # TODO allow customisation of policies
        self.clients["iam"].attach_role_policy(  # TODO Async
            RoleName=self.task_role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
        )

        weakref.finalize(self, self.delete_task_role)
        return response["Role"]["Arn"]

    def delete_task_role(self):  # TODO combine with delete execution role
        for policy in self.clients["iam"].list_attached_role_policies(  # TODO Async
            RoleName=self.task_role_name
        )["AttachedPolicies"]:
            self.clients["iam"].detach_role_policy(  # TODO Async
                RoleName=self.task_role_name, PolicyArn=policy["PolicyArn"]
            )
        self.clients["iam"].delete_role(RoleName=self.task_role_name)  # TODO Async

    def create_cloudwatch_logs_group(self):
        log_group_name = "dask-ecs"
        if log_group_name not in [
            group["logGroupName"]
            for group in self.clients["logs"].describe_log_groups()[
                "logGroups"
            ]  # TODO Async
        ]:
            self.clients["logs"].create_log_group(  # TODO Async
                logGroupName=log_group_name, tags=dict_to_aws(self.tags)
            )
            self.clients["logs"].put_retention_policy(  # TODO Async
                logGroupName=log_group_name,
                retentionInDays=self.cloudwatch_logs_default_retention,
            )
        # Note: Not cleaning up the logs here as they may be useful after the cluster is destroyed
        return log_group_name

    def get_default_vpc(self):
        [vpc] = [
            vpc
            for vpc in self.clients["ec2"].describe_vpcs()["Vpcs"]  # TODO Async
            if vpc["IsDefault"]
        ]
        return vpc["VpcId"]

    def get_vpc_subnets(self):
        [vpc] = [
            vpc
            for vpc in self.clients["ec2"].describe_vpcs()["Vpcs"]  # TODO Async
            if vpc["VpcId"] == self.vpc
        ]
        return [
            subnet["SubnetId"]
            for subnet in self.clients["ec2"].describe_subnets()[
                "Subnets"
            ]  # TODO Async
            if subnet["VpcId"] == vpc["VpcId"]
        ]

    def create_security_groups(self):
        response = self.clients["ec2"].create_security_group(  # TODO Async
            Description="A security group for dask-ecs",
            GroupName=self.cluster_name,
            VpcId=self.vpc,
            DryRun=False,
        )
        self.clients["ec2"].authorize_security_group_ingress(  # TODO Async
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
        self.clients["ec2"].create_tags(  # TODO Async
            Resources=[response["GroupId"]], Tags=dict_to_aws(self.tags, upper=True)
        )
        weakref.finalize(self, self.delete_security_groups)
        return [response["GroupId"]]

    def delete_security_groups(self):
        # TODO Add retries
        self.clients["ec2"].delete_security_group(  # TODO Async
            GroupName=self.cluster_name, DryRun=False
        )

    def create_scheduler_task_definition_arn(self):
        response = self.clients["ecs"].register_task_definition(  # TODO Async
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
                    "environment": dict_to_aws(self.environment, key_string="name"),
                    "command": ["dask-scheduler"],
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-region": self.clients[
                                "ecs"
                            ].meta.region_name,  # TODO Async??
                            "awslogs-group": self.cloudwatch_logs_group,
                            "awslogs-stream-prefix": self.cloudwatch_logs_stream_prefix,
                            "awslogs-create-group": "true",
                        },
                    },
                }
            ],
            volumes=[],
            requiresCompatibilities=["FARGATE"] if self.fargate else [],
            cpu=str(self.scheduler_cpu),
            memory=str(self.scheduler_mem),
            tags=dict_to_aws(self.tags),
        )
        weakref.finalize(self, self.delete_scheduler_task_definition_arn)
        return response["taskDefinition"]["taskDefinitionArn"]

    def delete_scheduler_task_definition_arn(self):
        self.clients["ecs"].deregister_task_definition(  # TODO Async
            taskDefinition=self.scheduler_task_definition_arn
        )

    def create_worker_task_definition_arn(self):
        response = self.clients["ecs"].register_task_definition(  # TODO Async
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
                    "environment": dict_to_aws(self.environment, key_string="name"),
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
                            "awslogs-region": self.clients[
                                "ecs"
                            ].meta.region_name,  # TODO Async??
                            "awslogs-group": self.cloudwatch_logs_group,
                            "awslogs-stream-prefix": self.cloudwatch_logs_stream_prefix,
                            "awslogs-create-group": "true",
                        },
                    },
                }
            ],
            volumes=[],
            requiresCompatibilities=["FARGATE"] if self.fargate else [],
            cpu=str(self.worker_cpu),
            memory=str(self.worker_mem),
            tags=dict_to_aws(self.tags),
        )
        weakref.finalize(self, self.delete_worker_task_definition_arn)
        return response["taskDefinition"]["taskDefinitionArn"]

    def delete_worker_task_definition_arn(self):
        self.clients["ecs"].deregister_task_definition(  # TODO Async
            taskDefinition=self.worker_task_definition_arn
        )


class FargateCluster(ECSCluster):
    """Deploy a Dask cluster using Fargate on ECS

    This creates a dask scheduler and workers on a Fargate powered ECS cluster.
    If you do not configure a cluster one will be created for you with sensible
    defaults.

    Parameters
    ----------
    kwargs: dict
        Keyword arguments to be passed to :class:`ECSCluster`.

    """

    def __init__(self, **kwargs):
        super().__init__(fargate=True, **kwargs)


# TODO add cleanup function which can be used to remove stray resources from
# stale clusters.
