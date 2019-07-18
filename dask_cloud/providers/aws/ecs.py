import asyncio
import logging
import sys
import time
import uuid
import weakref

from botocore.exceptions import ClientError
import aiobotocore
import dask

from dask_cloud.providers.aws.helper import dict_to_aws

from distributed.deploy.spec import SpecCluster

logger = logging.getLogger(__name__)


DEFAULT_TAGS = {"createdBy": "dask-cloud"}  # Package tags to apply to all resources


class Task:
    """ A superclass for managing ECS Tasks
    Parameters
    ----------
    clients: Dict[str, aiobotocore.client.Client]
        References to the boto clients created by the cluster. These will be
        used to interact with the AWS API.

    cluster_arn: str
        The ARN of the ECS cluster to launch the task in.

    task_definition_arn: str
        The ARN of the task definition that this object should use to launch
        itself.

    vpc_subnets: List[str]
        The VPC subnets to use for the ENI that will be created when launching
        this task.

    security_groups: List[str]
        The security groups to attach to the ENI that will be created when
        launching this task.

    log_group: str
        The log group to send all task logs to.

    log_stream_prefix: str
        A prefix for the log stream that will be created automatically in the
        log group when launching this task.

    fargate: bool
        Whether or not to launch with the Fargate launch type.

    tags: str
        AWS resource tags to be applied to any resources that are created.

    kwargs:
        Any additional kwargs which may need to be stored for later use.

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
        [self.task] = (
            await self.clients["ecs"].describe_tasks(
                cluster=self.cluster_arn, tasks=[self.task_arn]
            )
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
            await self.clients["ecs"].stop_task(
                cluster=self.cluster_arn, task=self.task_arn
            )
            await self._update_task()
            while self.task["lastStatus"] in ["RUNNING"]:
                await asyncio.sleep(1)
                await self._update_task()
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

    See :class:`Task` for parameter info.
    """


class Worker(Task):
    """ A Remote Dask Worker controled by ECS
    Parameters
    ----------
    scheduler: str
        The address of the scheduler

    kwargs:
        Other kwargs to be passed to :class:`Task`.
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
    task_role_policies: List[str] (optional)
        If you do not specify a ``task_role_arn`` you may want to list some
        IAM Policy ARNs to be attached to the role that will be created for you.

        E.g if you need your workers to read from S3 you could add
        ``arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess``.

        Default ``None`` (no policies will be attached to the role)
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
        task_role_policies=None,
        cloudwatch_logs_group=None,
        cloudwatch_logs_stream_prefix=None,
        cloudwatch_logs_default_retention=None,
        vpc=None,
        security_groups=None,
        tags=None,
        **kwargs
    ):
        self.fargate = fargate
        self.image = image
        self.scheduler_cpu = scheduler_cpu
        self.scheduler_mem = scheduler_mem
        self.scheduler_timeout = scheduler_timeout
        self.worker_cpu = worker_cpu
        self.worker_mem = worker_mem
        self.n_workers = n_workers
        self.cluster_arn = cluster_arn
        self.cluster_name_template = cluster_name_template
        self.execution_role_arn = execution_role_arn
        self.task_role_arn = task_role_arn
        self.task_role_policies = task_role_policies
        self.cloudwatch_logs_group = cloudwatch_logs_group
        self.cloudwatch_logs_stream_prefix = cloudwatch_logs_stream_prefix
        self.cloudwatch_logs_default_retention = cloudwatch_logs_default_retention
        self.vpc = vpc
        self.security_groups = security_groups
        self._tags = tags
        super().__init__(**kwargs)

    async def _start(self,):
        while self.status == "starting":
            await asyncio.sleep(0.01)
        if self.status == "running":
            return
        if self.status == "closed":
            raise ValueError("Cluster is closed")

        self.config = dask.config.get("cloud.ecs", {})
        self.clients = await self.get_clients()
        self.fargate = (
            self.config.get("fargate", False) if self.fargate is None else self.fargate
        )
        self._tags = self.config.get("tags", {}) if self._tags is None else self._tags
        self.image = (
            self.config.get("image", "daskdev/dask:latest")
            if self.image is None
            else self.image
        )
        self.scheduler_cpu = (
            self.config.get("scheduler_cpu", 1024)
            if self.scheduler_cpu is None
            else self.scheduler_cpu
        )
        self.scheduler_mem = (
            self.config.get("scheduler_mem", 4096)
            if self.scheduler_mem is None
            else self.scheduler_mem
        )
        self.scheduler_timeout = (
            self.config.get("scheduler_timeout", "5 minutes")
            if self.scheduler_timeout is None
            else self.scheduler_timeout
        )
        self.worker_cpu = (
            self.config.get("worker_cpu", 4096)
            if self.worker_cpu is None
            else self.worker_cpu
        )
        self.worker_mem = (
            self.config.get("worker_mem", 16384)
            if self.worker_mem is None
            else self.worker_mem
        )
        self.n_workers = (
            self.config.get("n_workers", 0)
            if self.n_workers is None
            else self.n_workers
        )
        self.environment = {
            "DASK_DISTRIBUTED__SCHEDULER__IDLE_TIMEOUT": self.scheduler_timeout
        }

        self.cluster_name = None
        self.cluster_name_template = (
            self.config.get("cluster_name", "dask-{uuid}")
            if self.cluster_name_template is None
            else self.cluster_name_template
        )

        self.cluster_arn = (
            self.config.get("cluster_arn", await self.create_cluster())
            if self.cluster_arn is None
            else self.cluster_arn
        )
        if self.cluster_name is None:
            [cluster_info] = (
                await self.clients["ecs"].describe_clusters(clusters=[self.cluster_arn])
            )["clusters"]
            self.cluster_name = cluster_info["clusterName"]

        self.execution_role_arn = (
            self.config.get("execution_role_arn", await self.create_execution_role())
            if self.execution_role_arn is None
            else self.execution_role_arn
        )
        self.task_role_policies = (
            self.config.get("task_role_policies", [])
            if self.task_role_policies is None
            else self.task_role_policies
        )
        self.task_role_arn = (
            self.config.get("task_role_arn", await self.create_task_role())
            if self.task_role_arn is None
            else self.task_role_arn
        )

        self.cloudwatch_logs_stream_prefix = (
            self.config.get("cloudwatch_logs_stream_prefix", "{cluster_name}")
            if self.cloudwatch_logs_stream_prefix is None
            else self.cloudwatch_logs_stream_prefix
        ).format(cluster_name=self.cluster_name)
        self.cloudwatch_logs_default_retention = (
            self.config.get("cloudwatch_logs_default_retention", 30)
            if self.cloudwatch_logs_default_retention is None
            else self.cloudwatch_logs_default_retention
        )
        self.cloudwatch_logs_group = (
            self.config.get(
                "cloudwatch_logs_group", await self.create_cloudwatch_logs_group()
            )
            if self.cloudwatch_logs_group is None
            else self.cloudwatch_logs_group
        )

        self.vpc = self.config.get("vpc", "default") if self.vpc is None else self.vpc
        if self.vpc == "default":
            self.vpc = await self.get_default_vpc()
        self.vpc_subnets = await self.get_vpc_subnets()

        self.security_groups = (
            self.config.get("security_groups", await self.create_security_groups())
            if self.security_groups is None
            else self.security_groups
        )

        self.scheduler_task_definition_arn = (
            await self.create_scheduler_task_definition_arn()
        )
        self.worker_task_definition_arn = await self.create_worker_task_definition_arn()

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

        self.scheduler_spec = {"cls": Scheduler, "options": scheduler_options}
        self.new_spec = {"cls": Worker, "options": worker_options}
        self.worker_spec = {i: self.new_spec for i in range(self.n_workers)}
        await super()._start()

    @property
    def tags(self):
        return {**self._tags, **DEFAULT_TAGS}

    async def get_clients(self):
        session = aiobotocore.get_session()
        return {
            "ec2": session.create_client("ec2"),
            "ecs": session.create_client("ecs"),
            "iam": session.create_client("iam"),
            "logs": session.create_client("logs"),
        }

    async def _close_clients(self):
        pass

    async def create_cluster(self):
        if not self.fargate:
            raise RuntimeError("You must specify a cluster when not using Fargate.")
        self.cluster_name = dask.config.expand_environment_variables(
            self.cluster_name_template
        )
        self.cluster_name = self.cluster_name.format(uuid=str(uuid.uuid4())[:10])
        response = await self.clients["ecs"].create_cluster(
            clusterName=self.cluster_name, tags=dict_to_aws(self.tags)
        )
        weakref.finalize(self, self.sync, self.delete_cluster)
        return response["cluster"]["clusterArn"]

    async def delete_cluster(self):
        async for page in self.clients["ecs"].get_paginator("list_tasks").paginate(
            cluster=self.cluster_arn, desiredStatus="RUNNING"
        ):
            for task in page["taskArns"]:
                await self.clients["ecs"].stop_task(cluster=self.cluster_arn, task=task)
        await self.clients["ecs"].delete_cluster(cluster=self.cluster_arn)

    @property
    def execution_role_name(self):
        return "{}-{}".format(self.cluster_name, "execution-role")

    async def create_execution_role(self):
        response = await self.clients["iam"].create_role(
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
        await self.clients["iam"].attach_role_policy(
            RoleName=self.execution_role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
        )
        await self.clients["iam"].attach_role_policy(
            RoleName=self.execution_role_name,
            PolicyArn="arn:aws:iam::aws:policy/CloudWatchLogsFullAccess",
        )
        await self.clients["iam"].attach_role_policy(
            RoleName=self.execution_role_name,
            PolicyArn="arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceRole",
        )
        weakref.finalize(self, self.sync, self.delete_execution_role)
        return response["Role"]["Arn"]

    async def delete_execution_role(self):
        attached_policies = (
            await self.clients["iam"].list_attached_role_policies(
                RoleName=self.execution_role_name
            )
        )["AttachedPolicies"]
        for policy in attached_policies:
            await self.clients["iam"].detach_role_policy(
                RoleName=self.execution_role_name, PolicyArn=policy["PolicyArn"]
            )
        await self.clients["iam"].delete_role(RoleName=self.execution_role_name)

    @property
    def task_role_name(self):
        return "{}-{}".format(self.cluster_name, "task-role")

    async def create_task_role(self):
        response = await self.clients["iam"].create_role(
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

        for policy in self.task_role_policies:
            await self.clients["iam"].attach_role_policy(
                RoleName=self.task_role_name, PolicyArn=policy
            )

        weakref.finalize(self, self.sync, self.delete_task_role)
        return response["Role"]["Arn"]

    async def delete_task_role(self):  # TODO combine with delete execution role
        attached_policies = (
            await self.clients["iam"].list_attached_role_policies(
                RoleName=self.task_role_name
            )
        )["AttachedPolicies"]
        for policy in attached_policies:
            await self.clients["iam"].detach_role_policy(
                RoleName=self.task_role_name, PolicyArn=policy["PolicyArn"]
            )
        await self.clients["iam"].delete_role(RoleName=self.task_role_name)

    async def create_cloudwatch_logs_group(self):
        log_group_name = "dask-ecs"
        if log_group_name not in [
            group["logGroupName"]
            for group in (await self.clients["logs"].describe_log_groups())["logGroups"]
        ]:
            await self.clients["logs"].create_log_group(
                logGroupName=log_group_name, tags=self.tags
            )
            await self.clients["logs"].put_retention_policy(
                logGroupName=log_group_name,
                retentionInDays=self.cloudwatch_logs_default_retention,
            )
        # Note: Not cleaning up the logs here as they may be useful after the cluster is destroyed
        return log_group_name

    async def get_default_vpc(self):
        vpcs = (await self.clients["ec2"].describe_vpcs())["Vpcs"]
        [vpc] = [vpc for vpc in vpcs if vpc["IsDefault"]]
        return vpc["VpcId"]

    async def get_vpc_subnets(self):
        vpcs = (await self.clients["ec2"].describe_vpcs())["Vpcs"]
        [vpc] = [vpc for vpc in vpcs if vpc["VpcId"] == self.vpc]
        subnets = (await self.clients["ec2"].describe_subnets())["Subnets"]
        return [
            subnet["SubnetId"] for subnet in subnets if subnet["VpcId"] == vpc["VpcId"]
        ]

    async def create_security_groups(self):
        response = await self.clients["ec2"].create_security_group(
            Description="A security group for dask-ecs",
            GroupName=self.cluster_name,
            VpcId=self.vpc,
            DryRun=False,
        )
        await self.clients["ec2"].authorize_security_group_ingress(
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
        await self.clients["ec2"].create_tags(
            Resources=[response["GroupId"]], Tags=dict_to_aws(self.tags, upper=True)
        )
        weakref.finalize(self, self.sync, self.delete_security_groups)
        return [response["GroupId"]]

    async def delete_security_groups(self):
        # TODO fix this deletion. Still doesn't seem to work.
        retries = 15
        while True:
            try:
                await self.clients["ec2"].delete_security_group(
                    GroupName=self.cluster_name, DryRun=False
                )
            except Exception as e:
                retries -= 1
                if retries > 0:
                    await asyncio.sleep(2)
                    continue
                else:
                    raise e
            break

    async def create_scheduler_task_definition_arn(self):
        response = await self.clients["ecs"].register_task_definition(
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
                            "awslogs-region": self.clients["ecs"].meta.region_name,
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
        weakref.finalize(self, self.sync, self.delete_scheduler_task_definition_arn)
        return response["taskDefinition"]["taskDefinitionArn"]

    async def delete_scheduler_task_definition_arn(self):
        await self.clients["ecs"].deregister_task_definition(
            taskDefinition=self.scheduler_task_definition_arn
        )

    async def create_worker_task_definition_arn(self):
        response = await self.clients["ecs"].register_task_definition(
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
                        "--memory-limit",
                        "{}GB".format(int(self.worker_mem / 1024)),
                        "--death-timeout",
                        "60",
                    ],
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-region": self.clients["ecs"].meta.region_name,
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
        weakref.finalize(self, self.sync, self.delete_worker_task_definition_arn)
        return response["taskDefinition"]["taskDefinitionArn"]

    async def delete_worker_task_definition_arn(self):
        await self.clients["ecs"].deregister_task_definition(
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

# TODO awaiting the cluster class seems to hang forever

# TODO consolidate finalization tasks and also close the clients

# TODO catch all credential errors.
#      Not all users will be able to create all the resources necessary for a default cluster.
#      We should catch any permissions errors that come back from AWS and cleanly tear everything back down and raise.
