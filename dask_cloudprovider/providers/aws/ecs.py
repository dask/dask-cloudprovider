import asyncio
import logging
import sys
import time
import uuid
import warnings
import weakref

from botocore.exceptions import ClientError
import aiobotocore
import dask

from dask_cloudprovider.utils.logs import Log, Logs
from dask_cloudprovider.utils.timeout import Timeout
from dask_cloudprovider.providers.aws.helper import dict_to_aws, aws_to_dict

from distributed.deploy.spec import SpecCluster
from distributed.utils import warn_on_duration

logger = logging.getLogger(__name__)


DEFAULT_TAGS = {
    "createdBy": "dask-cloudprovider"
}  # Package tags to apply to all resources


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

    environment: dict
        Environment variables to set when launching the task.

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
        environment,
        tags,
        name=None,
        **kwargs
    ):
        self.lock = asyncio.Lock()
        self.name = name
        self.address = None
        self.external_address = None
        self._clients = clients
        self.cluster_arn = cluster_arn
        self.task_definition_arn = task_definition_arn
        self.task = None
        self.task_arn = None
        self.task_type = None
        self.public_ip = None
        self.private_ip = None
        self.log_group = log_group
        self.log_stream_prefix = log_stream_prefix
        self.connection = None
        self._overrides = {}
        self._vpc_subnets = vpc_subnets
        self._security_groups = security_groups
        self.fargate = fargate
        self.environment = environment or {}
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

    @property
    def _use_public_ip(self):
        # Fargate needs public IP for image pull, EC2 doesn't support public IP, therefore
        # we will assume for now that we will use a public IP when in Fargate mode and not
        # when in EC2 mode.

        # TODO Fargate can also use a NAT to pull the image so we could allow this to be false
        # when in Fargate provided there is a NAT

        return self.fargate

    async def _is_long_arn_format_enabled(self):
        [response] = (
            await self._clients["ecs"].list_account_settings(
                name="taskLongArnFormat", effectiveSettings=True
            )
        )["settings"]
        return response["value"] == "enabled"

    async def _update_task(self):
        [self.task] = (
            await self._clients["ecs"].describe_tasks(
                cluster=self.cluster_arn, tasks=[self.task_arn]
            )
        )["tasks"]

    async def _set_address_from_logs(self):
        timeout = Timeout(
            30, "Failed to find %s ip address after 30 seconds." % self.task_type
        )
        while timeout.run():
            async for line in self.logs():
                for query_string in ["worker at:", "Scheduler at:"]:
                    if query_string in line:
                        address = line.split(query_string)[1].strip()
                        if self._use_public_ip:
                            self.external_address = address.replace(
                                self.private_ip, self.public_ip
                            )
                        logger.debug("%s", line)
                        self.address = address
                        return
            else:
                if not await self._task_is_running():
                    raise RuntimeError("%s exited unexpectedly!" % type(self).__name__)
                continue
            break

    async def _task_is_running(self):
        await self._update_task()
        return self.task["lastStatus"] == "RUNNING"

    async def start(self):
        timeout = Timeout(60, "Unable to start %s after 60 seconds" % self.task_type)
        while timeout.run():
            try:
                kwargs = (
                    {"tags": dict_to_aws(self.tags)}
                    if await self._is_long_arn_format_enabled()
                    else {}
                )  # Tags are only supported if you opt into long arn format so we need to check for that
                response = await self._clients["ecs"].run_task(
                    cluster=self.cluster_arn,
                    taskDefinition=self.task_definition_arn,
                    overrides={
                        "containerOverrides": [
                            {
                                "name": "dask-{}".format(self.task_type),
                                "environment": dict_to_aws(
                                    self.environment, key_string="name"
                                ),
                                **self._overrides,
                            }
                        ]
                    },
                    count=1,
                    launchType="FARGATE" if self.fargate else "EC2",
                    networkConfiguration={
                        "awsvpcConfiguration": {
                            "subnets": self._vpc_subnets,
                            "securityGroups": self._security_groups,
                            "assignPublicIp": "ENABLED"
                            if self._use_public_ip
                            else "DISABLED",
                        }
                    },
                    **kwargs
                )

                if not response.get("tasks"):
                    raise RuntimeError(response)  # print entire response

                [self.task] = response["tasks"]
                break
            except Exception as e:
                timeout.set_exception(e)
                await asyncio.sleep(1)

        self.task_arn = self.task["taskArn"]
        while self.task["lastStatus"] in ["PENDING", "PROVISIONING"]:
            await asyncio.sleep(1)
            await self._update_task()
        if not await self._task_is_running():
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
        eni = await self._clients["ec2"].describe_network_interfaces(
            NetworkInterfaceIds=[network_interface_id]
        )
        [interface] = eni["NetworkInterfaces"]
        if self._use_public_ip:
            self.public_ip = interface["Association"]["PublicIp"]
        self.private_ip = interface["PrivateIpAddresses"][0]["PrivateIpAddress"]
        await self._set_address_from_logs()
        self.status = "running"

    async def close(self, **kwargs):
        if self.task:
            await self._clients["ecs"].stop_task(
                cluster=self.cluster_arn, task=self.task_arn
            )
            await self._update_task()
            while self.task["lastStatus"] in ["RUNNING"]:
                await asyncio.sleep(1)
                await self._update_task()
        self.status = "closed"

    @property
    def task_id(self):
        return self.task_arn.split("/")[-1]

    @property
    def _log_stream_name(self):
        return "{prefix}/{container}/{task_id}".format(
            prefix=self.log_stream_prefix,
            container=self.task["containers"][0]["name"],
            task_id=self.task_id,
        )

    async def logs(self, follow=False):
        next_token = None
        read_from = 0
        while True:
            if next_token:
                l = await self._clients["logs"].get_log_events(
                    logGroupName=self.log_group,
                    logStreamName=self._log_stream_name,
                    nextToken=next_token,
                )
            else:
                l = await self._clients["logs"].get_log_events(
                    logGroupName=self.log_group,
                    logStreamName=self._log_stream_name,
                    startTime=read_from,
                )
            if next_token != l["nextForwardToken"]:
                next_token = l["nextForwardToken"]
            else:
                next_token = None
            if not l["events"]:
                if follow:
                    await asyncio.sleep(1)
                else:
                    break
            for event in l["events"]:
                read_from = event["timestamp"]
                yield event["message"]

    def __repr__(self):
        return "<ECS Task %s: status=%s>" % (type(self).__name__, self.status)


class Scheduler(Task):
    """ A Remote Dask Scheduler controled by ECS

    See :class:`Task` for parameter info.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.task_type = "scheduler"


class Worker(Task):
    """ A Remote Dask Worker controled by ECS
    Parameters
    ----------
    scheduler: str
        The address of the scheduler

    kwargs: Dict()
        Other kwargs to be passed to :class:`Task`.
    """

    def __init__(self, scheduler: str, cpu: int, mem: int, gpu: int, **kwargs):
        super().__init__(**kwargs)
        self.task_type = "worker"
        self.scheduler = scheduler
        self._cpu = cpu
        self._mem = mem
        self._gpu = gpu
        self._overrides = {
            "command": [
                "dask-cuda-worker" if self._gpu else "dask-worker",
                self.scheduler,
                "--name",
                str(self.name),
                "--nthreads",
                "{}".format(int(self._cpu / 1024)),
                "--memory-limit",
                "{}GB".format(int(self._mem / 1024)),
                "--death-timeout",
                "60",
            ]
        }


class ECSCluster(SpecCluster):
    """ Deploy a Dask cluster using ECS

    This creates a dask scheduler and workers on an ECS cluster. If you do not
    configure a cluster one will be created for you with sensible defaults.

    Parameters
    ----------
    fargate_scheduler: bool (optional)
        Select whether or not to use fargate for the scheduler.

        Defaults to ``False``. You must provide an existing cluster.
    fargate_workers: bool (optional)
        Select whether or not to use fargate for the workers.

        Defaults to ``False``. You must provide an existing cluster.
    image: str (optional)
        The docker image to use for the scheduler and worker tasks.

        Defaults to ``daskdev/dask:latest`` or ``rapidsai/rapidsai:latest`` if ``worker_gpu`` is set.
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
    worker_gpu: int (optional)
        The number of GPUs to expose to the worker.

        To provide GPUs to workers you need to use a GPU ready docker image
        that has ``dask-cuda`` installed and GPU nodes available in your ECS
        cluster. Fargate is not supported at this time.

        Defaults to `None`, no GPUs.
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
    subnets: List[str] (optional)
        A list of subnets to use when running your task.

        Defaults to ``None``. (all subnets available in your VPC will be used)
    security_groups: List[str] (optional)
        A list of security group IDs to use when launching tasks.

        Defaults to ``None`` (one will be created which allows all traffic
        between tasks and access to ports ``8786`` and ``8787`` from anywhere).
    environment: dict (optional)
        Extra environment variables to pass to the scheduler and worker tasks.

        Useful for setting ``EXTRA_APT_PACKAGES``, ``EXTRA_CONDA_PACKAGES`` and
        ```EXTRA_PIP_PACKAGES`` if you'ree using the default image.

        Defaults to ``None``.
    tags: dict (optional)
        Tags to apply to all resources created automatically.

        Defaults to ``None``. Tags will always include ``{"createdBy": "dask-cloudprovider"}``
    skip_cleanup: bool (optional)
        Skip cleaning up of stale resources. Useful if you have lots of resources
        and this operation takes a while.

        Default ``False``.
    **kwargs: dict
        Additional keyword arguments to pass to ``SpecCluster``.

    Examples
    --------

    """

    def __init__(
        self,
        fargate_scheduler=False,
        fargate_workers=False,
        image=None,
        scheduler_cpu=None,
        scheduler_mem=None,
        scheduler_timeout=None,
        worker_cpu=None,
        worker_mem=None,
        worker_gpu=None,
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
        subnets=None,
        security_groups=None,
        environment=None,
        tags=None,
        skip_cleanup=None,
        **kwargs
    ):
        self._clients = None
        self._fargate_scheduler = fargate_scheduler
        self._fargate_workers = fargate_workers
        self.image = image
        self._scheduler_cpu = scheduler_cpu
        self._scheduler_mem = scheduler_mem
        self._scheduler_timeout = scheduler_timeout
        self._worker_cpu = worker_cpu
        self._worker_mem = worker_mem
        self._worker_gpu = worker_gpu
        self._n_workers = n_workers
        self.cluster_arn = cluster_arn
        self.cluster_name = None
        self._cluster_name_template = cluster_name_template
        self._execution_role_arn = execution_role_arn
        self._task_role_arn = task_role_arn
        self._task_role_policies = task_role_policies
        self.cloudwatch_logs_group = cloudwatch_logs_group
        self._cloudwatch_logs_stream_prefix = cloudwatch_logs_stream_prefix
        self._cloudwatch_logs_default_retention = cloudwatch_logs_default_retention
        self._vpc = vpc
        self._vpc_subnets = subnets
        self._security_groups = security_groups
        self._environment = environment
        self._tags = tags
        self._skip_cleanup = skip_cleanup
        self._lock = asyncio.Lock()
        super().__init__(**kwargs)

    async def _start(self,):
        while self.status == "starting":
            await asyncio.sleep(0.01)
        if self.status == "running":
            return
        if self.status == "closed":
            raise ValueError("Cluster is closed")

        self.config = dask.config.get("cloudprovider.ecs", {})

        # Cleanup any stale resources before we start
        if self._skip_cleanup is None:
            self._skip_cleanup = self.config.get("skip_cleanup")
        if not self._skip_cleanup:
            await _cleanup_stale_resources()

        self._clients = await self._get_clients()

        if self._fargate_scheduler is None:
            self._fargate_scheduler = self.config.get("fargate_scheduler")
        if self._fargate_workers is None:
            self._fargate_workers = self.config.get("fargate_workers")

        if self._tags is None:
            self._tags = self.config.get("tags")

        if self._environment is None:
            self._environment = self.config.get("environment")

        if self._worker_gpu is None:
            self._worker_gpu = self.config.get(
                "worker_gpu"
            )  # TODO Detect whether cluster is GPU capable

        if self.image is None:
            if self._worker_gpu:
                self.image = self.config.get("gpu_image")
            else:
                self.image = self.config.get("image")

        if self._scheduler_cpu is None:
            self._scheduler_cpu = self.config.get("scheduler_cpu")

        if self._scheduler_mem is None:
            self._scheduler_mem = self.config.get("scheduler_mem")

        if self._scheduler_timeout is None:
            self._scheduler_timeout = self.config.get("scheduler_timeout")

        if self._worker_cpu is None:
            self._worker_cpu = self.config.get("worker_cpu")

        if self._worker_mem is None:
            self._worker_mem = self.config.get("worker_mem")

        if self._n_workers is None:
            self._n_workers = self.config.get("n_workers")

        if self._cluster_name_template is None:
            self._cluster_name_template = self.config.get("cluster_name_template")

        if self.cluster_arn is None:
            self.cluster_arn = (
                self.config.get("cluster_arn") or await self._create_cluster()
            )

        if self.cluster_name is None:
            [cluster_info] = (
                await self._clients["ecs"].describe_clusters(
                    clusters=[self.cluster_arn]
                )
            )["clusters"]
            self.cluster_name = cluster_info["clusterName"]

        if self._execution_role_arn is None:
            self._execution_role_arn = (
                self.config.get("execution_role_arn")
                or await self._create_execution_role()
            )

        if self._task_role_policies is None:
            self._task_role_policies = self.config.get("task_role_policies")

        if self._task_role_arn is None:
            self._task_role_arn = (
                self.config.get("task_role_arn") or await self._create_task_role()
            )

        if self._cloudwatch_logs_stream_prefix is None:
            self._cloudwatch_logs_stream_prefix = self.config.get(
                "cloudwatch_logs_stream_prefix"
            ).format(cluster_name=self.cluster_name)

        if self._cloudwatch_logs_default_retention is None:
            self._cloudwatch_logs_default_retention = self.config.get(
                "cloudwatch_logs_default_retention"
            )

        if self.cloudwatch_logs_group is None:
            self.cloudwatch_logs_group = (
                self.config.get("cloudwatch_logs_group")
                or await self._create_cloudwatch_logs_group()
            )

        if self._vpc is None:
            self._vpc = self.config.get("vpc")

        if self._vpc == "default":
            self._vpc = await self._get_default_vpc()

        if self._vpc_subnets is None:
            self._vpc_subnets = (
                self.config.get("subnets") or await self._get_vpc_subnets()
            )

        if self._security_groups is None:
            self._security_groups = (
                self.config.get("security_groups")
                or await self._create_security_groups()
            )

        self.scheduler_task_definition_arn = (
            await self._create_scheduler_task_definition_arn()
        )
        self.worker_task_definition_arn = (
            await self._create_worker_task_definition_arn()
        )

        options = {
            "clients": self._clients,
            "cluster_arn": self.cluster_arn,
            "vpc_subnets": self._vpc_subnets,
            "security_groups": self._security_groups,
            "log_group": self.cloudwatch_logs_group,
            "log_stream_prefix": self._cloudwatch_logs_stream_prefix,
            "environment": self._environment,
            "tags": self.tags,
        }
        scheduler_options = {
            "task_definition_arn": self.scheduler_task_definition_arn,
            "fargate": self._fargate_scheduler,
            **options,
        }
        worker_options = {
            "task_definition_arn": self.worker_task_definition_arn,
            "fargate": self._fargate_workers,
            "cpu": self._worker_cpu,
            "mem": self._worker_mem,
            "gpu": self._worker_gpu,
            **options,
        }

        self.scheduler_spec = {"cls": Scheduler, "options": scheduler_options}
        self.new_spec = {"cls": Worker, "options": worker_options}
        self.worker_spec = {i: self.new_spec for i in range(self._n_workers)}

        with warn_on_duration(
            "10s",
            "Creating your cluster is taking a surprisingly long time. "
            "This is likely due to pending resources on AWS. "
            "Hang tight! ",
        ):
            await super()._start()

    @property
    def tags(self):
        return {**self._tags, **DEFAULT_TAGS, "cluster": self.cluster_name}

    async def _get_clients(self):
        session = aiobotocore.get_session()
        weakref.finalize(self, self.sync, self._close_clients)
        return {
            "ec2": session.create_client("ec2"),
            "ecs": session.create_client("ecs"),
            "iam": session.create_client("iam"),
            "logs": session.create_client("logs"),
        }

    async def _close_clients(self):
        for client in self._clients.values():
            await client.close()

    async def _create_cluster(self):
        if not self._fargate_scheduler or not self._fargate_workers:
            raise RuntimeError("You must specify a cluster when not using Fargate.")
        if self._worker_gpu:
            raise RuntimeError(
                "It is not possible to use GPUs with Fargate. "
                "Please provide an existing cluster with GPU instances available. "
            )
        self.cluster_name = dask.config.expand_environment_variables(
            self._cluster_name_template
        )
        self.cluster_name = self.cluster_name.format(uuid=str(uuid.uuid4())[:10])
        response = await self._clients["ecs"].create_cluster(
            clusterName=self.cluster_name, tags=dict_to_aws(self.tags)
        )
        weakref.finalize(self, self.sync, self._delete_cluster)
        return response["cluster"]["clusterArn"]

    async def _delete_cluster(self):
        async for page in self._clients["ecs"].get_paginator("list_tasks").paginate(
            cluster=self.cluster_arn, desiredStatus="RUNNING"
        ):
            for task in page["taskArns"]:
                await self._clients["ecs"].stop_task(
                    cluster=self.cluster_arn, task=task
                )
        await self._clients["ecs"].delete_cluster(cluster=self.cluster_arn)

    @property
    def _execution_role_name(self):
        return "{}-{}".format(self.cluster_name, "execution-role")

    async def _create_execution_role(self):
        response = await self._clients["iam"].create_role(
            RoleName=self._execution_role_name,
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
        await self._clients["iam"].attach_role_policy(
            RoleName=self._execution_role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
        )
        await self._clients["iam"].attach_role_policy(
            RoleName=self._execution_role_name,
            PolicyArn="arn:aws:iam::aws:policy/CloudWatchLogsFullAccess",
        )
        await self._clients["iam"].attach_role_policy(
            RoleName=self._execution_role_name,
            PolicyArn="arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceRole",
        )
        weakref.finalize(self, self.sync, self._delete_role, self._execution_role_name)
        return response["Role"]["Arn"]

    @property
    def _task_role_name(self):
        return "{}-{}".format(self.cluster_name, "task-role")

    async def _create_task_role(self):
        response = await self._clients["iam"].create_role(
            RoleName=self._task_role_name,
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

        for policy in self._task_role_policies:
            await self._clients["iam"].attach_role_policy(
                RoleName=self._task_role_name, PolicyArn=policy
            )

        weakref.finalize(self, self.sync, self._delete_role, self._task_role_name)
        return response["Role"]["Arn"]

    async def _delete_role(self, role):
        attached_policies = (
            await self._clients["iam"].list_attached_role_policies(RoleName=role)
        )["AttachedPolicies"]
        for policy in attached_policies:
            await self._clients["iam"].detach_role_policy(
                RoleName=role, PolicyArn=policy["PolicyArn"]
            )
        await self._clients["iam"].delete_role(RoleName=role)

    async def _create_cloudwatch_logs_group(self):
        log_group_name = "dask-ecs"
        if log_group_name not in [
            group["logGroupName"]
            for group in (await self._clients["logs"].describe_log_groups())[
                "logGroups"
            ]
        ]:
            await self._clients["logs"].create_log_group(
                logGroupName=log_group_name, tags=self.tags
            )
            await self._clients["logs"].put_retention_policy(
                logGroupName=log_group_name,
                retentionInDays=self._cloudwatch_logs_default_retention,
            )
        # Note: Not cleaning up the logs here as they may be useful after the cluster is destroyed
        return log_group_name

    async def _get_default_vpc(self):
        vpcs = (await self._clients["ec2"].describe_vpcs())["Vpcs"]
        [vpc] = [vpc for vpc in vpcs if vpc["IsDefault"]]
        return vpc["VpcId"]

    async def _get_vpc_subnets(self):
        vpcs = (await self._clients["ec2"].describe_vpcs())["Vpcs"]
        [vpc] = [vpc for vpc in vpcs if vpc["VpcId"] == self._vpc]
        subnets = (await self._clients["ec2"].describe_subnets())["Subnets"]
        return [
            subnet["SubnetId"] for subnet in subnets if subnet["VpcId"] == vpc["VpcId"]
        ]

    async def _create_security_groups(self):
        response = await self._clients["ec2"].create_security_group(
            Description="A security group for dask-ecs",
            GroupName=self.cluster_name,
            VpcId=self._vpc,
            DryRun=False,
        )
        await self._clients["ec2"].authorize_security_group_ingress(
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
        await self._clients["ec2"].create_tags(
            Resources=[response["GroupId"]], Tags=dict_to_aws(self.tags, upper=True)
        )
        weakref.finalize(self, self.sync, self._delete_security_groups)
        return [response["GroupId"]]

    async def _delete_security_groups(self):
        timeout = Timeout(
            30, "Unable to delete AWS security group " + self.cluster_name, warn=True
        )
        while timeout.run():
            try:
                await self._clients["ec2"].delete_security_group(
                    GroupName=self.cluster_name, DryRun=False
                )
            except Exception:
                await asyncio.sleep(2)
            break

    async def _create_scheduler_task_definition_arn(self):
        response = await self._clients["ecs"].register_task_definition(
            family="{}-{}".format(self.cluster_name, "scheduler"),
            taskRoleArn=self._task_role_arn,
            executionRoleArn=self._execution_role_arn,
            networkMode="awsvpc",
            containerDefinitions=[
                {
                    "name": "dask-scheduler",
                    "image": self.image,
                    "cpu": self._scheduler_cpu,
                    "memory": self._scheduler_mem,
                    "memoryReservation": self._scheduler_mem,
                    "essential": True,
                    "command": [
                        "dask-scheduler",
                        "--idle-timeout",
                        self._scheduler_timeout,
                    ],
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-region": self._clients["ecs"].meta.region_name,
                            "awslogs-group": self.cloudwatch_logs_group,
                            "awslogs-stream-prefix": self._cloudwatch_logs_stream_prefix,
                            "awslogs-create-group": "true",
                        },
                    },
                }
            ],
            volumes=[],
            requiresCompatibilities=["FARGATE"] if self._fargate_scheduler else [],
            cpu=str(self._scheduler_cpu),
            memory=str(self._scheduler_mem),
            tags=dict_to_aws(self.tags),
        )
        weakref.finalize(self, self.sync, self._delete_scheduler_task_definition_arn)
        return response["taskDefinition"]["taskDefinitionArn"]

    async def _delete_scheduler_task_definition_arn(self):
        await self._clients["ecs"].deregister_task_definition(
            taskDefinition=self.scheduler_task_definition_arn
        )

    async def _create_worker_task_definition_arn(self):
        resource_requirements = []
        if self._worker_gpu:
            resource_requirements.append(
                {"type": "GPU", "value": str(self._worker_gpu)}
            )
        response = await self._clients["ecs"].register_task_definition(
            family="{}-{}".format(self.cluster_name, "worker"),
            taskRoleArn=self._task_role_arn,
            executionRoleArn=self._execution_role_arn,
            networkMode="awsvpc",
            containerDefinitions=[
                {
                    "name": "dask-worker",
                    "image": self.image,
                    "cpu": self._worker_cpu,
                    "memory": self._worker_mem,
                    "memoryReservation": self._worker_mem,
                    "resourceRequirements": resource_requirements,
                    "essential": True,
                    "command": [
                        "dask-cuda-worker" if self._worker_gpu else "dask-worker",
                        "--nthreads",
                        "{}".format(int(self._worker_cpu / 1024)),
                        "--memory-limit",
                        "{}MB".format(int(self._worker_mem)),
                        "--death-timeout",
                        "60",
                    ],
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-region": self._clients["ecs"].meta.region_name,
                            "awslogs-group": self.cloudwatch_logs_group,
                            "awslogs-stream-prefix": self._cloudwatch_logs_stream_prefix,
                            "awslogs-create-group": "true",
                        },
                    },
                }
            ],
            volumes=[],
            requiresCompatibilities=["FARGATE"] if self._fargate_workers else [],
            cpu=str(self._worker_cpu),
            memory=str(self._worker_mem),
            tags=dict_to_aws(self.tags),
        )
        weakref.finalize(self, self.sync, self._delete_worker_task_definition_arn)
        return response["taskDefinition"]["taskDefinitionArn"]

    async def _delete_worker_task_definition_arn(self):
        await self._clients["ecs"].deregister_task_definition(
            taskDefinition=self.worker_task_definition_arn
        )

    def logs(self):
        async def get_logs(task):
            log = ""
            async for line in task.logs():
                log += "{}\n".format(line)
            return Log(log)

        scheduler_logs = {
            "scheduler - {}".format(self.scheduler.address): self.sync(
                get_logs, self.scheduler
            )
        }
        worker_logs = {
            "worker {} - {}".format(key, worker.address): self.sync(get_logs, worker)
            for key, worker in self.workers.items()
        }
        return Logs({**scheduler_logs, **worker_logs})


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
        super().__init__(fargate_scheduler=True, fargate_workers=True, **kwargs)


async def _cleanup_stale_resources():
    """ Clean up any stale resources which are tagged with 'createdBy': 'dask-cloudprovider'.

    This function will scan through AWS looking for resources that were created
    by the ``ECSCluster`` class. Any ECS clusters which do not have any running
    tasks will be deleted and then any supporting resources such as task definitions
    security groups and IAM roles that are not associated with an active cluster
    will also be deleted.

    The ``ECSCluster`` should clean up after itself when it is garbage collected
    however if the Python process is terminated without notice this may not happen.
    Therefore this is useful to remove shrapnel from past failures.

    """
    # Clean up clusters (clusters with no running tasks)
    session = aiobotocore.get_session()
    async with session.create_client("ecs") as ecs:
        active_clusters = []
        clusters_to_delete = []
        async for page in ecs.get_paginator("list_clusters").paginate():
            clusters = (
                await ecs.describe_clusters(
                    clusters=page["clusterArns"], include=["TAGS"]
                )
            )["clusters"]
            for cluster in clusters:
                if DEFAULT_TAGS.items() <= aws_to_dict(cluster["tags"]).items():
                    if cluster["runningTasksCount"] == 0:
                        clusters_to_delete.append(cluster["clusterArn"])
                    else:
                        active_clusters.append(cluster["clusterName"])
        for cluster_arn in clusters_to_delete:
            await ecs.delete_cluster(cluster=cluster_arn)

        # Clean up task definitions (with no active clusters)
        async for page in ecs.get_paginator("list_task_definitions").paginate():
            for task_definition_arn in page["taskDefinitionArns"]:
                response = await ecs.describe_task_definition(
                    taskDefinition=task_definition_arn, include=["TAGS"]
                )
                task_definition = response["taskDefinition"]
                task_definition["tags"] = response["tags"]
                task_definition_cluster = aws_to_dict(task_definition["tags"]).get(
                    "cluster"
                )
                if (
                    task_definition_cluster is None
                    or task_definition_cluster not in active_clusters
                ):
                    await ecs.deregister_task_definition(
                        taskDefinition=task_definition_arn
                    )

    # Clean up security groups (with no active clusters)
    async with session.create_client("ec2") as ec2:
        async for page in ec2.get_paginator("describe_security_groups").paginate(
            Filters=[{"Name": "tag:createdBy", "Values": ["dask-cloudprovider"]}]
        ):
            for group in page["SecurityGroups"]:
                sg_cluster = aws_to_dict(group["Tags"]).get("cluster")
                if sg_cluster is None or sg_cluster not in active_clusters:
                    await ec2.delete_security_group(
                        GroupName=group["GroupName"], DryRun=False
                    )

    # Clean up roles (with no active clusters)
    async with session.create_client("iam") as iam:
        async for page in iam.get_paginator("list_roles").paginate():
            for role in page["Roles"]:
                role["Tags"] = (
                    await iam.list_role_tags(RoleName=role["RoleName"])
                ).get("Tags")
                if DEFAULT_TAGS.items() <= aws_to_dict(role["Tags"]).items():
                    role_cluster = aws_to_dict(role["Tags"]).get("cluster")
                    if role_cluster is None or role_cluster not in active_clusters:
                        attached_policies = (
                            await iam.list_attached_role_policies(
                                RoleName=role["RoleName"]
                            )
                        )["AttachedPolicies"]
                        for policy in attached_policies:
                            await iam.detach_role_policy(
                                RoleName=role["RoleName"], PolicyArn=policy["PolicyArn"]
                            )
                        await iam.delete_role(RoleName=role["RoleName"])
