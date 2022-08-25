import logging
from asyncio import sleep
import sys

import click
from distributed.cli.utils import install_signal_handlers
from distributed.core import Status
from tornado.ioloop import IOLoop, TimeoutError

from dask_cloudprovider.aws import ECSCluster


logger = logging.getLogger(__name__)


@click.command()
@click.option("--fargate", is_flag=True, help="Turn on fargate mode (default off)")
@click.option(
    "--fargate-scheduler",
    is_flag=True,
    help="Turn on fargate mode for scheduler (default off)",
)
@click.option(
    "--fargate-workers",
    is_flag=True,
    help="Turn on fargate mode for workers (default off)",
)
@click.option(
    "--image",
    type=str,
    default=None,
    help="Docker image to use for scheduler and workers",
)
@click.option(
    "--scheduler-cpu",
    type=int,
    default=None,
    help="Scheduler CPU reservation in milli-CPU",
)
@click.option(
    "--scheduler-mem", type=int, default=None, help="Scheduler memory reservation in MB"
)
@click.option(
    "--scheduler-port",
    type=int,
    default=8786,
    help="The port on which the scheduler will be reachable to the workers and clients",
)
@click.option(
    "--scheduler-timeout",
    type=int,
    default=None,
    help="Scheduler timeout (e.g 5 minutes)",
)
@click.option(
    "--worker-cpu", type=int, default=None, help="Worker CPU reservation in milli-CPU"
)
@click.option(
    "--worker-mem", type=int, default=None, help="Worker memory reservation in MB"
)
@click.option(
    "--n-workers",
    type=int,
    default=None,
    help="Number of workers to start with the cluster",
)
@click.option(
    "--cluster-arn",
    type=str,
    default=None,
    help="The ARN of an existing ECS cluster to use",
)
@click.option(
    "--cluster-name-template",
    type=str,
    default=None,
    help="A template to use for the cluster name if `--cluster-arn` is not set",
)
@click.option(
    "--execution-role-arn",
    type=str,
    default=None,
    help="The ARN of an existing IAM role to use for ECS execution",
)
@click.option(
    "--task-role-arn",
    type=str,
    default=None,
    help="The ARN of an existing IAM role to give to the tasks",
)
@click.option(
    "--task-role-policy",
    type=str,
    default=None,
    multiple=True,
    help="Policy to attach to a task if --task-role-arn is not set (can be used multiple times)",
)
@click.option(
    "--cloudwatch-logs-group", type=str, default=None, help="The group to send logs to"
)
@click.option(
    "--cloudwatch-logs-stream-prefix",
    type=str,
    default=None,
    help="An optional prefix to use for log streams",
)
@click.option(
    "--cloudwatch-logs-default-retention",
    type=int,
    default=None,
    help="Number of says to retain logs",
)
@click.option(
    "--vpc",
    type=str,
    default=None,
    help="The ID of an existing VPC (uses default if not specified)",
)
@click.option(
    "--subnet",
    type=str,
    default=None,
    multiple=True,
    help="VPC subnet to use (can be used multipel times, will defaul to all if none specified)",
)
@click.option(
    "--security-group",
    type=str,
    default=None,
    multiple=True,
    help="Security group to use for task communication (can be used multiple times, will be created if not specified)",
)
@click.option(
    "--environment",
    type=str,
    default=None,
    multiple=True,
    help="Environment variable for the scheduler and workers in the form FOO=bar (can be used multiple times)",
)
@click.option(
    "--tag",
    type=str,
    default=None,
    multiple=True,
    help="Tag to apply to all resources created automatically in the form FOO=bar (can be used multiple times)",
)
@click.option("--skip_cleanup", is_flag=True, help="Skip cleanup of stale resources")
@click.version_option()
def main(
    fargate,
    fargate_scheduler,
    fargate_workers,
    image,
    scheduler_cpu,
    scheduler_mem,
    scheduler_port,
    scheduler_timeout,
    worker_cpu,
    worker_mem,
    n_workers,
    cluster_arn,
    cluster_name_template,
    execution_role_arn,
    task_role_arn,
    task_role_policy,
    cloudwatch_logs_group,
    cloudwatch_logs_stream_prefix,
    cloudwatch_logs_default_retention,
    vpc,
    subnet,
    security_group,
    environment,
    tag,
    skip_cleanup,
):
    tag = {v.split("=")[0]: v.split("=")[1] for v in tag} if tag else None
    environment = (
        {v.split("=")[0]: v.split("=")[1] for v in environment} if environment else None
    )
    subnet = subnet or None
    security_group = security_group or None
    task_role_policy = task_role_policy or None
    logger.info("Starting ECS cluster")
    try:
        cluster = ECSCluster(
            fargate_scheduler=fargate_scheduler or fargate,
            fargate_workers=fargate_workers or fargate,
            image=image,
            scheduler_cpu=scheduler_cpu,
            scheduler_mem=scheduler_mem,
            scheduler_port=scheduler_port,
            scheduler_timeout=scheduler_timeout,
            worker_cpu=worker_cpu,
            worker_mem=worker_mem,
            n_workers=n_workers,
            cluster_arn=cluster_arn,
            cluster_name_template=cluster_name_template,
            execution_role_arn=execution_role_arn,
            task_role_arn=task_role_arn,
            task_role_policies=task_role_policy,
            cloudwatch_logs_group=cloudwatch_logs_group,
            cloudwatch_logs_stream_prefix=cloudwatch_logs_stream_prefix,
            cloudwatch_logs_default_retention=cloudwatch_logs_default_retention,
            vpc=vpc,
            subnets=subnet,
            security_groups=security_group,
            environment=environment,
            tags=tag,
            skip_cleanup=skip_cleanup,
        )
    except Exception as e:
        ctx = click.get_current_context()
        logger.error(str(e) + "\n")
        click.echo(ctx.get_help())
        sys.exit(1)

    async def run():
        logger.info("Ready")
        while cluster.status != Status.closed:
            await sleep(0.2)

    def on_signal(signum):
        logger.info("Exiting on signal %d", signum)
        cluster.close(timeout=2)

    loop = IOLoop.current()
    install_signal_handlers(loop, cleanup=on_signal)

    try:
        loop.run_sync(run)
    except (KeyboardInterrupt, TimeoutError):
        logger.info("Shutting down")
    finally:
        logger.info("End dask-ecs")


def go():
    main()


if __name__ == "__main__":
    go()
