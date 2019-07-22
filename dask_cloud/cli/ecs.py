import click


@click.command()
@click.option("--fargate", is_flag=True, help="Turn on fargate mode (default off)")
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
@click.option("--task-role-policy", type=str, default=None, help="")
@click.option("--cloudwatch-logs-group", type=str, default=None, help="")
@click.option("--cloudwatch-logs-stream-prefix", type=str, default=None, help="")
@click.option("--cloudwatch-logs-default-retention", type=int, default=None, help="")
@click.option("--vpc", type=str, default=None, help="")
@click.option("--subnet", type=str, default=None, help="")
@click.option("--security-group", type=str, default=None, help="")
@click.option("--environment", type=str, default=None, help="")
@click.option("--tag", type=str, default=None, help="")
@click.version_option()
def main(n, host, port):
    click.echo("." * n + str(port))


def go():
    main()


if __name__ == "__main__":
    go()
