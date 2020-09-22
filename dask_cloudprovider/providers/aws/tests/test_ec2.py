import asyncio
import pytest

import aiobotocore

from dask_cloudprovider.providers.aws.ec2 import EC2Cluster
from dask_cloudprovider.providers.aws.helper import get_latest_ami_id
from dask.distributed import Client
import dask.array as da
from distributed.core import Status


async def skip_without_credentials():
    try:
        async with aiobotocore.get_session().create_client("sts") as client:
            await client.get_caller_identity()
    except:
        pytest.skip(
            """
        You must configure Your AWS credentials to run this test.

            $ aws configure

        """
        )


@pytest.fixture
async def cluster():
    await skip_without_credentials()
    async with EC2Cluster(asynchronous=True) as cluster:
        yield cluster


@pytest.fixture
async def cluster_rapids():
    await skip_without_credentials()
    async with EC2Cluster(
        asynchronous=True,
        ami="ami-0c7c7d78f752f8f17",  # Deep Learning AMI (Ubuntu 18.04)
        docker_image="rapidsai/rapidsai:cuda10.1-runtime-ubuntu18.04-py3.8",  # Python version must match local version and CUDA version must match AMI CUDA version
        instance_type="p3.2xlarge",
        bootstrap=False,
        filesystem_size=120,
    ) as cluster:
        yield cluster


@pytest.fixture
async def ec2_client():
    await skip_without_credentials()
    async with aiobotocore.get_session().create_client("ec2") as client:
        yield client


@pytest.mark.asyncio
async def test_init():
    cluster = EC2Cluster(asynchronous=True)
    assert cluster.status == Status.created


@pytest.mark.asyncio
@pytest.mark.timeout(600)
async def test_create_cluster(cluster):
    assert cluster.status == Status.running

    cluster.scale(2)
    await cluster
    assert len(cluster.workers) == 2

    async with Client(cluster, asynchronous=True) as client:
        inc = lambda x: x + 1
        assert await client.submit(inc, 10).result() == 11


@pytest.mark.asyncio
@pytest.mark.timeout(1200)
async def test_create_rapids_cluster(cluster_rapids):
    assert cluster_rapids.status == Status.running

    cluster_rapids.scale(1)
    await cluster_rapids
    assert len(cluster_rapids.workers) == 1

    async with Client(cluster_rapids, asynchronous=True) as client:

        def f():
            import cupy

            return float(cupy.random.random(100).mean())

        assert await client.submit(f).result() < 1


@pytest.mark.asyncio
async def test_get_ubuntu_image(ec2_client):
    image = await get_latest_ami_id(
        ec2_client,
        "ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-*",
        "099720109477",  # Canonical
    )
    assert "ami-" in image
