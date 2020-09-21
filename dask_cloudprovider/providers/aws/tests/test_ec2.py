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
async def test_get_ubuntu_image(ec2_client):
    image = await get_latest_ami_id(
        ec2_client,
        "ubuntu/images/hvm-instance/ubuntu-bionic-18.04-amd64-server-*",
        "099720109477",  # Canonical
    )
    assert "ami-" in image
