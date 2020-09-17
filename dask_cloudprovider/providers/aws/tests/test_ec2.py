import asyncio
import pytest

from dask_cloudprovider.providers.aws.ec2 import EC2Cluster
from dask.distributed import Client
import dask.array as da
from distributed.core import Status


@pytest.fixture
async def cluster():
    async with EC2Cluster(asynchronous=True) as cluster:
        yield cluster


@pytest.mark.asyncio
async def test_init():
    cluster = EC2Cluster(asynchronous=True)
    assert cluster.status == Status.created


@pytest.mark.asyncio
@pytest.mark.timeout(600)
# @pytest.mark.integration
async def test_create_cluster(cluster):
    assert cluster.status == Status.running

    cluster.scale(2)
    await cluster
    assert len(cluster.workers) == 2

    async with Client(cluster, asynchronous=True) as client:
        inc = lambda x: x + 1
        assert await client.submit(inc, 10).result() == 11
