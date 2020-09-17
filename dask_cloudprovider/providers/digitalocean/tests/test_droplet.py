import asyncio
import pytest

from dask_cloudprovider.providers.digitalocean.droplet import DropletCluster
from dask.distributed import Client
from distributed.core import Status


@pytest.fixture
async def cluster():
    async with DropletCluster(asynchronous=True) as cluster:
        yield cluster


@pytest.mark.asyncio
async def test_init():
    cluster = DropletCluster(asynchronous=True)
    assert cluster.status == Status.created


@pytest.mark.asyncio
@pytest.mark.timeout(600)
# @pytest.mark.integration
async def test_create_cluster(cluster):
    assert cluster.status == Status.running

    cluster.scale(1)
    await cluster
    assert len(cluster.workers) == 1

    async with Client(cluster, asynchronous=True) as client:

        def inc(x):
            return x + 1

        assert await client.submit(inc, 10).result() == 11
