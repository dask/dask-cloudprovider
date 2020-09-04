import pytest

from dask_cloudprovider.providers.digitalocean.droplet import DropletCluster
from dask.distributed import Client
import dask.array as da
from distributed.core import Status


@pytest.fixture
async def gen_cluster():
    async with DropletCluster(asynchronous=True) as cluster:
        yield cluster


@pytest.mark.asyncio
async def test_init():
    cluster = DropletCluster(asynchronous=True)
    assert cluster.status == Status.created


@pytest.mark.asyncio
@pytest.mark.timeout(300)
# @pytest.mark.integration
async def test_create_cluster(gen_cluster):
    cluster = await gen_cluster
    assert cluster.status == Status.running

    cluster.scale(2)

    await cluster

    assert len(cluster.workers) == 2

    client = Client(cluster, asynchronous=True)  # noqa
    await client
