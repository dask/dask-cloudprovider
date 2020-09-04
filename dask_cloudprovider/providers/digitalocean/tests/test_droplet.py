import pytest

from dask_cloudprovider.providers.digitalocean.droplet import DropletCluster
from dask.distributed import Client
import dask.array as da
from distributed.core import Status


@pytest.fixture
async def gen_cluster():
    cluster = DropletCluster(asynchronous=True)
    yield cluster
    await cluster.close()


@pytest.mark.asyncio
async def test_init(gen_cluster):
    cluster = gen_cluster
    assert cluster.status == Status.created


@pytest.mark.asyncio
@pytest.mark.timeout(300)
# @pytest.mark.integration
async def test_create_cluster(gen_cluster):
    cluster = await gen_cluster
    assert cluster.status == Status.running

    client = Client(cluster)  # noqa

    cluster.scale(2)

    arr = da.random.random((1000, 1000), chunks=(100, 100))
    assert arr.mean().compute() < 1
