import pytest

from dask_cloudprovider.providers.generic.vmcluster import VMCluster
from distributed.core import Status


@pytest.fixture
async def gen_cluster():
    yield VMCluster(asynchronous=True)


@pytest.mark.asyncio
async def test_init(gen_cluster):
    cluster = gen_cluster
    assert cluster.status == Status.created
