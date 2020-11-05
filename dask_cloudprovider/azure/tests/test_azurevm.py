import pytest

import dask

azure_compute = pytest.importorskip("azure.mgmt.compute")

from dask_cloudprovider.azure import AzureVMCluster
from dask.distributed import Client
from distributed.core import Status


async def skip_without_credentials(config):
    return  # TODO Test for credentials


@pytest.fixture
async def config():
    return dask.config.get("cloudprovider.azure", {})


@pytest.fixture
async def cluster(config):
    await skip_without_credentials(config)
    async with AzureVMCluster(asynchronous=True) as cluster:
        yield cluster


@pytest.mark.asyncio
async def test_init():
    cluster = AzureVMCluster(asynchronous=True)
    assert cluster.status == Status.created


@pytest.mark.asyncio
@pytest.mark.timeout(600)
async def test_create_cluster(cluster):
    assert cluster.status == Status.running

    cluster.scale(1)
    await cluster
    assert len(cluster.workers) == 1

    async with Client(cluster, asynchronous=True) as client:

        def inc(x):
            return x + 1

        assert await client.submit(inc, 10).result() == 11
