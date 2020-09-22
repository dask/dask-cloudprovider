import pytest

from dask_cloudprovider.providers.gcp.instances import GCPCluster
from dask.distributed import Client
import dask.array as da
from distributed.core import Status


@pytest.fixture
async def gen_cluster():
    name = "aa-dask-rapids-gcp-test"
    zone = "us-east1-c"
    projectid = "nv-ai-infra"
    machine_type = "n1-standard-1"

    async with GCPCluster(
        asynchronous=True,
        name=name,
        zone=zone,
        projectid=projectid,
        machine_type=machine_type,
    ) as cluster:
        yield cluster


@pytest.mark.asyncio
async def test_init():
    name = "aa-dask-rapids-gcp-test"
    zone = "us-east1-c"
    projectid = "nv-ai-infra"
    machine_type = "n1-standard-1"

    cluster = GCPCluster(
        asynchronous=True,
        name=name,
        zone=zone,
        projectid=projectid,
        machine_type=machine_type,
    )
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
    breakpoint()
    1+1

    client = Client(cluster, asynchronous=True)  # noqa
    await client
