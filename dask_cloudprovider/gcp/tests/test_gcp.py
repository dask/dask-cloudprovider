import os
import pytest

import dask
import googleapiclient.discovery
from dask_cloudprovider.providers.gcp.instances import (
    GCPCluster,
    GCPWorker,
    authenticate,
)
from dask.distributed import Client
import dask.array as da
from distributed.core import Status


async def skip_without_credentials():
    try:
        googleapiclient.discovery.build("compute", "v1")
    except:
        pytest.skip(
            """
        You must configure your GCP credentials to run this test.

            $ export GOOGLE_APPLICATION_CREDENTIALS=<path-to-gcp-json-credentials>

        """
        )


@pytest.fixture
async def config():
    return dask.config.get("cloudprovider.gcp", {})


@pytest.fixture
async def cluster(config):

    await skip_without_credentials()

    async with GCPCluster(asynchronous=True, auto_shutdown=True) as cluster:
        yield cluster


@pytest.mark.asyncio
async def test_creds_file():
    await skip_without_credentials()

    # test GOOGLE_APPLICATION_CREDENTIALS env var
    compute = authenticate()
    assert isinstance(compute, googleapiclient.discovery.Resource)

    # test google auth login creds file
    tmp = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

    compute = authenticate()
    isinstance(compute, googleapiclient.discovery.Resource)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = tmp


@pytest.mark.asyncio
async def test_init():
    await skip_without_credentials()

    cluster = GCPCluster(asynchronous=True)
    assert cluster.status == Status.created


@pytest.mark.asyncio
async def test_get_cloud_init():
    cloud_init = GCPCluster.get_cloud_init()
    assert "dask-scheduler" in cloud_init


@pytest.mark.asyncio
@pytest.mark.timeout(1200)
async def test_create_cluster(cluster):
    await skip_without_credentials()

    assert cluster.status == Status.running

    cluster.scale(1)

    await cluster

    assert len(cluster.workers) == 1

    client = Client(cluster, asynchronous=True)  # noqa
    await client

    def gpu_mem():
        from pynvml.smi import nvidia_smi

        nvsmi = nvidia_smi.getInstance()
        return nvsmi.DeviceQuery("memory.free, memory.total")

    results = await client.run(gpu_mem)
    for w, res in results.items():
        assert "total" in res["gpu"][0]["fb_memory_usage"].keys()
        print(res)


@pytest.mark.timeout(1200)
def test_create_cluster_sync():
    cluster = GCPCluster(
        zone="us-east1-c",
        machine_type="n1-standard-1",
        filesystem_size=50,
        ngpus=2,
        gpu_type="nvidia-tesla-t4",
        docker_image="rapidsai/rapidsai:0.16-cuda11.0-runtime-ubuntu18.04",
        worker_class="dask_cuda.CUDAWorker",
        worker_options={"rmm_pool_size": "15GB"},
        asynchronous=False,
    )

    cluster.scale(1)

    client = Client(cluster)  # noqa
    client.wait_for_workers(2)

    def gpu_mem():
        from pynvml.smi import nvidia_smi

        nvsmi = nvidia_smi.getInstance()
        return nvsmi.DeviceQuery("memory.free, memory.total")

    results = client.run(gpu_mem)
    for w, res in results.items():
        assert "total" in res["gpu"][0]["fb_memory_usage"].keys()
        print(res)
