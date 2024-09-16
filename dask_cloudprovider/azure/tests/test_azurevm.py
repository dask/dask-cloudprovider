import pytest

import dask

azure_compute = pytest.importorskip("azure.mgmt.compute")

from dask_cloudprovider.azure import AzureVMCluster
from dask.distributed import Client
from distributed.core import Status


def skip_without_credentials(func):
    rg = dask.config.get("cloudprovider.azure.resource_group", None)
    vnet = dask.config.get("cloudprovider.azure.azurevm.vnet", None)
    security_group = dask.config.get("cloudprovider.azure.azurevm.security_group", None)
    location = dask.config.get("cloudprovider.azure.location", None)
    if rg is None or vnet is None or security_group is None or location is None:
        return pytest.mark.skip(
            reason="""
        You must configure your Azure resource group and vnet to run this test.

            $ export DASK_CLOUDPROVIDER__AZURE__LOCATION="<LOCATION>"
            $ export DASK_CLOUDPROVIDER__AZURE__RESOURCE_GROUP="<RESOURCE GROUP>"
            $ export DASK_CLOUDPROVIDER__AZURE__AZUREVM__VNET="<VNET>"
            $ export DASK_CLOUDPROVIDER__AZURE__AZUREVM__SECURITY_GROUP="<SECURITY GROUP>"

        """
        )(func)
    return func


async def get_config():
    return dask.config.get("cloudprovider.azure", {})


@pytest.mark.asyncio
@skip_without_credentials
@pytest.mark.external
async def test_init():
    cluster = AzureVMCluster(asynchronous=True)
    assert cluster.status == Status.created


@pytest.mark.asyncio
@pytest.mark.timeout(1200)
@skip_without_credentials
@pytest.mark.external
async def test_create_cluster():
    async with AzureVMCluster(asynchronous=True) as cluster:
        assert cluster.status == Status.running

        cluster.scale(2)
        await cluster
        assert len(cluster.workers) == 2

        async with Client(cluster, asynchronous=True) as client:

            def inc(x):
                return x + 1

            assert await client.submit(inc, 10).result() == 11


@pytest.mark.asyncio
@pytest.mark.timeout(1200)
@skip_without_credentials
@pytest.mark.external
async def test_create_cluster_sync():
    with AzureVMCluster() as cluster:
        with Client(cluster) as client:
            cluster.scale(1)
            client.wait_for_workers(1)
            assert len(cluster.workers) == 1

            def inc(x):
                return x + 1

            assert client.submit(inc, 10).result() == 11


@pytest.mark.asyncio
@pytest.mark.timeout(1200)
@skip_without_credentials
@pytest.mark.external
async def test_create_rapids_cluster_sync():
    with AzureVMCluster(
        vm_size="Standard_NC12s_v3",
        docker_image="rapidsai/rapidsai:cuda11.0-runtime-ubuntu18.04-py3.9",
        worker_class="dask_cuda.CUDAWorker",
        worker_options={"rmm_pool_size": "15GB"},
    ) as cluster:
        with Client(cluster) as client:
            cluster.scale(1)
            client.wait_for_workers(1)

            def gpu_mem():
                from pynvml.smi import nvidia_smi

                nvsmi = nvidia_smi.getInstance()
                return nvsmi.DeviceQuery("memory.free, memory.total")

            results = client.run(gpu_mem)
            for w, res in results.items():
                assert "total" in res["gpu"][0]["fb_memory_usage"].keys()
                print(res)


@pytest.mark.asyncio
@skip_without_credentials
async def test_render_cloud_init():
    cloud_init = AzureVMCluster.get_cloud_init(docker_args="--privileged")
    assert " --privileged " in cloud_init

    cloud_init = AzureVMCluster.get_cloud_init(
        docker_image="foo/bar:baz",
        extra_bootstrap=["echo 'hello world'", "echo 'foo bar'"],
    )
    assert "foo/bar:baz" in cloud_init
    assert "- echo 'hello world'" in cloud_init
    assert "- echo 'foo bar'" in cloud_init
