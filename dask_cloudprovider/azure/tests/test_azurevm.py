import pytest

import dask

azure_compute = pytest.importorskip("azure.mgmt.compute")
from azure.common.credentials import get_azure_cli_credentials

from dask_cloudprovider.azure import AzureVMCluster
from dask.distributed import Client
from distributed.core import Status


async def skip_without_credentials():
    try:
        get_azure_cli_credentials()
    except:
        pytest.skip(
            """
        You must configure your Azure credentials to run this test.

            $ az login

        """
        )

    rg = dask.config.get("cloudprovider.azure.azurevm.resource_group", None)
    vnet = dask.config.get("cloudprovider.azure.azurevm.vnet", None)
    if rg is None or vnet is None:
        pytest.skip(
            """
        You must configure your Azure resource group and vnet to run this test.

            $ export DASK_CLOUDPROVIDER__AZURE__AZUREVM__RESOURCE_GROUP="<RESOURCE GROUP>"
            $ export DASK_CLOUDPROVIDER__AZURE__AZUREVM__VNET="<VNET>"

        """
        )


async def get_config():
    return dask.config.get("cloudprovider.azure", {})


@pytest.mark.asyncio
async def test_init():
    cluster = AzureVMCluster(asynchronous=True)
    assert cluster.status == Status.created


@pytest.mark.asyncio
@pytest.mark.timeout(1200)
async def test_create_cluster():
    await skip_without_credentials()

    async with AzureVMCluster(asynchronous=True) as cluster:
        assert cluster.status == Status.running

        cluster.scale(1)
        await cluster
        assert len(cluster.workers) == 1

        async with Client(cluster, asynchronous=True) as client:

            def inc(x):
                return x + 1

            assert await client.submit(inc, 10).result() == 11


@pytest.mark.asyncio
@pytest.mark.timeout(1200)
async def test_create_cluster_sync():
    await skip_without_credentials()

    with AzureVMCluster() as cluster:
        with Client(cluster) as client:
            cluster.scale(1)
            client.wait_for_workers(1)
            assert len(cluster.workers) == 1

            def inc(x):
                return x + 1

            assert client.submit(inc, 10).result() == 11
