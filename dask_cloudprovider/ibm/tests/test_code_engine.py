import pytest

import dask

codeengine = pytest.importorskip("ibm_code_engine_sdk.code_engine_v2")

from dask_cloudprovider.ibm.code_engine import IBMCodeEngineCluster
from dask.distributed import Client
from distributed.core import Status


async def skip_without_credentials():
    if dask.config.get("cloudprovider.ibm.api_key") is None:
        pytest.skip(
            """
        You must configure a IBM API key to run this test.

        Either set this in your config

            # cloudprovider.yaml
            cloudprovider:
              ibm:
                api_key: "your_api_key"

        Or by setting it as an environment variable

            export DASK_CLOUDPROVIDER__IBM__API_KEY="your_api_key"

        """
        )

    if dask.config.get("cloudprovider.ibm.project_id") is None:
        pytest.skip(
            """
        You must configure a IBM project id to run this test.

        Either set this in your config

            # cloudprovider.yaml
            cloudprovider:
              ibm:
                project_id: "your_project_id"

        Or by setting it as an environment variable

            export DASK_CLOUDPROVIDER__IBM__PROJECT_ID="your_project_id"

        """
        )

    if dask.config.get("cloudprovider.ibm.region") is None:
        pytest.skip(
            """
        You must configure a IBM project id to run this test.

        Either set this in your config

            # cloudprovider.yaml
            cloudprovider:
              ibm:
                region: "your_region"

        Or by setting it as an environment variable

            export DASK_CLOUDPROVIDER__IBM__REGION="your_region"

        """
        )


@pytest.mark.asyncio
async def test_init():
    await skip_without_credentials()
    cluster = IBMCodeEngineCluster(asynchronous=True)
    assert cluster.status == Status.created


@pytest.mark.asyncio
@pytest.mark.timeout(1200)
@pytest.mark.external
async def test_create_cluster():
    async with IBMCodeEngineCluster(asynchronous=True) as cluster:
        cluster.scale(2)
        await cluster
        assert len(cluster.workers) == 2

        async with Client(cluster, asynchronous=True) as client:

            def inc(x):
                return x + 1

            assert await client.submit(inc, 10).result() == 11


@pytest.mark.asyncio
@pytest.mark.timeout(1200)
@pytest.mark.external
async def test_create_cluster_sync():
    with IBMCodeEngineCluster() as cluster:
        with Client(cluster) as client:
            cluster.scale(1)
            client.wait_for_workers(1)
            assert len(cluster.workers) == 1

            def inc(x):
                return x + 1

            assert client.submit(inc, 10).result() == 11
