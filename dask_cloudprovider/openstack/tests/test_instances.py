import pytest
import dask
from dask_cloudprovider.openstack.instances import OpenStackCluster
from dask.distributed import Client
from distributed.core import Status

# Optional: Skips tests if OpenStack credentials are not set


async def skip_without_credentials(config):
    if (
        config.get("auth_url") is None
        or config.get("application_credential_secret") is None
    ):
        pytest.skip(
            """
        You must configure OpenStack credentials to run this test.

        Set this in your config file or environment variables:

        # cloudprovider.yaml
        cloudprovider:
          openstack:
            auth_url: "your_auth_url"
            application_credential_id: "your_app_cred_id"
            application_credential_secret: "your_app_cred_secret"
        """
        )


@pytest.fixture
async def config():
    return dask.config.get("cloudprovider.openstack", {})


@pytest.fixture
@pytest.mark.external
async def cluster(config):
    await skip_without_credentials(config)

    async with OpenStackCluster(asynchronous=True) as cluster:
        yield cluster


@pytest.mark.asyncio
async def test_init():
    cluster = OpenStackCluster(asynchronous=True)
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


@pytest.mark.asyncio
async def test_get_cloud_init():
    cloud_init = OpenStackCluster.get_cloud_init(
        docker_args="--privileged",
    )
    assert " --privileged " in cloud_init
