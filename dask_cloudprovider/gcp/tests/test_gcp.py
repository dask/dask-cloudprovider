import pytest

import dask
from dask_cloudprovider.gcp.instances import (
    GCPCluster,
    GCPCompute,
    GCPCredentialsError,
)
from dask.distributed import Client
from distributed.core import Status


def skip_without_credentials():
    try:
        _ = GCPCompute()
    except GCPCredentialsError:
        pytest.skip(
            """
        You must configure your GCP credentials to run this test.

            $ google auth login

            or

            $ export GOOGLE_APPLICATION_CREDENTIALS=<path-to-gcp-json-credentials>

        """
        )

    if not dask.config.get("cloudprovider.gcp.projectid"):
        pytest.skip(
            """
        You must configure your Google project ID to run this test.

            # ~/.config/dask/cloudprovider.yaml
            cloudprovider:
              gcp:
                projectid: "YOUR PROJECT ID"

            or

            $ export DASK_CLOUDPROVIDER__GCP__PROJECTID="YOUR PROJECT ID"

        """
        )


@pytest.mark.asyncio
async def test_init():
    skip_without_credentials()

    cluster = GCPCluster(asynchronous=True)
    assert cluster.status == Status.created


@pytest.mark.asyncio
async def test_get_cloud_init():
    skip_without_credentials()
    cloud_init = GCPCluster.get_cloud_init(
        security=True,
        docker_args="--privileged",
        extra_bootstrap=["gcloud auth print-access-token"],
    )
    assert "dask-scheduler" in cloud_init
    assert "# Bootstrap" in cloud_init
    assert " --privileged " in cloud_init
    assert "- gcloud auth print-access-token" in cloud_init


@pytest.mark.asyncio
@pytest.mark.timeout(1200)
@pytest.mark.external
async def test_create_cluster():
    skip_without_credentials()

    async with GCPCluster(
        asynchronous=True, env_vars={"FOO": "bar"}, security=True
    ) as cluster:
        assert cluster.status == Status.running

        cluster.scale(2)
        await cluster
        assert len(cluster.workers) == 2

        async with Client(cluster, asynchronous=True) as client:

            def inc(x):
                return x + 1

            def check_env():
                import os

                return os.environ["FOO"]

            assert await client.submit(inc, 10).result() == 11
            assert await client.submit(check_env).result() == "bar"


@pytest.mark.asyncio
@pytest.mark.timeout(1200)
@pytest.mark.external
async def test_create_cluster_sync():
    skip_without_credentials()

    cluster = GCPCluster(n_workers=1)
    client = Client(cluster)

    def inc(x):
        return x + 1

    assert client.submit(inc, 10).result() == 11


@pytest.mark.asyncio
@pytest.mark.timeout(1200)
@pytest.mark.external
async def test_create_rapids_cluster():
    skip_without_credentials()

    async with GCPCluster(
        source_image="projects/nv-ai-infra/global/images/ngc-docker-11-20200916",
        zone="us-east1-c",
        machine_type="n1-standard-1",
        filesystem_size=50,
        ngpus=2,
        gpu_type="nvidia-tesla-t4",
        docker_image="rapidsai/rapidsai:cuda11.0-runtime-ubuntu18.04-py3.9",
        worker_class="dask_cuda.CUDAWorker",
        worker_options={"rmm_pool_size": "15GB"},
        asynchronous=True,
        auto_shutdown=True,
        bootstrap=False,
    ) as cluster:
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
@pytest.mark.external
def test_create_rapids_cluster_sync():
    skip_without_credentials()
    cluster = GCPCluster(
        source_image="projects/nv-ai-infra/global/images/packer-1607527229",
        network="dask-gcp-network-test",
        zone="us-east1-c",
        machine_type="n1-standard-1",
        filesystem_size=50,
        ngpus=2,
        gpu_type="nvidia-tesla-t4",
        docker_image="rapidsai/rapidsai:cuda11.0-runtime-ubuntu18.04-py3.9",
        worker_class="dask_cuda.CUDAWorker",
        worker_options={"rmm_pool_size": "15GB"},
        asynchronous=False,
        bootstrap=False,
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
    cluster.close()
