import pytest

aiobotocore = pytest.importorskip("aiobotocore")

from dask_cloudprovider.aws.ec2 import EC2Cluster
from dask_cloudprovider.aws.helper import get_latest_ami_id
from dask.distributed import Client
from distributed.core import Status


async def skip_without_credentials():
    try:
        async with aiobotocore.get_session().create_client("sts") as client:
            await client.get_caller_identity()
    except Exception:
        pytest.skip(
            """
        You must configure Your AWS credentials to run this test.

            $ aws configure

        """
        )


@pytest.fixture
@pytest.mark.external
async def cluster():
    await skip_without_credentials()
    async with EC2Cluster(asynchronous=True) as cluster:
        yield cluster


@pytest.fixture
@pytest.mark.external
async def cluster_sync():
    await skip_without_credentials()
    cluster = EC2Cluster()
    yield cluster


@pytest.fixture
@pytest.mark.external
async def cluster_rapids():
    await skip_without_credentials()
    async with EC2Cluster(
        asynchronous=True,
        # Deep Learning AMI (Ubuntu 18.04)
        ami="ami-0c7c7d78f752f8f17",
        # Python version must match local version and CUDA version must match AMI CUDA version
        docker_image="rapidsai/rapidsai:cuda10.1-runtime-ubuntu18.04-py3.9",
        instance_type="p3.2xlarge",
        bootstrap=False,
        filesystem_size=120,
    ) as cluster:
        yield cluster


@pytest.fixture
@pytest.mark.external
async def cluster_rapids_packer():
    await skip_without_credentials()
    async with EC2Cluster(
        asynchronous=True,
        # Packer AMI
        ami="ami-04e5539cb82859e69",
        # Python version must match local version and CUDA version must match AMI CUDA version
        docker_image="rapidsai/rapidsai:cuda10.1-runtime-ubuntu18.04-py3.9",
        instance_type="p3.2xlarge",
        bootstrap=False,
        filesystem_size=120,
    ) as cluster:
        yield cluster


@pytest.fixture
@pytest.mark.external
async def cluster_packer():
    await skip_without_credentials()
    async with EC2Cluster(
        asynchronous=True, ami="ami-0e6187593ace05a0c", bootstrap=False
    ) as cluster:
        yield cluster


@pytest.fixture
async def ec2_client():
    await skip_without_credentials()
    async with aiobotocore.get_session().create_client("ec2") as client:
        yield client


@pytest.mark.asyncio
@pytest.mark.external
async def test_init():
    cluster = EC2Cluster(asynchronous=True)
    assert cluster.status == Status.created


@pytest.mark.asyncio
@pytest.mark.timeout(600)
async def test_create_cluster(cluster):
    assert cluster.status == Status.running

    cluster.scale(2)
    await cluster
    assert len(cluster.workers) == 2

    async with Client(cluster, asynchronous=True) as client:
        inc = lambda x: x + 1
        assert await client.submit(inc, 10).result() == 11


@pytest.mark.asyncio
@pytest.mark.timeout(600)
async def test_create_cluster_sync(cluster_sync):
    assert cluster_sync.status == Status.running

    cluster_sync.scale(2)

    with Client(cluster_sync) as client:
        inc = lambda x: x + 1
        assert client.submit(inc, 10).result() == 11


@pytest.mark.asyncio
@pytest.mark.timeout(600)
async def test_create_cluster_with_packer(cluster_packer):
    assert cluster_packer.status == Status.running

    cluster_packer.scale(2)
    await cluster_packer
    assert len(cluster_packer.workers) == 2

    async with Client(cluster_packer, asynchronous=True) as client:
        inc = lambda x: x + 1
        assert await client.submit(inc, 10).result() == 11


@pytest.mark.asyncio
@pytest.mark.timeout(1200)
async def test_create_rapids_cluster(cluster_rapids):
    assert cluster_rapids.status == Status.running

    cluster_rapids.scale(1)
    await cluster_rapids
    assert len(cluster_rapids.workers) == 1

    async with Client(cluster_rapids, asynchronous=True) as client:

        def f():
            import cupy

            return float(cupy.random.random(100).mean())

        assert await client.submit(f).result() < 1


@pytest.mark.asyncio
@pytest.mark.timeout(1200)
async def test_create_rapids_cluster_with_packer(cluster_rapids_packer):
    assert cluster_rapids_packer.status == Status.running

    cluster_rapids_packer.scale(1)
    await cluster_rapids_packer
    assert len(cluster_rapids_packer.workers) == 1

    async with Client(cluster_rapids_packer, asynchronous=True) as client:

        def f():
            import cupy

            return float(cupy.random.random(100).mean())

        assert await client.submit(f).result() < 1


@pytest.mark.asyncio
async def test_get_ubuntu_image(ec2_client):
    image = await get_latest_ami_id(
        ec2_client,
        "ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-*",
        "099720109477",  # Canonical
    )
    assert "ami-" in image


@pytest.mark.asyncio
async def test_get_cloud_init():
    cloud_init = EC2Cluster.get_cloud_init(
        env_vars={"EXTRA_PIP_PACKAGES": "s3fs"},
        docker_args="--privileged",
    )
    assert "systemctl start docker" in cloud_init
    assert ' -e EXTRA_PIP_PACKAGES="s3fs" ' in cloud_init
    assert " --privileged " in cloud_init


@pytest.mark.asyncio
async def test_get_cloud_init_rapids():
    cloud_init = EC2Cluster.get_cloud_init(
        # Deep Learning AMI (Ubuntu 18.04)
        ami="ami-0c7c7d78f752f8f17",
        # Python version must match local version and CUDA version must match AMI CUDA version
        docker_image="rapidsai/rapidsai:cuda10.1-runtime-ubuntu18.04-py3.9",
        instance_type="p3.2xlarge",
        bootstrap=False,
        filesystem_size=120,
    )
    assert "rapidsai" in cloud_init
