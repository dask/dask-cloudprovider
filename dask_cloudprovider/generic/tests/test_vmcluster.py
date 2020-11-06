import pytest

from dask_cloudprovider.generic.vmcluster import VMCluster


@pytest.mark.asyncio
async def test_init(gen_cluster):
    with pytest.raises(RuntimeError):
        _ = VMCluster(asynchronous=True)
