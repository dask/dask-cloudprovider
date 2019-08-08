import pytest


def test_import():
    from dask_cloudprovider import ECSCluster
    from dask_cloudprovider import FargateCluster


# def test_ecscluster_raises():
#     from dask_cloudprovider import ECSCluster

#     with pytest.raises(RuntimeError):
#         _ = ECSCluster()
