import pytest


def test_import():
    from dask_cloud import ECSCluster
    from dask_cloud import FargateCluster


def test_ecscluster_raises():
    from dask_cloud import ECSCluster

    with pytest.raises(RuntimeError):
        _ = ECSCluster()
