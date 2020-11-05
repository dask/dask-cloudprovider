import pytest

aiobotocore = pytest.importorskip("aiobotocore")


def test_import():
    from dask_cloudprovider.aws import ECSCluster  # noqa
    from dask_cloudprovider.aws import FargateCluster  # noqa
