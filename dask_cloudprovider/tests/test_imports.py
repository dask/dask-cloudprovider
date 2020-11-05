import pytest


def test_imports():
    from dask_cloudprovider.aws import EC2Cluster  # noqa
    from dask_cloudprovider.aws import ECSCluster  # noqa
    from dask_cloudprovider.aws import FargateCluster  # noqa
    from dask_cloudprovider.azure import AzureMLCluster  # noqa
    from dask_cloudprovider.digitalocean import DropletCluster  # noqa


def test_import_exceptions():
    with pytest.raises(ImportError):
        from dask_cloudprovider import EC2Cluster  # noqa
    with pytest.raises(ImportError):
        from dask_cloudprovider import ECSCluster  # noqa
    with pytest.raises(ImportError):
        from dask_cloudprovider import FargateCluster  # noqa
    with pytest.raises(ImportError):
        from dask_cloudprovider import AzureMLCluster  # noqa
    with pytest.raises(ImportError):
        from dask_cloudprovider import DropletCluster  # noqa
