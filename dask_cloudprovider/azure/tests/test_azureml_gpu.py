import pytest

pytest.importorskip("azureml")


def test_aml():
    from dask_cloudprovider.azure import AzureMLCluster  # noqa
