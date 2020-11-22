import pytest

pytest.importorskip("azureml")


def test_aml():
    from dask_cloudprovider.azureml import AzureMLCluster  # noqa
