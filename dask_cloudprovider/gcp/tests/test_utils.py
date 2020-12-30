import pytest

from dask_cloudprovider.gcp.utils import build_request, is_inside_gce


def test_build_request():
    assert build_request()(None, lambda x: x, "https://example.com")


@pytest.mark.xfail(
    is_inside_gce(), reason="Fails if you run this test on GCE environment"
)
def test_is_gce_env():
    # Note: this test isn't super valuable, but at least we run the code
    assert is_inside_gce() is False
