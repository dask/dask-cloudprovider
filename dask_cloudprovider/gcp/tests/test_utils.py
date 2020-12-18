from dask_cloudprovider.gcp.utils import build_request


def test_build_request():
    assert build_request()(None, lambda x: x, "https://example.com")
