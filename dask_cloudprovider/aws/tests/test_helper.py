def test_aws_to_dict_and_back():
    from dask_cloudprovider.aws.helper import aws_to_dict, dict_to_aws

    aws_dict = [{"key": "hello", "value": "world"}]
    aws_upper_dict = [{"Key": "hello", "Value": "world"}]
    py_dict = {"hello": "world"}

    assert dict_to_aws(py_dict) == aws_dict
    assert dict_to_aws(py_dict, upper=True) == aws_upper_dict
    assert aws_to_dict(aws_dict) == py_dict

    assert aws_to_dict(dict_to_aws(py_dict, upper=True)) == py_dict
    assert aws_to_dict(dict_to_aws(py_dict)) == py_dict
    assert dict_to_aws(aws_to_dict(aws_dict)) == aws_dict
    assert dict_to_aws(aws_to_dict(aws_upper_dict), upper=True) == aws_upper_dict


def test_get_sleep_duration_first_try():
    from dask_cloudprovider.aws.helper import get_sleep_duration

    duration = get_sleep_duration(
        current_try=0, min_sleep_millis=10, max_sleep_millis=5000
    )
    assert duration == 0.01


def test_get_sleep_duration_max():
    from dask_cloudprovider.aws.helper import get_sleep_duration

    duration = get_sleep_duration(
        current_try=23, min_sleep_millis=10, max_sleep_millis=5000
    )
    assert duration == 5.0


def test_get_sleep_duration_negative_try():
    from dask_cloudprovider.aws.helper import get_sleep_duration

    duration = get_sleep_duration(
        current_try=-1, min_sleep_millis=10, max_sleep_millis=5000
    )
    assert duration == 0.01


def test_config_mixin():
    from dask_cloudprovider.aws.helper import ConfigMixin

    class MockCluster(ConfigMixin):
        config = None
        _attr1 = "foo"
        attr2 = None

        def __init__(self):
            self.config = {"attr2": "bar"}

    cluster_with_mixin = MockCluster()

    # Test that nothing happens if attr is already set
    attr1 = cluster_with_mixin._attr1
    cluster_with_mixin.update_attr_from_config(attr="attr1", private=True)
    assert cluster_with_mixin._attr1 == attr1

    # Test that attr is updated if existing value is None
    cluster_with_mixin.update_attr_from_config(attr="attr2", private=False)
    assert cluster_with_mixin.attr2 == "bar"
