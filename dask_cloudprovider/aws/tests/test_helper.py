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
