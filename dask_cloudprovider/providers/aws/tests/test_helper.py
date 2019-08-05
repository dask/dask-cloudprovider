def test_aws_to_dict_and_back():
    from dask_cloudprovider.providers.aws.helper import aws_to_dict, dict_to_aws

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
