"""Helper functions for working with AWS services."""


def dict_to_aws(py_dict, upper=False):
    key_string = "Key" if upper else "key"
    value_string = "Value" if upper else "value"
    return [{key_string: key, value_string: value} for key, value in py_dict.items()]


def aws_to_dict(aws_dict):
    try:
        return {item["key"]: item["value"] for item in aws_dict}
    except KeyError:
        return {item["Key"]: item["Value"] for item in aws_dict}
