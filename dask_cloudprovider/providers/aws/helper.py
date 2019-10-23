"""Helper functions for working with AWS services."""


def dict_to_aws(py_dict, upper=False, key_string=None, value_string=None):
    key_string = key_string or ("Key" if upper else "key")
    value_string = value_string or ("Value" if upper else "value")
    return [{key_string: key, value_string: value} for key, value in py_dict.items()]


def aws_to_dict(aws_dict):
    try:
        return {item["key"]: item["value"] for item in aws_dict}
    except KeyError:
        return {item["Key"]: item["Value"] for item in aws_dict}


# https://aws.amazon.com/blogs/messaging-and-targeting/how-to-handle-a-throttling-maximum-sending-rate-exceeded-error/
def get_sleep_duration(current_try, min_sleep_millis=10, max_sleep_millis=5000):
    current_try = max(1, current_try)
    current_sleep_millis = min_sleep_millis * current_try ** 2
    return min(current_sleep_millis, max_sleep_millis) / 1000  # return in seconds
