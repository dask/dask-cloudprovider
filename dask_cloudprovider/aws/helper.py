"""Helper functions for working with AWS services."""
from datetime import datetime

DEFAULT_SECURITY_GROUP_NAME = "dask-default"


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
    current_sleep_millis = min_sleep_millis * current_try**2
    return min(current_sleep_millis, max_sleep_millis) / 1000  # return in seconds


class ConfigMixin:
    def update_attr_from_config(self, attr: str, private: bool):
        """Update class attribute of given cluster based on config, if not already set. If `private` is True, the class
        attribute will be prefixed with an underscore.

        This mixin can be applied to any class that has a config dict attribute.
        """
        prefix = "_" if private else ""
        if getattr(self, f"{prefix}{attr}") is None:
            setattr(self, f"{prefix}{attr}", self.config.get(attr))


async def get_latest_ami_id(client, name_glob, owner):
    images = await client.describe_images(
        Filters=[
            {"Name": "name", "Values": [name_glob]},
            {"Name": "owner-id", "Values": [owner]},
        ]
    )
    creation_date = None
    image_id = None

    for image in images["Images"]:
        image_date = datetime.strptime(image["CreationDate"], "%Y-%m-%dT%H:%M:%S.%fZ")
        if creation_date is None or creation_date < image_date:
            image_id = image["ImageId"]
            creation_date = image_date
    return image_id


async def get_default_vpc(client):
    vpcs = (await client.describe_vpcs())["Vpcs"]
    [vpc] = [vpc for vpc in vpcs if vpc["IsDefault"]]
    return vpc["VpcId"]


async def get_vpc_subnets(client, vpc):
    vpcs = (await client.describe_vpcs())["Vpcs"]
    [vpc] = [x for x in vpcs if x["VpcId"] == vpc]
    subnets = (await client.describe_subnets())["Subnets"]
    return [subnet["SubnetId"] for subnet in subnets if subnet["VpcId"] == vpc["VpcId"]]


async def get_security_group(client, vpc, create_default=True):
    try:
        response = await client.describe_security_groups(
            GroupNames=[DEFAULT_SECURITY_GROUP_NAME]
        )
        groups = response["SecurityGroups"]
    except Exception:
        groups = []
    if len(groups) > 0:
        return groups[0]["GroupId"]
    else:
        if create_default:
            try:
                return await create_default_security_group(
                    client, DEFAULT_SECURITY_GROUP_NAME, vpc
                )
            except Exception as e:
                raise RuntimeError(
                    "Unable to create default security group. Please specify manually."
                ) from e
        else:
            raise RuntimeError(
                "Unable to find suitable security group. Please specify manually."
            )


async def create_default_security_group(client, group_name, vpc):
    response = await client.create_security_group(
        Description="A default security group for Dask",
        GroupName=group_name,
        VpcId=vpc,
        DryRun=False,
    )

    await client.authorize_security_group_ingress(
        GroupId=response["GroupId"],
        IpPermissions=[
            {
                "IpProtocol": "TCP",
                "FromPort": 8786,
                "ToPort": 8787,
                "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "Anywhere"}],
                "Ipv6Ranges": [{"CidrIpv6": "::/0", "Description": "Anywhere"}],
            },
            {
                "IpProtocol": "TCP",
                "FromPort": 0,
                "ToPort": 65535,
                "UserIdGroupPairs": [{"GroupId": response["GroupId"]}],
            },
        ],
        DryRun=False,
    )

    return response["GroupId"]
