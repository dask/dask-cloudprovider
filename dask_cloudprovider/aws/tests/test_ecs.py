from unittest import mock
from unittest.mock import AsyncMock

import pytest

aiobotocore = pytest.importorskip("aiobotocore")


def test_import():
    from dask_cloudprovider.aws import ECSCluster  # noqa
    from dask_cloudprovider.aws import FargateCluster  # noqa


def test_reuse_ecs_cluster():
    from dask_cloudprovider.aws import ECSCluster  # noqa

    fc1_name = "Spooky"
    fc2_name = "Weevil"
    vpc_name = "MyNetwork"
    vpc_subnets = ["MySubnet1", "MySubnet2"]
    cluster_arn = "CompletelyMadeUp"
    cluster_name = "Crunchy"
    log_group_name = "dask-ecs"

    expected_execution_role_name1 = f"dask-{fc1_name}-execution-role"
    expected_task_role_name1 = f"dask-{fc1_name}-task-role"
    expected_log_stream_prefix1 = f"{cluster_name}/{fc1_name}"
    expected_security_group_name1 = f"dask-{fc1_name}-security-group"
    expected_scheduler_task_name1 = f"dask-{fc1_name}-scheduler"
    expected_worker_task_name1 = f"dask-{fc1_name}-worker"

    expected_execution_role_name2 = f"dask-{fc2_name}-execution-role"
    expected_task_role_name2 = f"dask-{fc2_name}-task-role"
    expected_log_stream_prefix2 = f"{cluster_name}/{fc2_name}"
    expected_security_group_name2 = f"dask-{fc2_name}-security-group"
    expected_scheduler_task_name2 = f"dask-{fc2_name}-scheduler"
    expected_worker_task_name2 = f"dask-{fc2_name}-worker"

    mock_client = AsyncMock()
    mock_client.describe_clusters.return_value = {
        "clusters": [{"clusterName": cluster_name}]
    }
    mock_client.list_account_settings.return_value = {"settings": {"value": "enabled"}}
    mock_client.create_role.return_value = {"Role": {"Arn": "Random"}}
    mock_client.describe_log_groups.return_value = {"logGroups": []}

    class MockSession:
        class MockClient:
            async def __aenter__(self, *args, **kwargs):
                return mock_client

            async def __aexit__(self, *args, **kwargs):
                return

        def create_client(self, *args, **kwargs):
            return MockSession.MockClient()

    with (
        mock.patch(
            "dask_cloudprovider.aws.ecs.get_session", return_value=MockSession()
        ),
        mock.patch("distributed.deploy.spec.SpecCluster._start"),
        mock.patch("weakref.finalize"),
    ):
        # Make ourselves a test cluster
        fc1 = ECSCluster(
            name=fc1_name,
            cluster_arn=cluster_arn,
            vpc=vpc_name,
            subnets=vpc_subnets,
            skip_cleanup=True,
        )
        # Are we re-using the existing ECS cluster?
        assert fc1.cluster_name == cluster_name
        # Have we made completely unique AWS resources to run on that cluster?
        assert fc1._execution_role_name == expected_execution_role_name1
        assert fc1._task_role_name == expected_task_role_name1
        assert fc1._cloudwatch_logs_stream_prefix == expected_log_stream_prefix1
        assert (
            fc1.scheduler_spec["options"]["log_stream_prefix"]
            == expected_log_stream_prefix1
        )
        assert (
            fc1.new_spec["options"]["log_stream_prefix"] == expected_log_stream_prefix1
        )
        assert fc1.cloudwatch_logs_group == log_group_name
        assert fc1.scheduler_spec["options"]["log_group"] == log_group_name
        assert fc1.new_spec["options"]["log_group"] == log_group_name
        sg_calls = mock_client.create_security_group.call_args_list
        assert len(sg_calls) == 1
        assert sg_calls[0].kwargs["GroupName"] == expected_security_group_name1
        td_calls = mock_client.register_task_definition.call_args_list
        assert len(td_calls) == 2
        assert td_calls[0].kwargs["family"] == expected_scheduler_task_name1
        assert td_calls[1].kwargs["family"] == expected_worker_task_name1

        # Reset mocks ready for second cluster
        mock_client.create_security_group.reset_mock()
        mock_client.register_task_definition.reset_mock()

        # Make ourselves a second test cluster on the same ECS cluster
        fc2 = ECSCluster(
            name=fc2_name,
            cluster_arn=cluster_arn,
            vpc=vpc_name,
            subnets=vpc_subnets,
            skip_cleanup=True,
        )
        # Are we re-using the existing ECS cluster?
        assert fc2.cluster_name == cluster_name
        # Have we made completely unique AWS resources to run on that cluster?
        assert fc2._execution_role_name == expected_execution_role_name2
        assert fc2._task_role_name == expected_task_role_name2
        assert fc2._cloudwatch_logs_stream_prefix == expected_log_stream_prefix2
        assert (
            fc2.scheduler_spec["options"]["log_stream_prefix"]
            == expected_log_stream_prefix2
        )
        assert (
            fc2.new_spec["options"]["log_stream_prefix"] == expected_log_stream_prefix2
        )
        assert fc2.cloudwatch_logs_group == log_group_name
        assert fc2.scheduler_spec["options"]["log_group"] == log_group_name
        assert fc2.new_spec["options"]["log_group"] == log_group_name
        sg_calls = mock_client.create_security_group.call_args_list
        assert len(sg_calls) == 1
        assert sg_calls[0].kwargs["GroupName"] == expected_security_group_name2
        td_calls = mock_client.register_task_definition.call_args_list
        assert len(td_calls) == 2
        assert td_calls[0].kwargs["family"] == expected_scheduler_task_name2
        assert td_calls[1].kwargs["family"] == expected_worker_task_name2

        # Finish up
        fc1.close()
        fc2.close()
