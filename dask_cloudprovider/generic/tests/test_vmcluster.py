import pytest

import asyncio
import time

from dask_cloudprovider.generic.vmcluster import VMCluster, VMInterface


class DummyWorker(VMInterface):
    """A dummy worker for testing."""


class DummyScheduler(VMInterface):
    """A dummy scheduler for testing."""


class DummyCluster(VMCluster):
    """A dummy cluster for testing."""

    scheduler_class = DummyScheduler
    worker_class = DummyWorker


@pytest.mark.asyncio
async def test_init():
    with pytest.raises(RuntimeError):
        _ = VMCluster(asynchronous=True)


@pytest.mark.asyncio
async def test_call_async():
    cluster = DummyCluster(asynchronous=True)

    def blocking(string):
        time.sleep(0.1)
        return string

    start = time.time()

    a, b, c, d = await asyncio.gather(
        cluster.call_async(blocking, "hello"),
        cluster.call_async(blocking, "world"),
        cluster.call_async(blocking, "foo"),
        cluster.call_async(blocking, "bar"),
    )

    assert a == "hello"
    assert b == "world"
    assert c == "foo"
    assert d == "bar"

    # Each call to ``blocking`` takes 0.1 seconds, but they should've been run concurrently.
    assert time.time() - start < 0.2

    await cluster.close()
