import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--create-external-resources",
        action="store_true",
        default=False,
        help="Run tests that create external resources.",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "external: mark test as creates external resources"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--create-external-resources"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(
        reason="need --create-external-resources option to run"
    )
    for item in items:
        if "external" in item.keywords:
            item.add_marker(skip_slow)
