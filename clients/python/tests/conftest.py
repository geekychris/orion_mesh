"""Shared pytest fixtures. The unit tests use `responses` to mock the
controller's HTTP surface; integration tests against a live broker live
in tests/integration/ and are skipped unless --run-integration is passed."""
import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="run integration tests against a live NATS + controller",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-integration"):
        return
    skip = pytest.mark.skip(reason="needs --run-integration")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)
