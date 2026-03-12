"""Smoke tests: verify the test infrastructure itself works."""

import sys


def test_python_version():
    assert sys.version_info >= (3, 11)


def test_backend_importable():
    import importlib
    assert importlib.util.find_spec("backend") is not None


def test_fastapi_importable():
    import fastapi  # noqa: F401


def test_pytest_asyncio_importable():
    import pytest_asyncio  # noqa: F401


def test_httpx_importable():
    import httpx  # noqa: F401
