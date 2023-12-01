# tests/conftest.py
import pytest
import logging


logging.basicConfig()
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)


# Define custom markers
def pytest_configure(config):
    config.addinivalue_line("markers", "env: Conda virtual environment configuration")


def pytest_addoption(parser):
    parser.addini("custom_info", help="Custom information for the test report")


def pytest_report_header(config, startdir):
    """
    Custom hook to add information to the test report header.

    This function is called when generating the test report.

    Args:
        config: pytest config object
        startdir: root directory of the test run

    Returns:
        str: Additional information to be displayed in the test report header.
    """
    custom_info = config.getini("custom_info")

    # Check if custom_info is None or empty
    if custom_info is None or custom_info.strip() == "":
        return "No custom information provided in pytest.ini"

    additional_info = "Additional information for the test report\n"
    additional_info += f"Project root directory: {startdir}\n"
    additional_info += f"{config.getini('custom_info')}"

    return additional_info
