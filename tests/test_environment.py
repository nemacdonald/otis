# tests/test_environment.py

import pytest
import subprocess
import shutil


@pytest.fixture
def conda_environment():
    """
    Fixture to set up and tear down the Conda environment.

    This fixture:
    - Checks if the Conda environment already exists. If it does, remove it.
    - Installs a Conda environment based on the provided envtest.yml file.
    - Yields the environment name to the test.
    - Tears down the Conda environment after the test.

    Raises:
        ValueError: If the Conda environment installation or removal fails.
    """
    env_name = "testotis"

    # Check if the Conda environment already exists
    existing_env_check = subprocess.run(
        ["conda", "env", "list"], capture_output=True, text=True
    )
    if env_name in existing_env_check.stdout:
        # Remove the existing Conda environment
        remove_command = ["conda", "env", "remove", "--name", env_name, "--yes"]
        result_remove = subprocess.run(remove_command, capture_output=True, text=True)

        # Check for removal errors
        if result_remove.returncode != 0:
            raise ValueError(
                f"Conda environment removal failed:\n{result_remove.stderr}"
            )

    # Set up the Conda environment
    install_command = [
        "conda",
        "env",
        "create",
        "-f",
        "envtest.yml",
        "--name",
        env_name,
    ]
    result_install = subprocess.run(install_command, capture_output=True, text=True)

    # Check for installation errors
    if result_install.returncode != 0:
        raise ValueError(
            f"Conda environment installation failed:\n{result_install.stderr}"
        )

    # Provide the environment name to the test
    yield env_name

    # Tear down the Conda environment
    remove_command = ["conda", "env", "remove", "--name", env_name, "--yes"]
    result_remove = subprocess.run(remove_command, capture_output=True, text=True)

    # Check for removal errors
    if result_remove.returncode != 0:
        raise ValueError(f"Conda environment removal failed:\n{result_remove.stderr}")

    # Clean up the created envtest.yml file (optional)
    shutil.rmtree(env_name, ignore_errors=True)


@pytest.mark.env
def test_conda_environment(conda_environment):
    """
    Test the installation and removal of a conda environment.

    This test function installs a conda environment based on the provided envtest.yml file,
    performs additional test steps, and then removes the created environment.

    Raises:
        AssertionError: If the conda environment installation or removal fails.
    """
    # Access the environment name through the fixture
    environment_name = conda_environment

    # Additional test steps can be added here
    assert 1 + 1 == 2
