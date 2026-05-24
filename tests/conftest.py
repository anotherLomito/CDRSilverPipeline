from pathlib import Path

import pytest

from pipeline import create_spark_session


@pytest.fixture(scope="session")
def spark():
    session = create_spark_session("cdr-silver-pipeline-tests")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def raw_data_path() -> Path:
    return Path(__file__).resolve().parents[1] / "data"
