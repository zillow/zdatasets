import logging
import os
import time
import uuid
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from datasets import Context
from datasets.dataset_plugin import DatasetPlugin
from datasets.program_executor import ProgramExecutor


test_dir = Path(os.path.realpath(__file__)).parent
_run_id = str(uuid.uuid1())
_run_time = time.time()


@pytest.fixture
def data_path() -> Path:
    return test_dir / Path("data")


@pytest.fixture
def run_id() -> str:
    return _run_id


class TestExecutor(ProgramExecutor):
    current_context = Context.BATCH
    test_run_time = int(time.time())

    @property
    def current_run_id(self) -> str:
        return _run_id

    @property
    def datastore_path(self) -> str:
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), "data")

    @property
    def current_program_name(self) -> str:
        return "my_program"

    @property
    def context(self) -> Context:
        return TestExecutor.current_context

    @property
    def run_time(self) -> int:
        return TestExecutor.test_run_time


DatasetPlugin.register_executor(executor=TestExecutor())


@pytest.fixture(scope="session")
def spark_session():
    """Fixture for creating a spark context."""
    from pyspark.pandas.utils import SPARK_CONF_ARROW_ENABLED

    spark_session = (
        SparkSession.builder.master("local[2]")
        .config(SPARK_CONF_ARROW_ENABLED, True)
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.executor.instances", "1")
        .config("dfs.client.read.shortcircuit.skip.checksum", True)
        .config("hive.metastore.warehouse.dir", str(test_dir / Path("data")))
        .appName("dataset-pyspark-local-testing")
        .enableHiveSupport()
        .getOrCreate()
    )

    logger = logging.getLogger("py4j")
    logger.setLevel(logging.WARN)
    return spark_session
