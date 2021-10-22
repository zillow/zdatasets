import os
import uuid

from datasets.dataset import Dataset
from datasets.program_executor import ProgramExecutor


_run_id = str(uuid.uuid1())


class TestExecutor(ProgramExecutor):
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
    def context(self) -> str:
        return "offline"


Dataset.register_executor(executor=TestExecutor())
