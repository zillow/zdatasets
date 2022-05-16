from datetime import datetime

from dateutil import parser

from datasets.context import Context
from datasets.program_executor import ProgramExecutor


class MetaflowExecutor(ProgramExecutor):
    @property
    def current_run_id(self) -> str:
        from metaflow import current

        return str(current.run_id)

    @property
    def datastore_path(self) -> str:
        from metaflow import current
        from metaflow.datastore import TaskDataStore

        datastore: TaskDataStore = current.flow._datastore
        return datastore.parent_datastore._storage_impl.get_datastore_root_from_config(print)

    @property
    def current_program_name(self) -> str:
        from metaflow import current

        return current.flow_name

    @property
    def context(self) -> Context:
        return Context.BATCH

    @property
    def run_time(self) -> int:
        from metaflow import Run, current

        run = Run(f"{current.flow_name}/{current.run_id}")
        if isinstance(run.created_at, datetime):
            epoch = int(run.created_at.timestamp())
        else:
            epoch = int(parser.parse(run.created_at).timestamp())

        return epoch
