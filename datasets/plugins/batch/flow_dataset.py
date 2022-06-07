from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, Tuple, Union

from datasets._typing import ColumnNames
from datasets.context import Context
from datasets.dataset_plugin import DatasetPlugin, StorageOptions
from datasets.mode import Mode
from datasets.plugins import BatchDataset


if TYPE_CHECKING:
    from metaflow import Run


@dataclass
class FlowOptions(StorageOptions):
    flow_dataset: str


@DatasetPlugin.register(options_type=FlowOptions, context=Context.BATCH)
class FlowDataset(BatchDataset):
    def __init__(
        self,
        options: FlowOptions,
        columns: Optional[ColumnNames] = None,
        run_id: Optional[str] = None,
        mode: Union[Mode, str] = Mode.READ,
        **kwargs,  # to avoid TypeError: __init__() got an unexpected keyword argument 'name'
    ):
        self.flow_name, self.dataset_name = options.flow_dataset.split(".")

        run, queried_run_id = _get_run_id(self.flow_name, run_id)

        dataset: DatasetPlugin = getattr(run.data, self.dataset_name)

        super(FlowDataset, self).__init__(
            name=dataset.name,
            logical_key=dataset.key,
            columns=columns,
            run_id=queried_run_id,
            run_time=dataset.run_time,
            mode=mode,
            options=options,
        )
        # The program name is that of the original dataset name
        print(f"{self.program_name=}, {dataset.program_name=}")
        self.program_name = dataset.program_name
        self.hive_table_name = dataset.hive_table_name


def _get_run_id(flow_name: str, run_id: Optional[str]) -> Tuple["Run", Optional[str]]:
    from metaflow import Flow, Run

    if run_id is None:
        run_id = "latest_successful_run"

    flow = Flow(flow_name)
    run: Run
    ret_run_id: Optional[str]
    if run_id in ["latest_successful_run", "latest_run"]:
        run = flow.latest_run if run_id == "latest_run" else flow.latest_successful_run
        ret_run_id = run.id
    elif run_id:
        run = flow[run_id]
        ret_run_id = run_id
    return run, ret_run_id
