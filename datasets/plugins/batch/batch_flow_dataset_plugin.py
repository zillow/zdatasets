from typing import Optional

from datasets.context import Context
from datasets.dataset_plugin import DatasetPlugin
from datasets.plugins import BatchDatasetPlugin


@DatasetPlugin.register_plugin(constructor_keys={"flow_dataset"}, context=Context.Batch)
class BatchFlowDatasetPlugin(BatchDatasetPlugin):
    def __init__(
        self,
        flow_dataset: str,
        name: str = None,
        columns=None,
        run_id=None,
        attribute_name: Optional[str] = None,
    ):
        from metaflow import Flow, Run

        self.flow_dataset = flow_dataset
        self.flow_name, self.dataset_name = flow_dataset.split(".")

        if run_id:
            run = Run(f"{self.flow_name}/{run_id}")
        else:
            run = Flow(self.flow_name).latest_successful_run

        dataset = getattr(run.data, self.dataset_name)

        super(BatchFlowDatasetPlugin, self).__init__(
            name=dataset.name,
            logical_key=dataset.key,
            columns=columns,
            run_id=run_id,
            attribute_name=attribute_name if attribute_name else (name if name else self.dataset_name),
        )
        #
        self.program_name = dataset.program_name
