from datasets.dataset import Dataset
from datasets.plugins import OfflineDataset


@Dataset.register_plugin(constructor_keys={"flow_dataset"})
class OfflineFlowDataset(OfflineDataset):
    def __init__(
        self,
        flow_dataset: str,
        name: str = None,
        columns=None,
        run_id=None,
    ):
        from metaflow import Flow, Run

        self.flow_dataset = flow_dataset
        self.flow_name, self.dataset_name = flow_dataset.split(".")
        if run_id:
            dataset = getattr(Run(f"{self.flow_name/run_id}").data, self.dataset_name)
        else:
            run = Flow(self.flow_name).latest_successful_run
            dataset = getattr(run.data, self.dataset_name)
            run_id = run.id

        super(OfflineFlowDataset, self).__init__(
            name=self.dataset_name,
            logical_key=dataset.key,
            columns=columns,
            run_id=run_id,
            attribute_name=name if name else self.dataset_name,
        )
        self.program_name = dataset.program_name
