import os

import pandas as pd  # type: ignore
from metaflow import FlowSpec, step

from datasets import Mode, dataset
from datasets.plugins import BatchDataset, BatchOptions


flow_dir = os.path.dirname(os.path.realpath(__file__))
my_dataset_foreach_path = os.path.join(flow_dir, "data/my_dataset_foreach")


class ForeachDatasetFlow(FlowSpec):
    @step
    def start(self):
        self.regions = ["A", "B"]
        self.next(self.foreach_split, foreach="regions")

    @dataset(
        name="MyDataset",
        options=BatchOptions(
            partition_by="region,run_id",
            path=my_dataset_foreach_path,
        ),
        mode=Mode.READ_WRITE,
    )
    @step
    def foreach_split(self):
        df = pd.DataFrame({"zpid": [1, 2, 3] if self.input == "A" else [4, 5, 6]})

        # Set
        df["region"] = self.input
        print(f"saving: {self.input=}")

        # Example of writing to a dataset with a path within a foreach split
        self.my_dataset: BatchDataset
        self.my_dataset.write_pandas(df)

        self.next(self.join_step)

    @step
    def join_step(self, inputs):
        self.my_dataset = inputs[0].my_dataset
        self.next(self.end)

    @step
    def end(self):
        print(f"I have datasets \n{self.my_dataset=}\n")
        print(
            self.my_dataset.to_pandas(partitions=dict(region="A")).to_string(index=False),
        )


if __name__ == "__main__":
    ForeachDatasetFlow()
