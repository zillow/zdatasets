import pandas as pd
from metaflow import FlowSpec, Parameter, step

from datasets import DatasetType, Mode
from datasets.plugins import BatchDataset


# Can also invoke from CLI:
#  > python datasets/tutorials/0_hello_dataset_flow.py run \
#    --hello_dataset '{"name": "HelloDataset", "partition_by": "region", "mode": "READ_WRITE"}'
class HelloDatasetFlow(FlowSpec):
    hello_dataset: BatchDataset = Parameter(
        "hello_dataset",
        default=dict(name="HelloDataset", partition_by="region", mode=Mode.READ_WRITE),
        type=DatasetType,
    )

    @step
    def start(self):
        df = pd.DataFrame({"region": ["A", "A", "A", "B", "B", "B"], "zpid": [1, 2, 3, 4, 5, 6]})
        print("saving data_frame: \n", df.to_string(index=False))

        # Example of writing to a dataset
        print(f"{self.hello_dataset.program_name=}")
        self.hello_dataset.write(df)

        # save this as an output dataset
        self.output_dataset = self.hello_dataset

        self.next(self.end)

    @step
    def end(self):
        print(f"I have dataset \n{self.output_dataset=}")

        # output_dataset to_pandas(partitions=dict(region="A")) only
        df: pd.DataFrame = self.output_dataset.to_pandas(partitions=dict(region="A"))
        print('self.output_dataset.to_pandas(partitions=dict(region="A")):')
        print(df.to_string(index=False))


if __name__ == "__main__":
    HelloDatasetFlow()
