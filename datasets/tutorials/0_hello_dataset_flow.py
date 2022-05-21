import pandas as pd
from metaflow import FlowSpec, step

from datasets import Dataset, Mode
from datasets.metaflow import DatasetParameter
from datasets.plugins import BatchOptions


# Can also invoke from CLI:
#  > python datasets/tutorials/0_hello_dataset_flow.py run \
#    --hello_dataset '{"name": "HelloDataset", "mode": "READ_WRITE", \
#    "options": {"type": "BatchOptions", "partition_by": "region"}}'
class HelloDatasetFlow(FlowSpec):
    hello_dataset = DatasetParameter(
        "hello_dataset",
        default=Dataset("HelloDataset", mode=Mode.READ_WRITE, options=BatchOptions(partition_by="region")),
    )

    @step
    def start(self):
        df = pd.DataFrame({"region": ["A", "A", "A", "B", "B", "B"], "zpid": [1, 2, 3, 4, 5, 6]})
        print("saving data_frame: \n", df.to_string(index=False))

        # Example of writing to a dataset
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
