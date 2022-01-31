import pandas as pd
from metaflow import FlowSpec, Parameter, step

from datasets import DatasetType, Mode
from datasets.plugins import HiveDataset


# Can also invoke from CLI:
#  > python datasets/tutorials/6_hive_dataset_flow.py.py run \
#    --zpids_dataset '{"name": "ZpidsDataset", hive_table="zpids_dataset", \
#    "partition_by": "region", "mode": "READ_WRITE"}'


class HiveDatasetFlow(FlowSpec):
    zpids_dataset: HiveDataset = Parameter(
        "zpids_dataset",
        default=dict(
            name="ZpidsDataset",
            is_hive_table=True,
            partition_by="region,run_id",
            mode=Mode.READ_WRITE,
        ),
        type=DatasetType,
    )

    @step
    def start(self):
        df = pd.DataFrame({"region": ["A", "A", "A", "B", "B", "B"], "zpid": [1, 2, 3, 4, 5, 6]})
        print("saving data_frame: \n", df.to_string(index=False))

        # Example of writing to a dataset
        print(f"{self.zpids_dataset.program_name=}")
        self.zpids_dataset.write(df)

        # save this as an output dataset
        self.output_dataset = self.zpids_dataset
        read_df: pd.DataFrame = self.output_dataset.to_spark_pandas(partitions=dict(region="A")).to_pandas()
        print('self.output_dataset.to_pandas(partitions=dict(region="A")):')
        print(read_df.to_string(index=False))

        self.next(self.end)

    @step
    def end(self):
        print(f"I have dataset: {self.output_dataset=}")


if __name__ == "__main__":
    HiveDatasetFlow()
