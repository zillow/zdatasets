import pandas as pd
from metaflow import FlowSpec, Parameter, step

from datasets import DatasetType, Mode
from datasets.plugins import HiveDataset


# Can also invoke from CLI:
#  > python datasets/tutorials/6_hive_dataset_flow.py.py run \
#    --hive_dataset '{"name": "HelloDataset", hive_table="hive_dataset", \
#    "partition_by": "region", "mode": "READ_WRITE"}'


class HiveDatasetFlow(FlowSpec):
    hive_dataset: HiveDataset = Parameter(
        "hive_dataset",
        default=dict(
            # partitioning by run_id must be explicit!
            name="HelloDataset",
            hive_table="hive_dataset",
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
        print(f"{self.hive_dataset.program_name=}")
        self.hive_dataset.write(df)

        # save this as an output dataset
        self.output_dataset = self.hive_dataset
        read_df: pd.DataFrame = self.output_dataset.to_spark_pandas(partitions=dict(region="A")).to_pandas()
        print('self.output_dataset.to_pandas(partitions=dict(region="A")):')
        print(read_df.to_string(index=False))

        self.next(self.end)

    @step
    def end(self):
        print(f"I have dataset: {self.output_dataset=}")


if __name__ == "__main__":
    HiveDatasetFlow()
