import pandas as pd
from metaflow import FlowSpec, step

from datasets import Dataset, Mode
from datasets.metaflow import DatasetParameter
from datasets.plugins import HiveDataset, HiveOptions


# Can also invoke from CLI:
#  > python datasets/tutorials/6_hive_dataset_flow.py.py run \
#    --zpids_dataset '{"name": "ZpidsDataset", hive_table="zpids_dataset", \
#    "partition_by": "region", "mode": "READ_WRITE"}'


class HiveDatasetFlow(FlowSpec):
    zpids_dataset = DatasetParameter(
        "zpids_dataset",
        default=Dataset(
            name="ZpidsDataset",
            options=HiveOptions(partition_by="region,run_id"),
            mode=Mode.READ_WRITE,
        ),
    )

    @step
    def start(self):
        df = pd.DataFrame({"region": ["A", "A", "A", "B", "B", "B"], "zpid": [1, 2, 3, 4, 5, 6]})
        print("saving data_frame: \n", df.to_string(index=False))

        # Example of writing to a dataset
        self.zpids_dataset.write(df)

        # save this as an output dataset
        self.output_dataset: HiveDataset = self.zpids_dataset
        read_df: pd.DataFrame = self.output_dataset.to_spark_pandas(partitions=dict(region="A")).to_pandas()
        print(f"{read_df.to_string(index=False)=}")

        self.next(self.end)

    @step
    def end(self):
        print(f"I have dataset: {self.output_dataset=}")


if __name__ == "__main__":
    HiveDatasetFlow()
