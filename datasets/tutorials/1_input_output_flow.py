import pandas as pd  # type: ignore
from metaflow import Flow, FlowSpec, step

from datasets.datasets_decorator import datasets
from datasets.mode import Mode


class InputOutputDatasetFlow(FlowSpec):
    @datasets.dataset(flow_dataset="HelloDatasetFlow.hello_dataset")
    @datasets.dataset(name="output_dataset", partition_by="date_key,region", mode=Mode.Write)
    @step
    def start(self):
        df: pd.DataFrame = self.hello_dataset.read_pandas()
        df["date_key"] = "10-01-2021"
        self.output_dataset.write(df)
        # TODO: what do I support in the Pandas? ex: only parquet supported types
        #   lowest common denominator
        #  *map pandas -> parquet -> avro? -> hive -> dynamo
        #   nested types? timestamps? datetime? floats16, float64?
        #  *schema!! parquet inference may not be Hive schema!!
        #  *Hive -> iceberg ( doesn't rely on paths for partitions)? - types -
        #    can't change partitions Hive but can with iceberg

        # Optimization of pipe feature from S3 into PyTorch:
        # How do you pipe the data to the model?
        # How do you best represent the data in DF -> PyTorch

        # WAP : write audit publish, audit -> contracts!

        # TODO:
        #   *dataset tagging/@project(branches)/namespace? Can one experiment and not overwrite each other?
        #   *Wes -> Vaultron data, on how to publish arrow - @dataset with Arrow

        self.next(self.end)

    @step
    def end(self):
        print(f"I have dataset \n{self.output_dataset=}")
        print(
            "self.my_dataset.read_pandas:\n",
            self.output_dataset.read_pandas().to_string(index=False),
        )

        # Another way to access hello_dataset
        my_df = Flow("HelloDatasetFlow").latest_successful_run.data.hello_dataset.read_pandas()
        print(my_df.to_string(index=False))


if __name__ == "__main__":
    InputOutputDatasetFlow()
