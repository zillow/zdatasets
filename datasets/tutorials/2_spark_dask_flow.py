from dask.dataframe import DataFrame
from metaflow import FlowSpec, step

from datasets import datasets


class SparkDaskFlow(FlowSpec):
    @datasets.dataset(name="io_dataset", flow_dataset="InputOutputDatasetFlow.output_dataset")
    @step
    def start(self):
        print(f"I have dataset \n{self.io_dataset=}")
        self.next(self.end)

    @step
    def end(self):
        # io_dataset read_spark()
        # spark_df: DataFrame = self.io_dataset.read_spark()
        # spark_df.show()

        # io_dataset read_dask()
        dask_df: DataFrame = self.io_dataset.read_dask()
        dask_df = dask_df[dask_df.zpid < 4]
        print("self.io_dataset.read_dask: [zpid < 4]\n", dask_df.compute())


if __name__ == "__main__":
    SparkDaskFlow()
