import pandas as pd
from metaflow import Flow, FlowSpec, step

from datasets import Mode, dataset


class InputOutputDatasetFlow(FlowSpec):
    @dataset(flow_dataset="HelloDatasetFlow.output_dataset", name="HelloDataset")
    @dataset(name="OutputDataset", partition_by="date_key,region", mode=Mode.READ_WRITE)
    @step
    def start(self):
        df: pd.DataFrame = self.hello_dataset.to_pandas()
        df["date_key"] = "10-01-2021"
        self.output_dataset.write(df)

        self.next(self.end)

    @step
    def end(self):
        print(f"I have dataset \n{self.output_dataset=}")
        print(
            "self.my_dataset.to_pandas:\n",
            self.output_dataset.to_pandas().to_string(index=False),
        )

        # Another way to access hello_dataset
        run = Flow("HelloDatasetFlow").latest_successful_run
        my_df = run.data.hello_dataset.to_pandas(run_id=run.id)
        print(my_df.to_string(index=False))


if __name__ == "__main__":
    InputOutputDatasetFlow()
