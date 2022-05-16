import pandas as pd
from metaflow import FlowSpec, step

from datasets import dataset
from datasets.context import Context
from datasets.dataset_plugin import DatasetPlugin
from datasets.plugins import MetaflowExecutor
from datasets.tutorials.online_plugin import DefaultOnlineDatasetPlugin


# An online executor context!
class OnlineExecutor(MetaflowExecutor):
    @property
    def context(self) -> Context:
        return Context.ONLINE


DatasetPlugin.register_executor(executor=OnlineExecutor())


class HelloPluginFlow(FlowSpec):
    @dataset("HelloDataset")
    @step
    def start(self):
        assert isinstance(self.hello_dataset, DefaultOnlineDatasetPlugin)

        df: pd.DataFrame = self.hello_dataset.to_pandas(keys=["first", "third"])
        print(f"{df=}")

        self.next(self.end)

    @step
    def end(self):
        print("done!")


if __name__ == "__main__":
    HelloPluginFlow()
