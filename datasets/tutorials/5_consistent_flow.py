import os

import pandas as pd
from metaflow import FlowSpec, Parameter, step

from datasets import DatasetType, Mode
from datasets.context import Context
from datasets.dataset_plugin import DatasetPlugin
from datasets.plugins import BatchDataset, MetaflowExecutor
from datasets.tutorials.online_plugin import DefaultOnlineDatasetPlugin


class PortableExecutor(MetaflowExecutor):
    @property
    def context(self) -> Context:
        context_str = os.environ.get("CONTEXT", "BATCH")
        return Context[context_str]


DatasetPlugin.register_executor(executor=PortableExecutor())


# flake8: noqa: E501
# Can also invoke from CLI:
class ConsistentFlow(FlowSpec):
    """
    see README.ipynb for a tutorial usage
    """

    hello_ds = Parameter(
        name="hello_ds",
        type=DatasetType,
        default=dict(name="HelloDs", columns="key,value", mode=Mode.READ_WRITE),
    )

    @step
    def start(self):
        print(f"{self.hello_ds=}")
        print(f"{self.hello_ds._executor.context=}")

        df = pd.DataFrame([{"key": "secret", "value": 42}])
        self.hello_ds.write(df)

        read_df = self.hello_ds.to_pandas()
        if DatasetPlugin._executor.context == Context.ONLINE:
            assert isinstance(self.hello_ds, DefaultOnlineDatasetPlugin)
        else:
            assert isinstance(self.hello_ds, BatchDataset)

        print(f"{type(read_df)=}")
        print(f"{read_df=}")

        self.next(self.end)

    @step
    def end(self):
        print("done!")


if __name__ == "__main__":
    ConsistentFlow()
