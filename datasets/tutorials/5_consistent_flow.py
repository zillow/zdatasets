import os

import pandas as pd
from metaflow import FlowSpec, step

from datasets import Dataset, Mode
from datasets.context import Context
from datasets.dataset_plugin import DatasetPlugin
from datasets.metaflow import DatasetParameter
from datasets.plugins import BatchDataset, BatchOptions, MetaflowExecutor
from datasets.tutorials.online_plugin import (
    DefaultOnlineDatasetPlugin,
    OnlineOptions,
)


class PortableExecutor(MetaflowExecutor):
    @property
    def context(self) -> Context:
        context_str = os.environ.get("CONTEXT", "BATCH")
        return Context[context_str]


DatasetPlugin.register_executor(executor=PortableExecutor())


# Can also invoke from CLI:
class ConsistentFlow(FlowSpec):
    """
    see README.ipynb for a tutorial usage
    """

    hello_ds = DatasetParameter(
        name="hello_ds",
        default=Dataset(
            name="HelloDs",
            columns="key,value",
            mode=Mode.READ_WRITE,
            options_by_context={
                Context.BATCH: BatchOptions,
                Context.ONLINE: OnlineOptions,
            },
        ),
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
