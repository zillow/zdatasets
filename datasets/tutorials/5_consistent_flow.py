import os

import pandas as pd
from metaflow import FlowSpec, step

from datasets import Dataset, Mode
from datasets.context import Context
from datasets.dataset_plugin import DatasetPlugin
from datasets.metaflow import DatasetParameter
from datasets.plugins import BatchOptions, MetaflowExecutor
from datasets.tutorials.online_plugin import OnlineOptions


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
                Context.BATCH: BatchOptions(),
                Context.ONLINE: OnlineOptions(keys="first,second"),
            },
        ),
    )

    @step
    def start(self):
        print(f"{self.hello_ds=}")

        df = pd.DataFrame([{"key": "secret", "value": 42}])
        self.hello_ds.write(df)

        read_df = self.hello_ds.to_pandas()

        print(f"{read_df=}")

        self.next(self.end)

    @step
    def end(self):
        print("done!")


if __name__ == "__main__":
    ConsistentFlow()
