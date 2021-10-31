import pandas as pd
from metaflow import FlowSpec, step

from datasets import Mode, dataset
from datasets.context import Context
from datasets.dataset_plugin import DatasetPlugin
from datasets.plugins import MetaflowExecutor


# An online executor context!
class OnlineExecutor(MetaflowExecutor):
    @property
    def context(self) -> Context:
        return Context.Online


DatasetPlugin.register_executor(executor=OnlineExecutor())


# The default online plugin!
@DatasetPlugin.register_plugin(constructor_keys={"name"}, context=Context.Online)
class KVDatasetPlugin(DatasetPlugin):
    db = pd.DataFrame(
        {"key": ["first", "second", "third", "fourth"], "value": [1, 2, 3, 4]},
    )
    db.set_index("key")

    def __init__(
        self,
        name: str = None,
        logical_key: str = None,
        columns=None,
        run_id=None,
        mode: Mode = Mode.Read,
        attribute_name: str = None,
    ):
        super(KVDatasetPlugin, self).__init__(
            name=name,
            logical_key=logical_key,
            columns=columns,
            run_id=run_id,
            mode=mode,
            attribute_name=attribute_name,
        )

    def read(self, key):
        return self.db[self.db.key.isin(key)]


class HelloPluginFlow(FlowSpec):
    # FUTURE:
    # hello_dataset = Parameter("hello_dataset", type=dataset(columns="col1"))

    @dataset(name="hello_dataset")
    @step
    def start(self):
        df: pd.DataFrame = self.hello_dataset.read(["first", "third"])
        print(f"{df=}")

        self.next(self.end)

    @step
    def end(self):
        print("done!")


if __name__ == "__main__":
    HelloPluginFlow()
