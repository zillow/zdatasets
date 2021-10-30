import pandas as pd

from datasets import Mode
from datasets.context import Context
from datasets.dataset_plugin import DatasetPlugin


@DatasetPlugin.register_plugin(constructor_keys={"test_name"}, context=Context.Batch)
class TestDatasetPlugin(DatasetPlugin):
    db = pd.DataFrame(
        {"key": ["first", "second", "third", "fourth"], "value": [1, 2, 3, 4]},
    )
    db.set_index("key")

    def __init__(
        self,
        test_name: str,
        logical_key: str = None,
        columns=None,
        run_id=None,
        mode: Mode = Mode.Read,
        attribute_name: str = None,
    ):
        super(TestDatasetPlugin, self).__init__(
            name=test_name,
            logical_key=logical_key,
            columns=columns,
            run_id=run_id,
            mode=mode,
            attribute_name=attribute_name,
        )

    def read(self, key):
        return self.db[self.db.key.isin(key)]


@DatasetPlugin.register_plugin(constructor_keys={"test_name", "test_name2"}, context=Context.Batch)
class TestDatasetPlugin2(DatasetPlugin):
    def __init__(
        self,
        test_name: str,
        test_name2: str,
    ):
        super(TestDatasetPlugin2, self).__init__(
            name=f"{test_name}.{test_name2}",
        )


@DatasetPlugin.register_plugin(constructor_keys={"test_fee"}, context=Context.Online | Context.Streaming)
class TestFeeOnlineDatasetPlugin(DatasetPlugin):
    def __init__(
        self,
        test_fee: str,
        logical_key: str = None,
        columns=None,
        run_id=None,
        mode: Mode = Mode.Read,
        attribute_name: str = None,
    ):
        super(TestFeeOnlineDatasetPlugin, self).__init__(
            name=test_fee,
            logical_key=logical_key,
            columns=columns,
            run_id=run_id,
            mode=mode,
            attribute_name=attribute_name,
        )


def test_from_keys():
    import datetime

    a = datetime.datetime.now()
    dataset = DatasetPlugin.from_keys(test_name="foo")
    b = datetime.datetime.now()
    c = b - a
    assert c.microseconds < 500  # less than 0.5 ms, it actually takes ~24 microseconds
    assert dataset.name == "foo"
    assert dataset.read(["first", "fourth"])["value"].to_list() == [1, 4]
    assert isinstance(dataset, TestDatasetPlugin)

    dataset = DatasetPlugin.from_keys(test_name="t1", test_name2="t2")
    assert dataset.name == "t1.t2"
    assert isinstance(dataset, TestDatasetPlugin2)

    dataset = DatasetPlugin.from_keys(test_fee="test_fee", context=Context.Online)
    assert dataset.name == "test_fee"
    assert isinstance(dataset, TestFeeOnlineDatasetPlugin)

    dataset = DatasetPlugin.from_keys(test_fee="test_fee", context=Context.Streaming)
    assert dataset.name == "test_fee"
    assert isinstance(dataset, TestFeeOnlineDatasetPlugin)
