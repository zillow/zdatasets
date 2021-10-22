import pandas as pd

from datasets import Mode
from datasets.dataset import Dataset


@Dataset.register_plugin(constructor_keys={"test_name"})
class TestDataset(Dataset):
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
        super(TestDataset, self).__init__(
            name=test_name,
            logical_key=logical_key,
            columns=columns,
            run_id=run_id,
            mode=mode,
            attribute_name=attribute_name,
        )

    def read(self, key):
        return self.db[self.db.key.isin(key)]


@Dataset.register_plugin(constructor_keys={"test_fee"})
class TestFeeDataset(Dataset):
    def __init__(
        self,
        test_fee: str,
        logical_key: str = None,
        columns=None,
        run_id=None,
        mode: Mode = Mode.Read,
        attribute_name: str = None,
    ):
        super(TestFeeDataset, self).__init__(
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
    dataset = Dataset.from_keys(test_name="foo")
    b = datetime.datetime.now()
    c = b - a
    assert c.microseconds < 500  # less than 0.5 ms, it actually takes ~24 microseconds
    assert dataset.name == "foo"
    assert dataset.read(["first", "fourth"])["value"].to_list() == [1, 4]

    import pprint

    pprint.pprint(Dataset._plugins)
    dataset = Dataset.from_keys(test_fee="test_fee")
    assert dataset.name == "test_fee"
    assert isinstance(dataset, TestFeeDataset)
