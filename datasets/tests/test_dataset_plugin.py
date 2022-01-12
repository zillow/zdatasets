import pandas as pd
import pytest

from datasets.context import Context
from datasets.dataset_plugin import DatasetPlugin
from datasets.plugins import BatchDataset
from datasets.tests.conftest import TestExecutor


@DatasetPlugin.register(constructor_keys={"name"}, context=Context.STREAMING)
class DefaultStreamingDatasetPluginTest(DatasetPlugin):
    def __init__(self, **kwargs):
        super(DefaultStreamingDatasetPluginTest, self).__init__(**kwargs)


@DatasetPlugin.register(constructor_keys={"name"}, context=Context.ONLINE)
class DefaultOnlineDatasetPluginTest(DatasetPlugin):
    def __init__(self, **kwargs):
        super(DefaultOnlineDatasetPluginTest, self).__init__(**kwargs)


@DatasetPlugin.register(constructor_keys={"test_name"}, context=Context.BATCH)
class NameDatasetPluginTest(DatasetPlugin):
    db = pd.DataFrame(
        {"key": ["first", "second", "third", "fourth"], "value": [1, 2, 3, 4]},
    )
    db.set_index("key")

    def __init__(self, test_name: str, **kwargs):
        super(NameDatasetPluginTest, self).__init__(name=test_name, **kwargs)

    def to_pandas(self, key) -> pd.DataFrame:
        return self.db[self.db.key.isin(key)]


@DatasetPlugin.register(constructor_keys={"test_name", "test_name2"}, context=Context.BATCH)
class Name2DatasetPluginTest(DatasetPlugin):
    def __init__(self, test_name: str, test_name2: str, **kwargs):
        super(Name2DatasetPluginTest, self).__init__(name=f"{test_name}{test_name2}", **kwargs)


@DatasetPlugin.register(constructor_keys={"test_fee"}, context=Context.ONLINE | Context.STREAMING)
class FeeOnlineDatasetPluginTest(DatasetPlugin):
    def __init__(self, test_fee: str, **kwargs):
        super(FeeOnlineDatasetPluginTest, self).__init__(name=test_fee, **kwargs)


def test_from_keys_dataset_factory_latency():
    import datetime

    a = datetime.datetime.now()
    dataset = DatasetPlugin.from_keys(test_name="Foo")
    b = datetime.datetime.now()
    c = b - a
    assert c.microseconds < 500  # less than 0.5 ms, it actually takes ~24 microseconds

    assert dataset.name == "Foo"
    assert isinstance(dataset, NameDatasetPluginTest)


def test_from_keys():
    dataset = DatasetPlugin.from_keys(name="Foo")
    assert isinstance(dataset, BatchDataset)

    dataset = DatasetPlugin.from_keys(test_name="Foo")
    assert dataset.name == "Foo"
    assert dataset.to_pandas(["first", "fourth"])["value"].to_list() == [1, 4]
    assert isinstance(dataset, NameDatasetPluginTest)

    dataset = DatasetPlugin.from_keys(test_name="Ta", test_name2="Tb")
    assert dataset.name == "TaTb"
    assert isinstance(dataset, Name2DatasetPluginTest)

    dataset = DatasetPlugin.from_keys(test_fee="TestFee", context=Context.ONLINE)
    assert dataset.name == "TestFee"
    assert isinstance(dataset, FeeOnlineDatasetPluginTest)

    dataset = DatasetPlugin.from_keys(test_fee="TestFee", context=Context.STREAMING)
    assert dataset.name == "TestFee"
    assert isinstance(dataset, FeeOnlineDatasetPluginTest)

    dataset = DatasetPlugin.from_keys(test_fee="TestFee", context="STREAMING")
    assert isinstance(dataset, FeeOnlineDatasetPluginTest)


def test_from_keys_consistent_access():
    dataset = DatasetPlugin.from_keys(name="Foo")
    assert dataset.name == "Foo"
    assert isinstance(dataset, BatchDataset)

    TestExecutor.current_context = Context.STREAMING
    dataset = DatasetPlugin.from_keys(name="Foo")
    assert dataset.name == "Foo"
    assert isinstance(dataset, DefaultStreamingDatasetPluginTest)

    TestExecutor.current_context = Context.ONLINE
    dataset = DatasetPlugin.from_keys(name="Foo")
    assert dataset.name == "Foo"
    assert isinstance(dataset, DefaultOnlineDatasetPluginTest)

    TestExecutor.current_context = Context.BATCH


def test_register_plugin():
    with pytest.raises(ValueError) as execinfo:

        @DatasetPlugin.register(constructor_keys={"name"}, context=Context.ONLINE)
        class FooPlugin(DatasetPlugin):
            def __init__(self, **kwargs):
                super(FooPlugin, self).__init__(**kwargs)

    assert "already registered" in str(execinfo.value)

    with pytest.raises(ValueError) as execinfo:

        @DatasetPlugin.register(constructor_keys=None, context=Context.ONLINE)
        class FooPlugin1(DatasetPlugin):
            def __init__(self, **kwargs):
                super(FooPlugin1, self).__init__(**kwargs)

    assert "constructor_keys cannot be None" in str(execinfo.value)

    with pytest.raises(ValueError) as execinfo:

        @DatasetPlugin.register(constructor_keys={"name"}, context=None)
        class FooPlugin2(DatasetPlugin):
            def __init__(self, **kwargs):
                super(FooPlugin2, self).__init__(**kwargs)

    assert "context cannot be None" in str(execinfo.value)

    with pytest.raises(ValueError) as execinfo:

        @DatasetPlugin.register(constructor_keys={"name"}, context=1)
        class FooPlugin3(DatasetPlugin):
            def __init__(self, **kwargs):
                super(FooPlugin3, self).__init__(**kwargs)

    assert "is not of type(Context)" in str(execinfo.value)


def test_is_valid_dataset_name():
    bad_name = "ds-fee"
    with pytest.raises(ValueError) as exec_info:
        DatasetPlugin.from_keys(name=bad_name)

    assert f"'{bad_name}' is not a valid Dataset name.  Please use Upper Pascal Case syntax:" in str(
        exec_info.value
    )
