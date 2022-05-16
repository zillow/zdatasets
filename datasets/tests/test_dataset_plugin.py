from dataclasses import dataclass

import pandas as pd
import pytest

from datasets import Dataset
from datasets.context import Context
from datasets.dataset_plugin import DatasetPlugin, StorageOptions
from datasets.plugins import HiveDataset
from datasets.tests.conftest import TestExecutor


@DatasetPlugin.register(context=Context.STREAMING, as_default_context_plugin=True)
class DefaultStreamingDatasetPluginTest(DatasetPlugin):
    def __init__(self, *args, **kwargs):
        super(DefaultStreamingDatasetPluginTest, self).__init__(*args, **kwargs)


@DatasetPlugin.register(context=Context.ONLINE, as_default_context_plugin=True)
class DefaultOnlineDatasetPluginTest(DatasetPlugin):
    def __init__(self, **kwargs):
        super(DefaultOnlineDatasetPluginTest, self).__init__(**kwargs)


@dataclass
class DatasetTestOptions(StorageOptions):
    a: str


@DatasetPlugin.register(context=Context.BATCH, options=DatasetTestOptions)
class DatasetPluginTest(DatasetPlugin):
    db = pd.DataFrame(
        {"key": ["first", "second", "third", "fourth"], "value": [1, 2, 3, 4]},
    )
    db.set_index("key")

    def __init__(self, name: str, options: DatasetTestOptions, **kwargs):
        self.a = options.a
        super(DatasetPluginTest, self).__init__(name=name, options=options, **kwargs)

    def to_pandas(self, key) -> pd.DataFrame:
        return self.db[self.db.key.isin(key)]


@dataclass
class DatasetTestOptions2(StorageOptions):
    a: str
    b: str


@DatasetPlugin.register(context=Context.BATCH, options=DatasetTestOptions2)
class DatasetPluginTest2(DatasetPlugin):
    def __init__(self, name: str, options: DatasetTestOptions2, **kwargs):
        self.c = f"{options.a}{options.b}"
        super(DatasetPluginTest2, self).__init__(name=name, options=options, **kwargs)


@dataclass
class FeeOnlineDatasetOptions(StorageOptions):
    test_fee: str


@DatasetPlugin.register(context=Context.ONLINE | Context.STREAMING, options=FeeOnlineDatasetOptions)
class FeeOnlineDatasetPluginTest(DatasetPlugin):
    def __init__(self, name: str, options: FeeOnlineDatasetOptions, **kwargs):
        self.test_fee = options.test_fee
        super(FeeOnlineDatasetPluginTest, self).__init__(name=name, options=options, **kwargs)


def test_dataset_factory_latency():
    import datetime

    a = datetime.datetime.now()
    dataset = Dataset("Foo", options=DatasetTestOptions(a="Foo"))
    b = datetime.datetime.now()
    c = b - a
    assert c.microseconds < 500  # less than 0.5 ms, it actually takes ~24 microseconds

    assert dataset.name == "Foo"
    assert isinstance(dataset, DatasetPluginTest)


def test_dataset_factory_constructor():
    dataset = Dataset("FooName")
    assert isinstance(dataset, HiveDataset)
    assert dataset.name == "FooName"

    dataset = Dataset("FooName", options=DatasetTestOptions("Foo"))
    assert dataset.name == "FooName"
    assert dataset.a == "Foo"
    assert dataset.to_pandas(["first", "fourth"])["value"].to_list() == [1, 4]
    assert isinstance(dataset, DatasetPluginTest)

    dataset = Dataset("Tata", options=DatasetTestOptions2(a="Ta", b="Tb"))
    assert dataset.name == "Tata"
    assert dataset.c == "TaTb"
    assert isinstance(dataset, DatasetPluginTest2)

    dataset = Dataset("TestFee", options=FeeOnlineDatasetOptions(test_fee="TestFee"), context=Context.ONLINE)
    assert dataset.name == "TestFee"
    assert dataset.test_fee == "TestFee"
    assert isinstance(dataset, FeeOnlineDatasetPluginTest)


def test_dataset_factory_constructor_unhappy():
    class UnHappyOptions(StorageOptions):
        pass

    options = UnHappyOptions()
    with pytest.raises(ValueError) as exec_info:
        Dataset("FooName", options=options)
    assert f"{type(options)=} not in" in str(exec_info.value)

    options = {Context.ONLINE: options}
    with pytest.raises(ValueError) as exec_info:
        Dataset("FooName", options=options)

    context_lookup = Context.BATCH
    assert f"{context_lookup=} not in {options.keys=}" in str(exec_info.value)


def test_dataset_factory_consistent_access():
    dataset = Dataset("Foo")
    assert dataset.name == "Foo"
    assert isinstance(dataset, HiveDataset)

    TestExecutor.current_context = Context.STREAMING
    dataset = Dataset("Foo")
    assert dataset.name == "Foo"
    assert isinstance(dataset, DefaultStreamingDatasetPluginTest)

    TestExecutor.current_context = Context.ONLINE
    dataset = Dataset(name="Foo")
    assert dataset.name == "Foo"
    assert isinstance(dataset, DefaultOnlineDatasetPluginTest)

    TestExecutor.current_context = Context.BATCH


def test_register_plugin():
    with pytest.raises(ValueError) as execinfo:

        @DatasetPlugin.register(context=Context.ONLINE, as_default_context_plugin=True)
        class FooPlugin(DatasetPlugin):
            def __init__(self, **kwargs):
                super(FooPlugin, self).__init__(**kwargs)

    assert "already registered" in str(execinfo.value)

    with pytest.raises(ValueError) as execinfo:

        @DatasetPlugin.register(context=None)
        class FooPlugin2(DatasetPlugin):
            def __init__(self, **kwargs):
                super(FooPlugin2, self).__init__(**kwargs)

    assert "context cannot be None" in str(execinfo.value)

    with pytest.raises(ValueError) as execinfo:

        @DatasetPlugin.register(context=1)
        class FooPlugin3(DatasetPlugin):
            def __init__(self, **kwargs):
                super(FooPlugin3, self).__init__(**kwargs)

    assert "is not of type(Context)" in str(execinfo.value)


def test_is_valid_dataset_name():
    bad_name = "ds-fee"
    with pytest.raises(ValueError) as exec_info:
        Dataset(bad_name)

    assert f"'{bad_name}' is not a valid Dataset name.  Please use Upper Pascal Case syntax:" in str(
        exec_info.value
    )
