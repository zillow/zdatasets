import pytest

from datasets import dataset
from datasets.plugins import BatchDatasetPlugin


def test_step_decorator():
    class Foo:
        @dataset(name="ds")
        def hi(self):
            assert self.ds.name == "ds"
            assert isinstance(self.ds, BatchDatasetPlugin)

    foo = Foo()
    foo.hi()
    assert foo.ds.name == "ds"
    assert isinstance(foo.ds, BatchDatasetPlugin)


def test_step_decorator_class_field_name():
    ds_name = "ds_fee_yoyo"

    class Foo:
        @dataset(name=ds_name, field_name="ds_fee")
        def hi(self):
            assert self.ds_fee.name == ds_name
            assert isinstance(self.ds_fee, BatchDatasetPlugin)

    foo = Foo()
    foo.hi()
    assert foo.ds_fee.name == ds_name
    assert isinstance(foo.ds_fee, BatchDatasetPlugin)


def test_step_decorator_class_bad_name():
    bad_name = "ds-fee"

    class Foo:
        @dataset(name=bad_name)
        def hi(self):
            pass

    with pytest.raises(ValueError) as exc_info:
        foo = Foo()
        foo.hi()

    assert f"'{bad_name}' is not a valid Dataset name.  Please use Snake Case syntax:" in str(exc_info.value)


def test_step_decorator_field_bad_name():
    bad_name = "ds-fee"

    class Foo:
        @dataset(name="foo", field_name=bad_name)
        def hi(self):
            pass

    with pytest.raises(ValueError) as exc_info:
        foo = Foo()
        foo.hi()

    assert f"{bad_name} is not a valid Python identifier" in str(exc_info.value)
