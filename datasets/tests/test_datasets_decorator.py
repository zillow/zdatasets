import pytest

from datasets import dataset_attribute
from datasets.plugins import HiveDataset


def test_step_decorator():
    class Foo:
        @dataset_attribute("DsFee")
        def hi(self):
            assert self.ds_fee.name == "DsFee"
            assert isinstance(self.ds_fee, HiveDataset)

    foo = Foo()
    foo.hi()
    assert foo.ds_fee.name == "DsFee"
    assert isinstance(foo.ds_fee, HiveDataset)


def test_step_decorator_class_field_name():
    ds_name = "DsFeeYoyo"

    class Foo:
        @dataset_attribute(name=ds_name, field_name="ds_fee")
        def hi(self):
            assert self.ds_fee.name == ds_name
            assert isinstance(self.ds_fee, HiveDataset)

    foo = Foo()
    foo.hi()
    assert foo.ds_fee.name == ds_name
    assert isinstance(foo.ds_fee, HiveDataset)


def test_step_decorator_class_bad_name():
    bad_name = "Ds-Fee"

    class Foo:
        @dataset_attribute(name=bad_name)
        def hi(self):
            pass

    with pytest.raises(ValueError) as exc_info:
        foo = Foo()
        foo.hi()

    assert f"'{bad_name}' is not a valid Dataset name.  Please use Upper Pascal Case syntax:" in str(
        exc_info.value
    )


def test_step_decorator_field_bad_name():
    bad_name = "ds-fee"

    class Foo:
        @dataset_attribute(name="Foo", field_name=bad_name)
        def hi(self):
            pass

    with pytest.raises(ValueError) as exc_info:
        foo = Foo()
        foo.hi()

    assert f"{bad_name} is not a valid Python identifier" in str(exc_info.value)
