from datasets.mode import Mode
from datasets.metaflow import _DatasetTypeClass, _PydanticDatasetParameters
from datasets.plugins.batch.batch_base_plugin import BatchOptions
from datasets.plugins.batch.hive_dataset import HiveOptions


def test_dataset_type_class():
    json_value = (
        '{"name": "HiDataset", "mode": "READ_WRITE", "options":{"type":"HiveOptions", "path":"/foo_hive"}}'
    )
    dataset = _DatasetTypeClass().convert(json_value, None, None)
    assert dataset.name == "HiDataset"
    assert dataset.hive_table_name == "hi_dataset"
    assert dataset.mode == Mode.READ_WRITE


def test_dataset_parameters_decoder():
    params = _PydanticDatasetParameters.parse_raw(
        '{"name": "hi", "columns": "a,b", "mode":"WRITE", "options":{"type":"BatchOptions"}}'
    )
    assert params.name == "hi"
    assert params.columns == "a,b"
    assert params.mode == "WRITE"
    assert params.options == BatchOptions()

    # test case insensitivity
    params = _PydanticDatasetParameters.parse_raw(
        '{"name": "hi", "options":{"type":"batchOptions", "path":"/foo"}}'
    )
    assert params.options == BatchOptions(path="/foo")
    assert params.mode == Mode.READ

    params = _PydanticDatasetParameters.parse_raw(
        '{"name": "hi", "options":{"type":"HiveOptions", "path":"/foo_hive"}}'
    )
    assert params.options == HiveOptions(path="/foo_hive")

    # test empty HiveOptions
    params = _PydanticDatasetParameters.parse_raw('{"name": "hi", "options":{"type":"HiveOptions"}}')
    assert params.options == HiveOptions()

    # test dict options_by_context
    params = _PydanticDatasetParameters.parse_raw(
        '{"name": "hi", "options_by_context": {"ONLINE": {"type":"HiveOptions"}}}'
    )
    assert params.options is None
    assert params.options_by_context == {"ONLINE": HiveOptions()}
