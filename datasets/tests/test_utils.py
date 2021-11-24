from datasets.utils import _is_upper_pascal_case


def test_is_valid_dataset_name():
    for name in ["Ds", "Ds1", "DsName", "DsName2"]:
        assert _is_upper_pascal_case(name)

    for name in ["dS", "data_name", "data_Name", "_Ds", "Ds_", "2Ds", "for", "is", "Data%"]:
        assert not _is_upper_pascal_case(name)
