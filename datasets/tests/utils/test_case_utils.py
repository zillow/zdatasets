from datasets.utils.case_utils import (
    is_upper_pascal_case,
    pascal_to_snake_case,
)


def test_is_valid_dataset_name():
    for name in ["Ds", "Ds1", "DsName", "DsName2"]:
        assert is_upper_pascal_case(name)

    for name in ["dS", "data_name", "data_Name", "_Ds", "Ds_", "2Ds", "for", "is", "Data%"]:
        assert not is_upper_pascal_case(name)


def test_pascal_to_snake_case():
    expected_values = dict(
        Ab="ab",
        Ds="ds",
        Snake1="snake_1",
        HelloWorld="hello_world",
        DsHelloWorld2="ds_hello_world_2",
    )

    for name, expected in expected_values.items():
        assert expected == pascal_to_snake_case(name)
