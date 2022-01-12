from datasets.utils.case_utils import (
    is_snake_case,
    is_upper_pascal_case,
    pascal_to_snake_case,
    snake_case_to_pascal,
)


def test_is_valid_dataset_name():
    for name in ["Ds", "Ds1", "DsName", "DsName2"]:
        assert is_upper_pascal_case(name)

    for name in ["dS", "data_name", "data_Name", "_Ds", "Ds_", "2Ds", "for", "is", "Data%"]:
        assert not is_upper_pascal_case(name)


def test_is_snake_case():
    for name in ["a", "ds", "snake_1", "hello_world", "ds_hello_world_2"]:
        assert is_snake_case(name)

    for name in ["d_S", "Data_name", "data_Name", "_ds", "ds_", "2ds", "for", "is", "Data%"]:
        assert not is_snake_case(name)


def test_snake_case_to_pascal():
    expected_values = dict(
        ab="Ab", ds="Ds", snake_1="Snake1", hello_world="HelloWorld", ds_hello_world_2="DsHelloWorld2"
    )

    for name, expected in expected_values.items():
        assert expected == snake_case_to_pascal(name)


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
