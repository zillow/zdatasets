from datasets.dataset import Dataset


def test_dataset():
    dataset = Dataset(
        name="foo",
        key="my_key",
        partition_by="col1,col2",
        path="my_path",
        columns="col1,col2",
    )

    assert dataset.dataset_path == "my_path"
