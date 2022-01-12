from pathlib import Path
from typing import List

import pytest
from moto import mock_s3, mock_sts

from datasets.utils.aws import get_aws_client
from datasets.utils.partitions import Partition, get_path_partitions


@pytest.fixture
def assume_role() -> str:
    return "arn:aws:iam::333333333333:role/test-role"


@pytest.fixture
def region_partitioned_paths() -> List[str]:
    return [
        "data/models/region=king/",
        "data/models/region=la/",
    ]


def _sort_partitions(partitions: List[Partition]) -> List[Partition]:
    return sorted(partitions, key=lambda p: p.name)


def test_get_path_partitions(data_path: Path, assume_role: str):
    root_path = data_path / Path("train") / Path("date=2020-07-23")
    path_partitions = get_path_partitions(root_path, assume_role)

    assert _sort_partitions(path_partitions) == [
        Partition(name="king", path=str(root_path / Path("region=king"))),
        Partition(name="la", path=str(root_path / Path("region=la"))),
    ]


def test_get_path_partitions_suffix(data_path: Path, assume_role: str):
    root_path = data_path / Path("train") / Path("date=2020-07-23")
    path_partitions = get_path_partitions(root_path, assume_role, suffix="*.parquet")

    assert _sort_partitions(path_partitions) == [
        Partition(name="king", path=str(root_path / Path("region=king/*.parquet"))),
        Partition(name="la", path=str(root_path / Path("region=la/*.parquet"))),
    ]


@pytest.mark.parametrize("assume_role", [None, "arn:aws:iam::333333333333:role/test-role"])
@mock_sts
@mock_s3
def test_load_partitions_s3(assume_role: str, region_partitioned_paths: List[str]):
    bucket_name = "test-bucket"
    path = f"s3://{bucket_name}/data/models/"
    s3 = get_aws_client(assume_role, "s3")
    s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "us-west-2"})
    for region_path in region_partitioned_paths:
        s3.put_object(Bucket=bucket_name, Key=region_path, Body="foo")

    ret: List[Partition] = get_path_partitions(path, assume_role)
    assert ret == [
        Partition(name="king", path=path + "region=king/"),
        Partition(name="la", path=path + "region=la/"),
    ]
