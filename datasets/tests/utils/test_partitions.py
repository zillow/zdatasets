from pathlib import Path
from typing import List
from unittest.mock import Mock, patch

import pytest

from datasets.utils.partitions import Partition, get_path_partitions


@pytest.fixture
def assume_role() -> str:
    return "arn:aws:iam::170606514770:role/dev-zestimate-role"


@pytest.fixture
def s3_response() -> list:
    return [
        {"Prefix": "aip/kfp/aip-kfp-example/data/models/region=king/"},
        {"Prefix": "aip/kfp/aip-kfp-example/data/models/region=la/"},
    ]


@pytest.fixture
def s3_response_dates() -> list:
    return [
        {"Prefix": "aip/kfp/aip-kfp-example/data/date=2020-07-23/models/region=king/"},
        {"Prefix": "aip/kfp/aip-kfp-example/data/date=2020-07-23/models/region=la/"},
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


@patch("datasets.utils.partitions.get_aws_client")
def test_load_partitions_s3(get_aws_client_mock: Mock, assume_role: str, s3_response: list):
    mocked_s3_client = get_aws_client_mock.return_value
    mocked_s3_client.get_paginator.return_value.paginate.return_value.search = Mock(
        return_value=iter(s3_response)
    )

    path = "s3://workspace-zillow-analytics-stage/aip/kfp/aip-kfp-example/data/models/"
    ret: List[Partition] = get_path_partitions(path, assume_role)
    assert ret == [
        Partition(name="king", path=path + "region=king/"),
        Partition(name="la", path=path + "region=la/"),
    ]


@patch("datasets.utils.partitions.get_aws_client")
def test_load_partitions_s3_dates(get_aws_client_mock: Mock, assume_role: str, s3_response_dates: list):
    mocked_s3_client = get_aws_client_mock.return_value
    mocked_s3_client.get_paginator.return_value.paginate.return_value.search = Mock(
        return_value=iter(s3_response_dates)
    )

    path = "s3://workspace-zillow-analytics-stage/aip/kfp/aip-kfp-example/data/date=2020-07-23/models/"
    ret: List[Partition] = get_path_partitions(path, assume_role)
    assert ret == [
        Partition(name="king", path=path + "region=king/"),
        Partition(name="la", path=path + "region=la/"),
    ]
