import logging
from os import listdir
from pathlib import Path
from typing import List, NamedTuple, Union

from datasets.utils.aws import (
    get_aws_client,
    get_paginated_list_objects_iterator,
    get_s3_bucket_key,
)


Partition = NamedTuple("Partition", [("name", str), ("path", str)])

_logger = logging.getLogger(__name__)


def get_path_partitions(
    data_path: Union[str, Path], assume_role: str = None, suffix: str = None
) -> List[Partition]:
    """
    Example out_path "train/":
    ```
        train/
            ├── region=king
            │ └── data.csv
            └── region=la
                └── data.csv
    ```
    Would return partitions:
    >>> [
    >>>    Partition(name="king", path="train/region=king/*"),
    >>>    Partition(name="la", path="train/region=la/*"),
    >>> ]

    Args:
        data_path: s3 or file system path
        assume_role: AWS role to assume.
        suffix: pattern to append to the path of returned Partitions, example: "*.parquet"

    Returns: List of named Partitions with path.
    """

    data_path = str(data_path)
    _logger.info(f"get_partitions_pipeline: {data_path} {suffix}")

    if data_path.startswith("s3"):
        # out_path would look like:
        # s3://workspace-zillow-analytics-stage/aip/kfp/aip-kfp-example/data/test/region=la/data.csv
        s3_client = get_aws_client(assume_role, "s3")
        bucket, key = get_s3_bucket_key(data_path)
        # Ref: https://github.com/boto/boto3/blob/develop/boto3/examples/
        #      s3.rst#list-top-level-common-prefixes-in-amazon-s3-bucket
        s3_response = get_paginated_list_objects_iterator(
            s3_client,
            search="CommonPrefixes",
            Bucket=bucket,
            Prefix=key.lstrip("/"),
            Delimiter="/",
        )
        return [
            Partition(
                name=x["Prefix"].split("=")[-1].replace("/", ""),
                path=f"s3://{bucket}/{x['Prefix']}" + (suffix if suffix else ""),
            )
            for x in s3_response
        ]
    else:
        # local test path
        from pathlib import Path

        return [
            Partition(
                name=x.split("=")[-1],
                path=str(Path(data_path) / Path(x) / (Path(suffix) if suffix else Path())),
            )
            for x in listdir(data_path)
        ]
